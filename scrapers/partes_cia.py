import re
import time
from datetime import datetime, date, timedelta
from urllib.parse import urlencode
from bs4 import BeautifulSoup

from db import conn

URL = "http://www.bomberosperu.gob.pe/extranet/depa/ceem/partesycomi/CEEMParteComiLis.asp"

VEHICULOS_CIA = {
    "0570": "RES-150",
    "1288": "M150-1",
    "1377": "AMB-150",
    "1544": "M150-3",
    "1737": "RESLIG-150",
    "2002": "CIST-150",
}

def _parse_fecha(texto):
    texto = texto.strip()
    if texto == "--" or not texto:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(texto, fmt)
        except ValueError:
            continue
    return None

def _buscar_bombero_al_mando(cur, texto):
    if not texto or texto.strip() == "--":
        return None, None
    texto = re.sub(r"\s+", " ", texto.strip())
    palabras = texto.split()
    if len(palabras) < 2:
        return None, texto

    # El formato varía: "Grado Nombres APELLIDO1 APELLIDO2" o "Grado APELLIDO1 APELLIDO2 Nombres"
    # Probamos ventanas de 2 palabras consecutivas en distintas posiciones
    for i in range(1, len(palabras) - 1):
        candidato = " ".join(palabras[i:i+2])
        cur.execute(r"""
            SELECT id FROM bombero
            WHERE REGEXP_REPLACE(apellidos, '\s+', ' ', 'g') ILIKE %s
            LIMIT 1
        """, (f"%{candidato}%",))
        row = cur.fetchone()
        if row:
            return row[0], texto

    return None, texto

def _buscar_o_crear_tipo_emergencia(cur, descripcion):
    if not descripcion or descripcion.strip() == "--":
        return None
    descripcion = descripcion.strip()
    cur.execute("SELECT id FROM tipo_emergencia WHERE descripcion = %s", (descripcion,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO tipo_emergencia (descripcion) VALUES (%s) RETURNING id", (descripcion,))
    return cur.fetchone()[0]

def _buscar_vehiculo(cur, codigo):
    cur.execute("SELECT id FROM vehiculo WHERE codigo = %s", (codigo.strip(),))
    row = cur.fetchone()
    return row[0] if row else None

_DISTRITOS_CONOCIDOS = [
    # Lima Norte
    "SAN MARTIN DE PORRES",
    "PUENTE PIEDRA",
    "LOS OLIVOS",
    "SANTA ROSA",
    "CARABAYLLO",
    "COMAS",
    "INDEPENDENCIA",
    "ANCON",
    "ANCÓN",
    # Callao
    "VENTANILLA",
    "MI PERU",
    "CALLAO",
    "BELLAVISTA",
    "LA PERLA",
    "LA PUNTA",
    "CARMEN DE LA LEGUA",
    # Lima Centro
    "LIMA",
    "RIMAC",
    "BREÑA",
    "BRENA",
    "LA VICTORIA",
    "EL AGUSTINO",
    "SAN LUIS",
    "SANTA ANITA",
    "ATE",
    "CERCADO DE LIMA",
    "BARRIOS ALTOS",
    # Lima Este / otros cercanos
    "SAN JUAN DE LURIGANCHO",
    "LURIGANCHO",
    "CHACLACAYO",
]

def _extraer_distrito(direccion):
    if not direccion:
        return None
    texto = re.sub(r"\s+", " ", direccion.strip().upper())
    for distrito in _DISTRITOS_CONOCIDOS:
        if texto.endswith(distrito):
            return distrito
    return None

def _buscar_o_crear_distrito(cur, direccion):
    nombre = _extraer_distrito(direccion)
    if not nombre:
        return None
    cur.execute("SELECT id FROM distrito WHERE nombre = %s", (nombre,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO distrito (nombre) VALUES (%s) RETURNING id", (nombre,))
    return cur.fetchone()[0]

def _sesion_activa(driver):
    return "extranet" in driver.current_url and "ini.asp" not in driver.current_url

def scrape_partes_cia(session, driver=None):
    cur = conn.cursor()
    nuevos = actualizados = 0

    hoy = date.today()
    params_base = {
        "NivelArbol": "../../../",
        "txtOrden": "numParte",
        "txtOrdenSentido": "asc",
        "txtOrdenAnterior": "",
        "txtTotalRegistro": "",
        "txtCodigoUbigeo": "",
        "txtCodIdenEst": "",
        "txtCodEstructura": "",
        "txtDireccion": "",
        "txtValoresCadenaDependencia": "",
        "opc": "1",
        "cboTipos": "T",
        "cboMesFechaInicio":  f"{hoy.month:02d}",
        "cboAnioFechaInicio": str(hoy.year),
        "cboMesFechaFin":     f"{hoy.month:02d}",
        "cboAnioFechaFin":    str(hoy.year),
        "cboDiaFechaInicio":  str(hoy.day),
        "cboDiaFechaFin":     str(hoy.day),
        "cboHoraFechaInicio": "0",
        "cboHoraFechaFin":    "23",
        "cboMinutoFechaInicio": "0",
        "cboMinutoFechaFin":    "59",
        "txtTitulo":          "ESTADO DE COMPANIAS",
    }

    for cod_vehi, cod_texto in VEHICULOS_CIA.items():
        try:
            params = {**params_base, "cboVehi": cod_vehi}
            if driver:
                for intento in range(3):
                    try:
                        driver.get(f"{URL}?{urlencode(params)}")
                        time.sleep(3)
                        break
                    except Exception:
                        print(f"  Reintentando {cod_texto} ({intento+1}/3)...")
                        time.sleep(10)
                if "localhost" in driver.current_url or "ini.asp" in driver.current_url:
                    from browser import login
                    login(driver)
                    driver.get(f"{URL}?{urlencode(params)}")
                    time.sleep(3)
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
            else:
                r = session.get(URL, params=params, timeout=30)
                soup = BeautifulSoup(r.text, "html.parser")
            filas = [tr for tr in soup.find_all("tr") if tr.get("onmouseover") or tr.get("onMouseOver")]

            for fila in filas:
                tds = fila.find_all("td")
                if len(tds) < 18:
                    continue

                tipo_parte   = tds[1].get_text(strip=True)
                numero_parte = tds[2].get_text(strip=True)
                if not numero_parte or numero_parte == "--":
                    continue

                fecha_despacho = _parse_fecha(tds[5].get_text(strip=True))
                fecha_salida   = _parse_fecha(tds[6].get_text(strip=True))
                fecha_llegada  = _parse_fecha(tds[7].get_text(strip=True))
                fecha_retorno  = _parse_fecha(tds[8].get_text(strip=True))
                fecha_ingreso  = _parse_fecha(tds[9].get_text(strip=True))
                tipo_emerg     = tds[10].get_text(strip=True).lstrip("–-").strip() or None
                observacion    = tds[11].get_text(strip=True).lstrip("–-").strip() or None
                direccion      = tds[12].get_text(strip=True).lstrip("–-").strip() or None
                al_mando_raw   = tds[13].get_text(strip=True).lstrip("–-").strip() or None
                num_efectivos  = tds[14].get_text(strip=True)
                piloto_nombre  = tds[15].get_text(strip=True).lstrip("–-").strip() or None
                km_salida      = tds[16].get_text(strip=True)
                km_ingreso     = tds[17].get_text(strip=True)

                num_efectivos = int(num_efectivos) if num_efectivos.isdigit() else None
                km_salida     = int(km_salida)     if km_salida.isdigit()     else None
                km_ingreso    = int(km_ingreso)    if km_ingreso.isdigit()    else None

                al_mando_id, _ = _buscar_bombero_al_mando(cur, al_mando_raw)
                tipo_emerg_id  = _buscar_o_crear_tipo_emergencia(cur, tipo_emerg)
                vehiculo_id    = _buscar_vehiculo(cur, cod_texto)
                distrito_id    = _buscar_o_crear_distrito(cur, direccion)

                cur.execute("""
                    INSERT INTO emergencia
                        (numero_parte, tipo, estado, piloto_nombre, numero_efectivos,
                         direccion, observacion, fecha_despacho, fecha_salida,
                         fecha_llegada, fecha_retorno, fecha_ingreso,
                         km_salida, km_ingreso, al_mando_id, tipo_emergencia_id, distrito_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (numero_parte) DO UPDATE SET
                        estado         = CASE WHEN emergencia.estado = 'CANCELADA' THEN 'CANCELADA' ELSE EXCLUDED.estado END,
                        fecha_salida   = COALESCE(EXCLUDED.fecha_salida,   emergencia.fecha_salida),
                        fecha_llegada  = COALESCE(EXCLUDED.fecha_llegada,  emergencia.fecha_llegada),
                        fecha_retorno  = COALESCE(EXCLUDED.fecha_retorno,  emergencia.fecha_retorno),
                        fecha_ingreso  = COALESCE(EXCLUDED.fecha_ingreso,  emergencia.fecha_ingreso),
                        km_salida      = COALESCE(EXCLUDED.km_salida,      emergencia.km_salida),
                        km_ingreso     = COALESCE(EXCLUDED.km_ingreso,     emergencia.km_ingreso),
                        numero_efectivos = COALESCE(EXCLUDED.numero_efectivos, emergencia.numero_efectivos),
                        piloto_nombre  = COALESCE(EXCLUDED.piloto_nombre,  emergencia.piloto_nombre),
                        al_mando_id    = COALESCE(EXCLUDED.al_mando_id,    emergencia.al_mando_id),
                        tipo_emergencia_id = COALESCE(EXCLUDED.tipo_emergencia_id, emergencia.tipo_emergencia_id),
                        distrito_id    = COALESCE(EXCLUDED.distrito_id,    emergencia.distrito_id)
                    RETURNING id, (xmax = 0) AS es_nueva
                """, (numero_parte, tipo_parte, "CERRADO" if fecha_ingreso else "ATENDIENDO",
                      piloto_nombre, num_efectivos, direccion, observacion,
                      fecha_despacho, fecha_salida, fecha_llegada, fecha_retorno, fecha_ingreso,
                      km_salida, km_ingreso, al_mando_id, tipo_emerg_id, distrito_id))

                row = cur.fetchone()
                emergencia_id, es_nueva = row[0], row[1]

                if es_nueva:
                    nuevos += 1
                else:
                    actualizados += 1

                # Vincular vehículo propio
                if vehiculo_id:
                    try:
                        cur.execute("""
                            INSERT INTO emergencia_vehiculo (emergencia_id, vehiculo_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (emergencia_id, vehiculo_id))
                    except Exception:
                        conn.rollback()

            conn.commit()
            print(f"  {cod_texto}: {len(filas)} partes procesados")

        except Exception as e:
            conn.rollback()
            print(f"  ERROR partes {cod_texto}: {e}")

    cur.close()
    print(f"[{datetime.now():%H:%M:%S}] Partes CIA — {nuevos} nuevos | {actualizados} actualizados")


def scrape_partes_cia_rango(session, driver, fecha_inicio: date, fecha_fin: date):
    total_nuevos = total_actualizados = 0
    dia = fecha_inicio
    while dia <= fecha_fin:
        print(f"  Procesando {dia.strftime('%d/%m/%Y')}...")
        cur = conn.cursor()
        nuevos = actualizados = 0

        params = {
            "NivelArbol": "../../../",
            "txtOrden": "numParte", "txtOrdenSentido": "asc", "txtOrdenAnterior": "",
            "txtTotalRegistro": "", "txtCodigoUbigeo": "", "txtCodIdenEst": "",
            "txtCodEstructura": "", "txtDireccion": "", "txtValoresCadenaDependencia": "",
            "opc": "1", "cboTipos": "T",
            "cboMesFechaInicio":  f"{dia.month:02d}",
            "cboAnioFechaInicio": str(dia.year),
            "cboMesFechaFin":     f"{dia.month:02d}",
            "cboAnioFechaFin":    str(dia.year),
            "cboDiaFechaInicio":  str(dia.day),
            "cboDiaFechaFin":     str(dia.day),
            "cboHoraFechaInicio": "0", "cboHoraFechaFin": "23",
            "cboMinutoFechaInicio": "0", "cboMinutoFechaFin": "59",
            "txtTitulo": "ESTADO DE COMPANIAS",
            "cboVehi": "",  # sin filtro de vehículo = todos
        }

        try:
            from browser import login as _login
            for intento in range(3):
                try:
                    driver.get(f"{URL}?{urlencode(params)}")
                    time.sleep(3)
                except Exception:
                    pass
                if "localhost" in driver.current_url or "ini.asp" in driver.current_url or driver.current_url == "data:,":
                    print(f"    Sesión expirada, reconectando...")
                    _login(driver)
                    time.sleep(2)
                else:
                    break

            soup = BeautifulSoup(driver.page_source, "html.parser")
            filas = [tr for tr in soup.find_all("tr") if tr.get("onmouseover") or tr.get("onMouseOver")]

            for fila in filas:
                tds = fila.find_all("td")
                if len(tds) < 18:
                    continue

                tipo_parte   = tds[1].get_text(strip=True)
                numero_parte = tds[2].get_text(strip=True)
                if not numero_parte or numero_parte == "--":
                    continue

                fecha_despacho = _parse_fecha(tds[5].get_text(strip=True))
                fecha_salida   = _parse_fecha(tds[6].get_text(strip=True))
                fecha_llegada  = _parse_fecha(tds[7].get_text(strip=True))
                fecha_retorno  = _parse_fecha(tds[8].get_text(strip=True))
                fecha_ingreso  = _parse_fecha(tds[9].get_text(strip=True))
                tipo_emerg     = tds[10].get_text(strip=True).lstrip("–-").strip() or None
                observacion    = tds[11].get_text(strip=True).lstrip("–-").strip() or None
                direccion      = tds[12].get_text(strip=True).lstrip("–-").strip() or None
                al_mando_raw   = tds[13].get_text(strip=True).lstrip("–-").strip() or None
                num_efectivos  = tds[14].get_text(strip=True)
                piloto_nombre  = tds[15].get_text(strip=True).lstrip("–-").strip() or None
                km_salida      = tds[16].get_text(strip=True)
                km_ingreso     = tds[17].get_text(strip=True)

                num_efectivos = int(num_efectivos) if num_efectivos.isdigit() else None
                km_salida     = int(km_salida)     if km_salida.isdigit()     else None
                km_ingreso    = int(km_ingreso)    if km_ingreso.isdigit()    else None

                al_mando_id, _ = _buscar_bombero_al_mando(cur, al_mando_raw)
                tipo_emerg_id  = _buscar_o_crear_tipo_emergencia(cur, tipo_emerg)
                distrito_id    = _buscar_o_crear_distrito(cur, direccion)

                # Determinar vehículo por código en tds[4]
                cod_vehi_texto = tds[4].get_text(strip=True)
                vehiculo_id    = _buscar_vehiculo(cur, cod_vehi_texto)

                estado = "CERRADO" if fecha_ingreso else "ATENDIENDO"

                cur.execute("""
                    INSERT INTO emergencia
                        (numero_parte, tipo, estado, piloto_nombre, numero_efectivos,
                         direccion, observacion, fecha_despacho, fecha_salida,
                         fecha_llegada, fecha_retorno, fecha_ingreso,
                         km_salida, km_ingreso, al_mando_id, tipo_emergencia_id, distrito_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (numero_parte) DO UPDATE SET
                        estado             = CASE WHEN emergencia.estado = 'CANCELADA' THEN 'CANCELADA' ELSE EXCLUDED.estado END,
                        direccion          = COALESCE(EXCLUDED.direccion,        emergencia.direccion),
                        fecha_salida       = COALESCE(EXCLUDED.fecha_salida,     emergencia.fecha_salida),
                        fecha_llegada      = COALESCE(EXCLUDED.fecha_llegada,    emergencia.fecha_llegada),
                        fecha_retorno      = COALESCE(EXCLUDED.fecha_retorno,    emergencia.fecha_retorno),
                        fecha_ingreso      = COALESCE(EXCLUDED.fecha_ingreso,    emergencia.fecha_ingreso),
                        km_salida          = COALESCE(EXCLUDED.km_salida,        emergencia.km_salida),
                        km_ingreso         = COALESCE(EXCLUDED.km_ingreso,       emergencia.km_ingreso),
                        numero_efectivos   = COALESCE(EXCLUDED.numero_efectivos, emergencia.numero_efectivos),
                        piloto_nombre      = COALESCE(EXCLUDED.piloto_nombre,    emergencia.piloto_nombre),
                        al_mando_id        = COALESCE(EXCLUDED.al_mando_id,      emergencia.al_mando_id),
                        tipo_emergencia_id = COALESCE(EXCLUDED.tipo_emergencia_id, emergencia.tipo_emergencia_id),
                        distrito_id        = COALESCE(EXCLUDED.distrito_id,      emergencia.distrito_id)
                    RETURNING id, (xmax = 0) AS es_nueva
                """, (numero_parte, tipo_parte, estado,
                      piloto_nombre, num_efectivos, direccion, observacion,
                      fecha_despacho, fecha_salida, fecha_llegada, fecha_retorno, fecha_ingreso,
                      km_salida, km_ingreso, al_mando_id, tipo_emerg_id, distrito_id))

                row = cur.fetchone()
                emergencia_id, es_nueva = row[0], row[1]
                if es_nueva:
                    nuevos += 1
                else:
                    actualizados += 1

                if vehiculo_id:
                    try:
                        cur.execute("""
                            INSERT INTO emergencia_vehiculo (emergencia_id, vehiculo_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (emergencia_id, vehiculo_id))
                    except Exception:
                        conn.rollback()

            conn.commit()
            print(f"    {len(filas)} filas — {nuevos} nuevas | {actualizados} actualizadas")
            total_nuevos += nuevos
            total_actualizados += actualizados

        except Exception as e:
            conn.rollback()
            print(f"    ERROR {dia}: {e}")
        finally:
            cur.close()

        dia += timedelta(days=1)
        time.sleep(2)

    print(f"\nHistórico completo — {total_nuevos} nuevos | {total_actualizados} actualizados")
