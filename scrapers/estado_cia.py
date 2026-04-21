import re
from datetime import datetime
from bs4 import BeautifulSoup

from db import conn

URL       = "https://www.bomberosperu.gob.pe/extranet/DEPA/CEEM/EstadoCia/CEEMCiaLis.asp"
CODIGO_CIA = "31501980006"

def parse_fecha(texto):
    for fmt in ("%d/%m/%Y %I:%M:%S %p.", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(texto.strip(), fmt)
        except ValueError:
            continue
    return None

def limpiar(td):
    return td.get_text(" ", strip=True).replace("\xa0", "").strip()

def _buscar_bombero(cur, nombre_raw):
    nombre = re.sub(r"\s*\(\d*\)\s*$", "", nombre_raw).strip()
    nombre = re.sub(r"\s+", " ", nombre)
    if "," not in nombre:
        return None
    parte_izq, nombres = nombre.split(",", 1)
    nombres = nombres.strip()
    palabras = parte_izq.strip().split()
    apellidos = " ".join(palabras[-2:]) if len(palabras) >= 2 else palabras[-1]
    cur.execute("""
        SELECT id FROM bombero
        WHERE REGEXP_REPLACE(apellidos, '\\s+', ' ', 'g') ILIKE %s
        AND   REGEXP_REPLACE(nombres,   '\\s+', ' ', 'g') ILIKE %s
        LIMIT 1
    """, (f"%{apellidos}%", f"%{nombres}%"))
    row = cur.fetchone()
    return row[0] if row else None

def _buscar_o_crear_piloto(cur, nombre_raw):
    nombre = re.sub(r"\s*\(\d*\)\s*$", "", nombre_raw).strip()
    nombre = re.sub(r"\s+", " ", nombre)
    if "," in nombre:
        parte_izq, nombres = nombre.split(",", 1)
        nombres = nombres.strip()
        palabras = parte_izq.strip().split()
        apellidos = " ".join(palabras[-2:]) if len(palabras) >= 2 else palabras[-1]
    else:
        apellidos, nombres = nombre, ""
    cur.execute("""
        SELECT id FROM piloto_rentado
        WHERE REGEXP_REPLACE(apellidos, '\\s+', ' ', 'g') ILIKE %s
        AND   REGEXP_REPLACE(nombres,   '\\s+', ' ', 'g') ILIKE %s
        LIMIT 1
    """, (f"%{apellidos}%", f"%{nombres}%"))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO piloto_rentado (apellidos, nombres) VALUES (%s, %s) RETURNING id",
                (apellidos, nombres))
    pid = cur.fetchone()[0]
    print(f"  Nuevo piloto rentado: {apellidos}, {nombres}")
    return pid

def actualizar_estado_bombero(cur, bombero_id, nuevo_estado):
    cur.execute("SELECT estado FROM bombero_estado_actual WHERE bombero_id=%s", (bombero_id,))
    row = cur.fetchone()
    if row:
        if row[0] != nuevo_estado:
            cur.execute("UPDATE bombero_estado_actual SET estado=%s, desde=now() WHERE bombero_id=%s",
                        (nuevo_estado, bombero_id))
            cur.execute("""
                INSERT INTO bombero_historial_estado (bombero_id, estado_anterior, estado_nuevo, fuente)
                VALUES (%s, %s, %s, 'scraper')
            """, (bombero_id, row[0], nuevo_estado))
    else:
        cur.execute("""
            INSERT INTO bombero_estado_actual (bombero_id, estado)
            VALUES (%s, %s)
            ON CONFLICT (bombero_id) DO UPDATE SET estado=EXCLUDED.estado, desde=now()
        """, (bombero_id, nuevo_estado))
        cur.execute("""
            INSERT INTO bombero_historial_estado (bombero_id, estado_anterior, estado_nuevo, fuente)
            VALUES (%s, NULL, %s, 'scraper')
        """, (bombero_id, nuevo_estado))

def scrape_estado_cia(session, driver=None):
    cur = conn.cursor()
    try:
        r = session.get(URL, params={"CodigoCia": CODIGO_CIA}, timeout=30)
        # Detectar redirect a localhost (sesión expirada)
        if "localhost" in r.url or len(r.content) < 500:
            if driver:
                from browser import login as _login, nueva_session as _nueva_session
                _login(driver)
                session = _nueva_session(driver)
                r = session.get(URL, params={"CodigoCia": CODIGO_CIA}, timeout=30)
            else:
                print(f"[{datetime.now():%H:%M:%S}] Estado CIA: sesión expirada, sin driver para renovar")
                return
        soup = BeautifulSoup(r.text, "html.parser")
        tablas = soup.find_all("table")
        if len(tablas) < 3:
            print(f"[{datetime.now():%H:%M:%S}] Estado CIA: respuesta inesperada")
            return

        # Tabla 1: jefes y estado general
        filas_t1 = tablas[0].find_all("tr")
        def celda(fila, idx=1):
            tds = fila.find_all("td")
            return limpiar(tds[idx]) if len(tds) > idx else ""

        primer_jefe    = celda(filas_t1[0]) or None if len(filas_t1) > 0 else None
        segundo_jefe   = celda(filas_t1[1]) or None if len(filas_t1) > 1 else None
        estado_general = celda(filas_t1[2]) or None if len(filas_t1) > 2 else None

        # Tabla 3: disponibilidad
        pilotos = paramedicos = personal = None
        observaciones = informante = fecha_hora = None
        for fila in tablas[2].find_all("tr"):
            tds = fila.find_all("td")
            if len(tds) < 2:
                continue
            label = limpiar(tds[0]).lower()
            valor = limpiar(tds[1])
            if "piloto"    in label: pilotos       = int(valor) if valor.isdigit() else None
            elif "param"   in label: paramedicos    = int(valor) if valor.isdigit() else None
            elif "personal"in label: personal       = int(valor) if valor.isdigit() else None
            elif "observa" in label: observaciones  = valor or None
            elif "informante" in label: informante  = valor or None
            elif "fecha"   in label: fecha_hora     = parse_fecha(valor)

        cur.execute("""
            INSERT INTO estado_compania
                (primer_jefe, segundo_jefe, estado_general,
                 pilotos_disponibles, paramedicos_disponibles, personal_disponible,
                 observaciones, informante, fecha_hora)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (primer_jefe, segundo_jefe, estado_general,
              pilotos, paramedicos, personal,
              observaciones, informante, fecha_hora))
        estado_id = cur.fetchone()[0]

        # Tabla 2: vehículos propios
        for fila in tablas[1].find_all("tr")[2:]:
            inputs = fila.find_all("input")
            if len(inputs) < 7:
                continue
            cod_v    = inputs[0].get("value", "").strip()
            estado_v = inputs[3].get("value", "").strip() or "EN BASE"
            motivo_v = inputs[4].get("value", "").strip() or None
            tipo_v   = inputs[6].get("value", "").strip() or "DESCONOCIDO"
            if not cod_v:
                continue
            try:
                cur.execute("""
                    INSERT INTO vehiculo (codigo, tipo, estado, motivo)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (codigo) DO UPDATE SET
                        estado = EXCLUDED.estado, motivo = EXCLUDED.motivo, tipo = EXCLUDED.tipo
                    RETURNING id
                """, (cod_v, tipo_v, estado_v, motivo_v))
                vehiculo_id = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO estado_compania_vehiculo
                        (estado_compania_id, vehiculo_id, codigo_vehiculo, estado, motivo, tipo_vehiculo)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (estado_id, vehiculo_id, cod_v, estado_v, motivo_v, tipo_v))
            except Exception as e:
                conn.rollback()
                print(f"  ERROR vehiculo {cod_v}: {e}")

        # Tabla 4: asistencia de turno
        bomberos_en_turno = set()
        if len(tablas) >= 4:
            for fila in tablas[3].find_all("tr")[2:]:
                tds = fila.find_all("td")
                if len(tds) < 10:
                    continue
                tipo_ef    = limpiar(tds[0])
                nombre_raw = limpiar(tds[1])
                hora_ing   = limpiar(tds[2]) or None
                es_bom     = "X" in tds[3].get_text()
                es_mando   = "X" in tds[4].get_text()
                es_piloto  = "X" in tds[5].get_text()
                es_medico  = "X" in tds[6].get_text()
                es_appa    = "X" in tds[7].get_text()
                es_map     = "X" in tds[8].get_text()
                es_brec    = "X" in tds[9].get_text()

                bombero_id = piloto_rentado_id = None
                if tipo_ef == "BOM":
                    bombero_id = _buscar_bombero(cur, nombre_raw)
                    if bombero_id:
                        bomberos_en_turno.add(bombero_id)
                        actualizar_estado_bombero(cur, bombero_id, "en_turno")
                elif tipo_ef == "REN":
                    piloto_rentado_id = _buscar_o_crear_piloto(cur, nombre_raw)

                try:
                    cur.execute("""
                        INSERT INTO asistencia_turno
                            (estado_compania_id, bombero_id, piloto_rentado_id, nombre_raw, tipo,
                             hora_ingreso, es_bombero, es_al_mando, es_piloto,
                             es_medico, es_appa, es_map, es_brec)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (estado_id, bombero_id, piloto_rentado_id, nombre_raw, tipo_ef,
                          hora_ing, es_bom, es_mando, es_piloto,
                          es_medico, es_appa, es_map, es_brec))
                except Exception as e:
                    conn.rollback()
                    print(f"  ERROR asistencia {nombre_raw}: {e}")

        # Marcar como franco a quienes no están en turno
        if bomberos_en_turno:
            cur.execute("""
                SELECT bombero_id FROM bombero_estado_actual
                WHERE estado = 'en_turno' AND bombero_id != ALL(%s)
            """, (list(bomberos_en_turno),))
            for (bid,) in cur.fetchall():
                actualizar_estado_bombero(cur, bid, "franco")

        conn.commit()
        print(f"[{datetime.now():%H:%M:%S}] Estado CIA — {estado_general} | personal: {personal} | vehículos: {len(tablas[1].find_all('tr'))-2}")

    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now():%H:%M:%S}] ERROR estado CIA: {e}")
    finally:
        cur.close()
