import time
from datetime import datetime
from urllib.parse import urlencode
from bs4 import BeautifulSoup

from db import conn

URL = "http://www.bomberosperu.gob.pe/extranet/depa/ceem/asistencia_bomberos/CEEMAsisLis.asp"

def scrape_asistencia_mensual(driver, mes: int, anio: int):
    cur = conn.cursor()
    nuevos = actualizados = 0

    params = {
        "NivelArbol": "../../../",
        "txtOrden": "numParte",
        "txtOrdenSentido": "asc",
        "txtOrdenAnterior": "",
        "txtCodIdenEst": "",
        "chk": "checkbox",
        "txtCodEstructura": "",
        "txtValoresCadenaDependencia": "",
        "txtCodigoUbigeo": "",
        "cboMes": str(mes),
        "cboAnio": str(anio),
        "txtTitulo": "ESTADO DE COMPANIAS",
        "opc": "1",
    }

    try:
        driver.get(f"{URL}?{urlencode(params)}")
        time.sleep(3)
        if "localhost" in driver.current_url or "ini.asp" in driver.current_url:
            from browser import login as _login
            _login(driver)
            driver.get(f"{URL}?{urlencode(params)}")
            time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        filas = soup.select("table tr")

        for fila in filas:
            tds = fila.find_all("td")
            if len(tds) < 8:
                continue
            # Saltar filas de encabezado (contienen "CODIGO" o "N.")
            primer_texto = tds[0].get_text(strip=True)
            if not primer_texto.isdigit():
                continue

            codigo    = tds[1].get_text(strip=True).strip()
            if not codigo:
                continue

            dias_asistidos  = tds[4].get_text(strip=True)
            dias_guardia    = tds[5].get_text(strip=True)
            horas_acum      = tds[6].get_text(strip=True)
            num_emergencias = tds[7].get_text(strip=True)

            dias_asistidos  = int(dias_asistidos)  if dias_asistidos.isdigit()  else 0
            dias_guardia    = int(dias_guardia)     if dias_guardia.isdigit()    else 0
            horas_acum      = int(horas_acum)       if horas_acum.isdigit()      else 0
            num_emergencias = int(num_emergencias)  if num_emergencias.isdigit() else 0

            # Buscar bombero por código
            cur.execute("SELECT id FROM bombero WHERE codigo = %s", (codigo,))
            row = cur.fetchone()
            if not row:
                continue
            bombero_id = row[0]

            cur.execute("""
                INSERT INTO asistencia_mensual
                    (bombero_id, mes, anio, dias_asistidos, dias_guardia, horas_acumuladas, num_emergencias)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bombero_id, mes, anio) DO UPDATE SET
                    dias_asistidos  = EXCLUDED.dias_asistidos,
                    dias_guardia    = EXCLUDED.dias_guardia,
                    horas_acumuladas = EXCLUDED.horas_acumuladas,
                    num_emergencias = EXCLUDED.num_emergencias
                RETURNING (xmax = 0) AS es_nuevo
            """, (bombero_id, mes, anio, dias_asistidos, dias_guardia, horas_acum, num_emergencias))

            es_nuevo = cur.fetchone()[0]
            if es_nuevo:
                nuevos += 1
            else:
                actualizados += 1

        conn.commit()
        print(f"[{datetime.now():%H:%M:%S}] Asistencia {mes:02d}/{anio} — {nuevos} nuevos | {actualizados} actualizados")

    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now():%H:%M:%S}] ERROR asistencia mensual: {e}")
    finally:
        cur.close()
