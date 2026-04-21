import re
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

from db import conn

URL = "https://www.bomberosperu.gob.pe/extranet/DEPA/BOM/BOMBomLis.asp"

def scrape_bomberos(driver):
    cur = conn.cursor()
    nuevos = total = 0

    driver.get(URL)
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    total_paginas = 1
    total_registros = 0
    js_pag = soup.find("script", string=lambda s: s and "ArmarComboPagina" in s)
    if js_pag:
        m = re.search(r"ArmarComboPagina\(\d+,(\d+),(\d+)\)", js_pag.string)
        if m:
            total_paginas   = int(m.group(1))
            total_registros = int(m.group(2))

    session = requests.Session()
    for cookie in driver.get_cookies():
        session.cookies.set(cookie["name"], cookie["value"])
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": URL})

    params_base = {
        "txtDNI": "", "txtApeMat": "", "NivelArbol": "../../",
        "txtOrden": "apepat", "txtOrdenSentido": "asc", "txtOrdenAnterior": "",
        "txtCodIdenEst": "", "txtCodEstructura": "", "cboEstado": "100",
        "txtValoresCadenaDependencia": "", "txtCodBom": "",
        "txtTitulo": "RELACION DE BOMBEROS", "txtApePat": "", "txtNombres": "",
        "cboGrado": "", "txtTotalPagina": str(total_paginas),
        "txtTotalRegistro": str(total_registros),
    }

    for pagina in range(1, total_paginas + 1):
        if pagina == 1:
            html = driver.page_source
        else:
            for intento in range(3):
                try:
                    html = session.get(
                        URL, params={**params_base, "cboPagina": str(pagina)}, timeout=45
                    ).text
                    break
                except Exception as e:
                    print(f"  Timeout página {pagina}, intento {intento+1}/3: {e}")
                    time.sleep(10)
            else:
                print(f"  Saltando página {pagina} tras 3 intentos fallidos")
                continue

        filas = BeautifulSoup(html, "html.parser").select("table tr[onmouseover]")
        for fila in filas:
            celdas = fila.find_all("td")
            if len(celdas) < 6:
                continue
            codigo     = celdas[2].get_text(strip=True)
            grado      = celdas[3].get_text(strip=True)
            nombre_raw = celdas[4].get_text(strip=True)
            dni        = celdas[5].get_text(strip=True) or None
            if "," in nombre_raw:
                apellidos, nombres = nombre_raw.split(",", 1)
                apellidos = apellidos.strip()
                nombres   = nombres.strip()
            else:
                apellidos, nombres = nombre_raw.strip(), ""
            try:
                cur.execute("""
                    INSERT INTO bombero (codigo, grado, apellidos, nombres, dni)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (codigo) DO UPDATE SET
                        grado     = EXCLUDED.grado,
                        apellidos = EXCLUDED.apellidos,
                        nombres   = EXCLUDED.nombres,
                        dni       = EXCLUDED.dni
                """, (codigo, grado, apellidos, nombres, dni))
                if cur.rowcount == 1:
                    nuevos += 1
                total += 1
            except Exception as e:
                conn.rollback()
                print(f"  ERROR bombero {codigo}: {e}")
        conn.commit()
        print(f"  Página {pagina}/{total_paginas} — {len(filas)} filas")

    cur.close()
    print(f"[{datetime.now():%H:%M:%S}] Bomberos: {total} procesados | {nuevos} nuevos")
