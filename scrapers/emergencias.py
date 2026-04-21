import requests
from datetime import datetime
from bs4 import BeautifulSoup

from db import conn

URL = "https://sgonorte.bomberosperu.gob.pe/24horas"

def _parse_fecha(texto):
    for fmt in ("%d/%m/%Y %I:%M:%S %p.", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(texto.strip(), fmt)
        except ValueError:
            continue
    return None

def scrape_24horas():
    cur = conn.cursor()
    try:
        r = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        nuevas = actualizadas = 0

        for fila in soup.select("table tbody tr"):
            tds = fila.find_all("td")
            if len(tds) < 6:
                continue

            numero_parte = tds[0].get_text(strip=True)
            if not numero_parte:
                continue

            fecha_dt  = _parse_fecha(tds[1].get_text(strip=True))
            tipo      = tds[3].get_text(strip=True)
            badge     = tds[4].find("span", string=True)
            estado    = badge.get_text(strip=True) if badge else tds[4].get_text(strip=True)
            maquinas  = [li.get_text(strip=True) for li in tds[5].find_all("li")]

            cur.execute("""
                INSERT INTO emergencia (numero_parte, tipo, estado, fecha_despacho)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (numero_parte) DO UPDATE SET
                    estado         = EXCLUDED.estado,
                    tipo           = EXCLUDED.tipo,
                    fecha_despacho = COALESCE(emergencia.fecha_despacho, EXCLUDED.fecha_despacho)
                RETURNING id, (xmax = 0) AS es_nueva
            """, (numero_parte, tipo, estado, fecha_dt))
            row = cur.fetchone()
            emergencia_id, es_nueva = row[0], row[1]

            if es_nueva:
                nuevas += 1
            else:
                actualizadas += 1

            for cod in maquinas:
                if not cod:
                    continue
                try:
                    cur.execute("""
                        INSERT INTO emergencia_vehiculo_externo (emergencia_id, codigo_vehiculo)
                        VALUES (%s, %s)
                        ON CONFLICT (emergencia_id, codigo_vehiculo) DO NOTHING
                    """, (emergencia_id, cod))
                except Exception as e:
                    conn.rollback()
                    print(f"  ERROR vehiculo externo {cod}: {e}")

        conn.commit()
        print(f"[{datetime.now():%H:%M:%S}] SGO Norte — {nuevas} nuevas | {actualizadas} actualizadas")

    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now():%H:%M:%S}] ERROR SGO Norte: {e}")
    finally:
        cur.close()
