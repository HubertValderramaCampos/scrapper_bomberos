"""
Vuelve a leer del portal los días de todos los partes que siguen en ATENDIENDO,
para cerrar los que ya tienen ingreso a base registrado.

El portal permite UNA sola sesión por cuenta: detén el scraper principal antes
de correr esto (docker compose stop), o se desloguearán mutuamente.

Uso: python reparar_atendiendo.py
"""
from browser import iniciar_driver, login, nueva_session
from db import conn
from scrapers.partes_cia import scrape_partes_cia

cur = conn.cursor()
cur.execute("""
    SELECT DISTINCT COALESCE(fecha_despacho, fecha_salida)::date
    FROM emergencia
    WHERE estado = 'ATENDIENDO' AND COALESCE(fecha_despacho, fecha_salida) IS NOT NULL
    ORDER BY 1
""")
dias = [r[0] for r in cur.fetchall()]
cur.execute("SELECT count(*) FROM emergencia WHERE estado = 'ATENDIENDO'")
antes = cur.fetchone()[0]
cur.close()

print(f"{antes} partes en ATENDIENDO, repartidos en {len(dias)} días. Revisando...")

driver = iniciar_driver()
try:
    login(driver)
    scrape_partes_cia(nueva_session(driver), driver=driver, dias=dias)
finally:
    driver.quit()

cur = conn.cursor()
cur.execute("SELECT count(*) FROM emergencia WHERE estado = 'ATENDIENDO'")
despues = cur.fetchone()[0]
cur.close()
print(f"ATENDIENDO: {antes} -> {despues}")
