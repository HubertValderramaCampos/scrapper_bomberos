"""
Carga histórica de asistencia mensual.
Uso: python cargar_asistencia_historica.py
"""
from datetime import date, timedelta
from browser import iniciar_driver, login
from scrapers.asistencia_mensual import scrape_asistencia_mensual

driver = iniciar_driver()
login(driver)

# Últimos 6 meses (sin incluir el mes actual)
hoy = date.today()
mes_actual = hoy.replace(day=1)

meses = []
cursor = mes_actual
for _ in range(4):
    cursor = cursor - timedelta(days=1)
    cursor = cursor.replace(day=1)
    meses.append((cursor.month, cursor.year))

meses.reverse()

print(f"Cargando asistencia de {len(meses)} meses...")
for mes, anio in meses:
    scrape_asistencia_mensual(driver, mes, anio)

driver.quit()
print("Carga histórica de asistencia completada.")
