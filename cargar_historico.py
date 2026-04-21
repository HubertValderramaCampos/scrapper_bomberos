"""
Carga histórica de partes de emergencia — últimos 2 meses.
Uso: python cargar_historico.py
"""
from browser import iniciar_driver, login, nueva_session
from scrapers.partes_cia import scrape_partes_cia_rango
from datetime import date, timedelta

fecha_fin   = date.today()
fecha_inicio = fecha_fin - timedelta(days=60)

print(f"Cargando partes desde {fecha_inicio} hasta {fecha_fin}...")

driver = iniciar_driver()
login(driver)
scrape_partes_cia_rango(nueva_session(driver), driver, fecha_inicio, fecha_fin)
driver.quit()
print("Carga histórica completada.")
