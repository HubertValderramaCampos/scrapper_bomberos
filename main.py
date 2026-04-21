import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, date, timedelta

from browser import iniciar_driver, login, nueva_session
from scrapers.estado_cia import scrape_estado_cia
from scrapers.partes_cia import scrape_partes_cia
from scrapers.asistencia_mensual import scrape_asistencia_mensual

class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, *_):
        pass

threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 8080), _Health).serve_forever(),
    daemon=True,
).start()

INTERVALO_ESTADO = 2 * 60
INTERVALO_PARTES = 15 * 60
INTERVALO_ASISTENCIA = 6 * 60 * 60  # cada 6 horas (solo actúa si día <= 5)

def iniciar_sesion():
    d = iniciar_driver()
    login(d)
    return d

driver = iniciar_sesion()

ultimo_estado = 0
ultimo_partes = 0
ultimo_asistencia = 0

print(f"[{datetime.now():%H:%M:%S}] Loop iniciado — estado cada 2min | partes cada 15min")
print(f"[{datetime.now():%H:%M:%S}] Para actualizar padrón de bomberos: python actualizar_bomberos.py")

while True:
    ahora = time.time()
    hoy = date.today()
    try:
        if ahora - ultimo_estado >= INTERVALO_ESTADO:
            scrape_estado_cia(nueva_session(driver), driver=driver)
            ultimo_estado = time.time()

        if ahora - ultimo_partes >= INTERVALO_PARTES:
            print(f"[{datetime.now():%H:%M:%S}] Actualizando partes CIA...")
            scrape_partes_cia(nueva_session(driver), driver=driver)
            ultimo_partes = time.time()

        # Asistencia mensual: solo entre día 1 y 5, scrapeando el mes anterior
        if hoy.day <= 5 and ahora - ultimo_asistencia >= INTERVALO_ASISTENCIA:
            mes_anterior = hoy.replace(day=1) - timedelta(days=1)
            print(f"[{datetime.now():%H:%M:%S}] Actualizando asistencia {mes_anterior.month:02d}/{mes_anterior.year}...")
            scrape_asistencia_mensual(driver, mes_anterior.month, mes_anterior.year)
            ultimo_asistencia = time.time()

    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] ERROR crítico: {e}")
        print(f"[{datetime.now():%H:%M:%S}] Reiniciando driver...")
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(10)
        driver = iniciar_sesion()

    time.sleep(30)
