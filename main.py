import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, date, timedelta

from browser import iniciar_driver, login, nueva_session
from scrapers.estado_cia import scrape_estado_cia
from scrapers.partes_cia import scrape_partes_cia, dias_pendientes
from db import conn
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

# Intervalos pensados para no cargar la PC: cada lectura abre páginas en Chromium.
INTERVALO_ESTADO = 5 * 60              # estado de la compañía
INTERVALO_PARTES = 15 * 60             # partes de hoy
INTERVALO_PARTES_PENDIENTES = 3 * 60 * 60  # días anteriores con partes aún en ATENDIENDO
INTERVALO_ASISTENCIA = 12 * 60 * 60    # solo los días 1-5 (mes anterior ya cerrado)
PAUSA_LOOP = 60

def iniciar_sesion():
    d = iniciar_driver()
    login(d)
    return d

driver = iniciar_sesion()

ultimo_estado = 0
ultimo_partes = 0
ultimo_pendientes = 0
ultimo_asistencia = 0

print(f"[{datetime.now():%H:%M:%S}] Loop iniciado — estado cada 5min | partes hoy cada 15min | pendientes cada 3h")
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
            scrape_partes_cia(nueva_session(driver), driver=driver, dias=[hoy])
            ultimo_partes = time.time()

        # Ayer + días con partes abiertos: rara vez cambian, basta cada 3h.
        if ahora - ultimo_pendientes >= INTERVALO_PARTES_PENDIENTES:
            cur = conn.cursor()
            dias = dias_pendientes(cur, hoy)
            cur.close()
            if dias:
                print(f"[{datetime.now():%H:%M:%S}] Revisando partes pendientes: {', '.join(f'{d:%d/%m}' for d in dias)}")
                scrape_partes_cia(nueva_session(driver), driver=driver, dias=dias)
            ultimo_pendientes = time.time()

        # Asistencia: el mes en curso siempre responde "Mes no cerrado", así que
        # solo se pide el mes anterior durante los días 1-5.
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

    time.sleep(PAUSA_LOOP)
