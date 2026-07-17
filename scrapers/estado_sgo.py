import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

# Esta vista (SGO) sí cruza la disponibilidad real de la unidad contra el
# personal/pilotos/paramédicos de turno, a diferencia de la vista vieja
# EstadoCia que usa el resto del scraper — ahí una unidad puede seguir
# marcada "EN BASE" aunque no haya nadie para tripularla. La usamos solo
# para completar el motivo cuando la vista vieja no dio ninguno.
URL = "https://www.bomberosperu.gob.pe/sgo/ceem/SGO_CEEM_CDVehiculos.asp"
CD_LIMA_NORTE = "225000"
CODIGO_CIA_TEXTO = "B-150"

COLOR_ESTADO = {
    "#00CC66": "DISPONIBLE",
    "#FF0000": "NO_DISPONIBLE",
    "#FFFF00": "EN_EMERGENCIA",
    "#999999": "TALLER",
}

def obtener_disponibilidad_sgo(driver):
    """Devuelve {codigo_vehiculo: 'DISPONIBLE'|'NO_DISPONIBLE'|'EN_EMERGENCIA'|'TALLER'}
    para las unidades de la Cía B-150, según el color real que el portal les asigna
    en esta vista. Si algo falla, devuelve {} y el llamador sigue con la lógica vieja."""
    try:
        driver.get(URL)
        time.sleep(3)
        Select(driver.find_element(By.NAME, "cboCD")).select_by_value(CD_LIMA_NORTE)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        fila = None
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if tds and tds[0].get_text(strip=True) == CODIGO_CIA_TEXTO:
                fila = tr
                break
        if fila is None:
            print(f"[{datetime.now():%H:%M:%S}] SGO: no se encontró la fila de {CODIGO_CIA_TEXTO}")
            return {}

        resultado = {}
        for td in fila.find_all("td", attrs={"bgcolor": True}):
            codigo = td.get_text(strip=True)
            color  = (td.get("bgcolor") or "").upper()
            if codigo:
                resultado[codigo] = COLOR_ESTADO.get(color, "DESCONOCIDO")
        return resultado

    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] ERROR leyendo disponibilidad SGO: {e}")
        return {}
