"""Estado en tiempo real de las unidades (SGO interno → "Vehículos por Comandancia").

Usa la MISMA sesión del scraper (el portal permite una sola sesión por cuenta).
El resultado queda en memoria y lo sirve el HTTP interno en GET /unidades, para
que Alerta 116 sepa qué unidades están EN EMERGENCIA en cada comandancia sin
iniciar una segunda sesión.
"""
import threading
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

URL = "https://www.bomberosperu.gob.pe/sgo/ceem/SGO_CEEM_CDVehiculos.asp"

# Comandancias de Lima Metropolitana y Callao
COMANDANCIAS = {
    "204000": "IV CD Lima",
    "224000": "XXIV CD Lima Sur",
    "225000": "XXV CD Lima Norte",
    "228000": "XXVIII CD Lima Este",
    "205000": "V CD Callao",
}

COLOR_ESTADO = {
    "#00CC66": "DISPONIBLE",
    "#FF0000": "NO_DISPONIBLE",
    "#FFFF00": "EN_EMERGENCIA",
    "#999999": "TALLER",
}

_lock = threading.Lock()
_estado = {"actualizado": None, "ok": False, "error": None, "unidades": []}


def estado_actual():
    with _lock:
        return dict(_estado)


class SesionVencida(Exception):
    pass


def _leer_comandancia(session, cd):
    try:
        r = session.post(URL, data={"cboCD": cd}, timeout=25)
    except requests.exceptions.ConnectionError as e:
        # Con la sesión vencida el portal redirige a http://localhost/...
        if "localhost" in str(e):
            raise SesionVencida() from e
        raise
    if "localhost" in r.url or "ini.asp" in r.url:
        raise SesionVencida()
    soup = BeautifulSoup(r.text, "html.parser")
    unidades = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue
        cia = tds[0].get_text(strip=True)
        if not cia.startswith("B-"):
            continue
        for td in tds[1:]:
            codigo = td.get_text(strip=True)
            color = (td.get("bgcolor") or "").upper()
            if codigo and color in COLOR_ESTADO:
                unidades.append({"cd": cd, "cia": cia, "unidad": codigo, "estado": COLOR_ESTADO[color]})
    return unidades


def scrape_unidades_sgo(session, relogin=None):
    """Lee todas las comandancias. Si la sesión venció, llama a relogin() una vez y reintenta."""
    inicio = time.time()
    try:
        try:
            todas = [u for cd in COMANDANCIAS for u in _leer_comandancia(session, cd)]
        except SesionVencida:
            if relogin is None:
                raise
            session = relogin()
            todas = [u for cd in COMANDANCIAS for u in _leer_comandancia(session, cd)]
        # Una unidad puede repetirse si la página anida tablas: se deja una por código
        unicas = {u["unidad"]: u for u in todas}
        lista = sorted(unicas.values(), key=lambda u: (u["cd"], u["cia"], u["unidad"]))
        with _lock:
            _estado.update(actualizado=datetime.now().astimezone().isoformat(), ok=True, error=None, unidades=lista)
        en_emergencia = sum(1 for u in lista if u["estado"] == "EN_EMERGENCIA")
        print(f"[{datetime.now():%H:%M:%S}] Unidades SGO: {len(lista)} | en emergencia: {en_emergencia} ({time.time() - inicio:.1f}s)")
    except Exception as e:
        with _lock:
            _estado.update(ok=False, error=str(e)[:200])
        print(f"[{datetime.now():%H:%M:%S}] ERROR unidades SGO: {e}")
