import os
import time
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv

load_dotenv()

def iniciar_driver():
    options = Options()
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    if os.getenv("HEADLESS", "1") != "0":
        options.add_argument("--headless=new")
    else:
        options.add_argument("--start-maximized")
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        options.binary_location = chrome_bin
    return webdriver.Chrome(options=options)

def _aceptar_alerts(driver, intentos=5):
    for _ in range(intentos):
        try:
            driver.switch_to.alert.accept()
            time.sleep(0.5)
        except Exception:
            break

def login(driver, intentos=5):
    for i in range(intentos):
        try:
            driver.get("http://www.bomberosperu.gob.pe/extranet/ini.asp")
        except Exception:
            pass
        time.sleep(2)
        _aceptar_alerts(driver)
        try:
            driver.find_element(By.NAME, "txtUsuario").clear()
            driver.find_element(By.NAME, "txtUsuario").send_keys(os.getenv("USUARIO_INTRANET"))
            driver.find_element(By.NAME, "txtContrasenia").clear()
            driver.find_element(By.NAME, "txtContrasenia").send_keys(os.getenv("CONTRASEÑA_INTRANET"))
            driver.find_element(By.CSS_SELECTOR, "input.Boton[value='ACEPTAR']").click()
            time.sleep(4)
            _aceptar_alerts(driver)
            if "bienvenida" in driver.current_url:
                print(f"[{datetime.now():%H:%M:%S}] Login OK: {driver.current_url}")
                return
        except Exception as e:
            print(f"[{datetime.now():%H:%M:%S}] Login intento {i+1}/{intentos} falló: {e}")
            time.sleep(10)
    print(f"[{datetime.now():%H:%M:%S}] Login falló tras {intentos} intentos")

def nueva_session(driver):
    s = requests.Session()
    for cookie in driver.get_cookies():
        s.cookies.set(cookie["name"], cookie["value"])
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s
