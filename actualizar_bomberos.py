from browser import iniciar_driver, login
from scrapers.bomberos import scrape_bomberos

driver = iniciar_driver()
login(driver)
scrape_bomberos(driver)
driver.quit()
