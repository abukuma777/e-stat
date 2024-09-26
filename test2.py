from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# WebDriverの起動
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# Googleを開く
driver.get("https://www.google.com")

# タイトルを表示
print(driver.title)

# WebDriverを閉じる
driver.quit()
