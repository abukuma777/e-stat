import os
import time
import zipfile

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ダウンロード先のフォルダパスを指定
download_dir = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(download_dir):
    os.makedirs(download_dir)  # フォルダがない場合は作成

# Chromeオプションを設定してダウンロード先を指定し、複数ファイルのダウンロードを自動許可
chrome_options = Options()
prefs = {
    "download.default_directory": download_dir,  # ダウンロード先のパスを指定
    "download.prompt_for_download": False,  # ダウンロード確認のポップアップを無効化
    "profile.default_content_setting_values.automatic_downloads": 1,  # 複数ファイルのダウンロードを許可
    "directory_upgrade": True,
    "safebrowsing.enabled": True,  # セーフブラウジングを有効化
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--disable-cache")  # キャッシュを無効化

# WebDriverの起動
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# サイトのURL
url = "https://www.e-stat.go.jp/gis/statmap-search?page=1&type=1&toukeiCode=00200553"
driver.get(url)


def wait_for_new_file_in_directory(download_dir, timeout=120, check_interval=0.5):
    """新しいファイルがダウンロードされるまで待機"""
    initial_files = set(os.listdir(download_dir))  # ダウンロードフォルダ内の初期ファイルセット
    start_time = time.time()

    while True:
        current_files = set(os.listdir(download_dir))  # 現在のファイルセット
        new_files = current_files - initial_files  # 新しく追加されたファイルを確認

        # 新しく追加された.zipファイルがあれば、それがダウンロードされたファイル
        if any(file.endswith(".zip") for file in new_files):
            return True
        if time.time() - start_time > timeout:
            raise TimeoutError("ダウンロードがタイムアウトしました。")

        # 待機時間を 0.5 秒に短縮
        time.sleep(check_interval)


def download_files_from_page():
    """現在のページからCSVファイルをダウンロード"""
    # すべてのCSVダウンロードリンクを取得
    csv_links = driver.find_elements(
        By.XPATH, "//a[contains(@class, 'stat-dl_icon') and contains(@href, 'downloadType=2')]"
    )

    if csv_links:
        for index, csv_link in enumerate(csv_links, start=1):
            csv_href = csv_link.get_attribute("href")
            # print(f"{index} - CSVリンクを取得しました: {csv_href}")

            # JavaScriptでCSVリンクをクリックしてダウンロード
            driver.execute_script(f"window.location.href='{csv_href}';")

            # ダウンロードが完了するまで待機（新しいファイルがダウンロードされるのを確認）
            wait_for_new_file_in_directory(download_dir)
            print(f"{index} - CSVファイルをダウンロードしました: {csv_href}")
    else:
        print("CSVリンクが見つかりませんでした。")


def navigate_to_next_page(page_number):
    """指定されたページ番号に移動"""
    try:
        next_page_button = driver.find_element(By.XPATH, f"//span[@data-page='{page_number}']")
        driver.execute_script("arguments[0].scrollIntoView();", next_page_button)
        next_page_button.click()
        print(f"{page_number} ページに移動しました。")
        time.sleep(3)  # ページが読み込まれるのを待機
    except Exception as e:
        print(f"{page_number} ページに移動できませんでした: {e}")


def get_total_pages():
    """ページネーションから総ページ数を動的に取得"""
    try:
        # 最後のページ番号を取得する
        last_page_element = driver.find_element(By.XPATH, "//span[@class='stat-paginate-last js-gisdownload-tabindex']")
        total_pages = int(last_page_element.get_attribute("data-page"))
        print(f"総ページ数: {total_pages}")
        return total_pages
    except Exception as e:
        print(f"ページ数の取得に失敗しました: {e}")
        return 1  # デフォルトで1ページ


try:
    # ページが完全に読み込まれるまで待機
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'2021年')]")))

    # 2021年をクリック
    year_2021 = driver.find_element(By.XPATH, "//span[contains(text(),'2021年')]")
    driver.execute_script("arguments[0].scrollIntoView();", year_2021)
    year_2021.click()

    # ページ遷移後の待機
    time.sleep(3)

    # "4次メッシュ（500mメッシュ）" の要素をクリック
    mesh_500m_link = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//span[@data-value2='4次メッシュ（500mメッシュ）']"))
    )
    driver.execute_script("arguments[0].scrollIntoView();", mesh_500m_link)
    mesh_500m_link.click()

    # 新しいウィンドウやタブが開くのを待機
    time.sleep(3)

    # "産業（大分類）別事業所数及び従業者数（JGD2000）" のリンクの href を直接取得して移動
    industry_link = driver.find_element(
        By.XPATH, "//a[contains(text(),'産業（大分類）別事業所数及び従業者数（JGD2000）')]"
    )
    industry_url = industry_link.get_attribute("href")

    # JavaScriptを使用してリンクに直接遷移
    driver.execute_script(f"window.location.href='{industry_url}';")
    print(f"リンクに直接遷移しました: {industry_url}")

    # ページ遷移後に何か確認したい場合（例：新しい要素の待機）
    time.sleep(3)  # ページが読み込まれるのを待機

    # 最初のページからCSVファイルをダウンロード
    download_files_from_page()

    # ページ数を動的に取得
    total_pages = get_total_pages()

    # すべてのページを順番に処理
    for page_number in range(2, total_pages + 1):
        navigate_to_next_page(page_number)
        download_files_from_page()

    print(f"すべてのCSVファイルが {download_dir} にダウンロードされました。")

except Exception as e:
    print(f"エラーが発生しました: {e}")

finally:
    driver.quit()
