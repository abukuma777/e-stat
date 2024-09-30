import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ベースのダウンロードフォルダのパス
downloads_dir = os.path.join(os.getcwd(), "downloads")

# 一時ダウンロードフォルダのパス
tmp_dir = os.path.join(os.getcwd(), "tmp")


# Chromeオプションを設定して一時ダウンロード先を指定
def setup_chrome_options(download_dir):
    chrome_options = Options()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return chrome_options


# Moving the files from tmp_dir to year_folder
def move_files_to_year_folder(year, tmp_dir):
    """Move downloaded files from temporary folder to the year-specific folder."""
    year_folder = os.path.join("downloads", year)

    if not os.path.exists(year_folder):
        os.makedirs(year_folder)

    # Iterate over files in tmp_dir and move them to year_folder
    for file_name in os.listdir(tmp_dir):  # tmp_dir should be a string path
        src_file = os.path.join(tmp_dir, file_name)
        dest_file = os.path.join(year_folder, file_name)
        try:
            shutil.move(src_file, dest_file)
        except Exception as e:
            print(f"Failed to move {file_name}: {e}")


# Clear the tmp_dir after moving files
def clear_tmp_folder(tmp_dir):
    """Clear the temporary download folder after moving files."""
    try:
        shutil.rmtree(tmp_dir)
        print(f"Temporary folder {tmp_dir} cleared.")
    except Exception as e:
        print(f"Failed to clear temporary folder: {e}")


# 新しいファイルが tmp フォルダにダウンロードされるまで待機する関数
def wait_for_new_file_in_directory(download_dir, timeout=120, check_interval=0.5):
    """新しいファイルがダウンロードされるまで待機"""
    initial_files = set(os.listdir(download_dir))
    start_time = time.time()

    while True:
        current_files = set(os.listdir(download_dir))
        new_files = current_files - initial_files

        if any(file.endswith(".zip") for file in new_files):
            return new_files  # 新しいファイルを返す
        if time.time() - start_time > timeout:
            raise TimeoutError("ダウンロードがタイムアウトしました。")

        time.sleep(check_interval)


def download_csv_file(csv_link, tmp_dir, index):
    """CSVリンクをクリックしてファイルをダウンロード."""
    try:
        # Scroll the element into view to make sure it's clickable
        driver.execute_script("arguments[0].scrollIntoView();", csv_link)

        # Click on the link to trigger the download
        csv_link.click()

        # Wait for the download to complete
        wait_for_new_file_in_directory(tmp_dir)
        # print(f"{index} - CSV file downloaded successfully.")
    except Exception as e:
        print(f"Error downloading file {index}: {e}")


def download_files_from_page(tmp_dir):
    """Download CSV files from the current page using parallel processing."""
    try:
        # Wait for the resource list body containing CSV links to be present
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "stat-resorce_list-body")))

        # Find all the download links
        csv_links = driver.find_elements(
            By.XPATH,
            "//div[@class='stat-resorce_list-body']//a[contains(@class, 'stat-dl_icon') and span[contains(text(), 'CSV')]]",
        )
        # tmp_dirを作成する．
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        if csv_links:
            downloaded_files = []  # To track the downloaded files

            # Get half the number of CPU cores
            cpu_count = os.cpu_count() or 1  # Fallback to 1 if CPU count can't be determined
            max_workers = max(1, cpu_count // 2)  # Ensure at least 1 worker

            print(f"Using {max_workers} threads for downloading.")

            # Create a thread pool for downloading files in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_link = {
                    executor.submit(download_csv_file, csv_link, tmp_dir, index): index
                    for index, csv_link in enumerate(csv_links, start=1)
                }

                # Wait for the download to complete for all threads
                for future in as_completed(future_to_link):
                    index = future_to_link[future]
                    try:
                        future.result()
                        downloaded_files.append(index)  # Track successful downloads
                    except Exception as e:
                        print(f"Error in downloading file {index}: {e}")

            return downloaded_files
        else:
            print("No CSV links found.")
            return []

    except Exception as e:
        print(f"Download error: {e}")
        return []


# ページが完全に読み込まれるのを待つ関数
def wait_for_page_to_load(driver, timeout=30):
    """ページが完全に読み込まれるのを待機"""
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    print("ページが読み込まれました")


def get_industry_link_for_year(year_text):
    """年度に応じた産業リンクを動的に取得"""
    try:
        # 年の部分から "年" を削除する
        clean_year = year_text.replace("年", "")

        # 2012年の場合、異なるXPathを使用
        if clean_year == "2012":
            xpath = f"//a[contains(text(),'事業所数及び従業者数') and contains(@href, 'toukeiYear={clean_year}') and contains(@href, 'aggregateUnit=H')]"
        else:
            # 通常のXPathを使用
            xpath = f"//a[contains(text(),'産業（大分類）別事業所数及び従業者数') and contains(@href, 'toukeiYear={clean_year}') and contains(@href, 'aggregateUnit=H')]"

        print(f"調査中のXPath: {xpath}")

        # 指定されたXPathに基づいてリンクを取得
        industry_link = driver.find_element(By.XPATH, xpath)
        return industry_link.get_attribute("href")
    except Exception as e:
        print(f"{year_text} のリンクが見つかりませんでした: {e}")
        return None


# TODO: ここのアイコンクリックに問題が出ている模様(処理上では問題なし)
def click_plus_icon(mesh_type):
    """特定のメッシュを展開するプラスアイコンをクリック"""
    try:
        plus_icon = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, f"//span[@data-value2='{mesh_type}']"))
        )
        driver.execute_script("arguments[0].scrollIntoView();", plus_icon)
        plus_icon.click()
        print(f"プラスアイコンをクリックしました: {mesh_type}")
    except Exception as e:
        print(f"プラスアイコンをクリックできませんでした: {e}")


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
        last_page_element = driver.find_element(By.XPATH, "//span[@class='stat-paginate-last js-gisdownload-tabindex']")
        total_pages = int(last_page_element.get_attribute("data-page"))
        return total_pages
    except Exception as e:
        print(f"ページ数の取得に失敗しました: {e}")
        return 1


try:
    # サイトのURL
    url = "https://www.e-stat.go.jp/gis/statmap-search?page=1&type=1&toukeiCode=00200553"

    # Chromeドライバーのセットアップ
    chrome_options = setup_chrome_options(tmp_dir)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)

    WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.XPATH, "//span[contains(text(),'年')]")))

    # "年" を含む要素を取得して、空白でないテキストのみをリストに保存
    years_texts = [
        year.text for year in driver.find_elements(By.XPATH, "//span[contains(text(),'年')]") if year.text.strip()
    ]

    for year_text in years_texts:
        clean_year = year_text.replace("年", "")
        print(f"クリックする年: {year_text}")

        # 年のリンクがクリック可能になるまで待つ
        year_element = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, f"//span[contains(text(),'{year_text}')]"))
        )
        driver.execute_script("arguments[0].scrollIntoView();", year_element)
        year_element.click()

        # ページが完全に読み込まれるのを待つ
        wait_for_page_to_load(driver)
        # TODO: ここのアイコンクリックに問題が出ている模様(処理上では問題なし)
        # 2021だけ問題なくクリックできる
        # 4次メッシュ（500mメッシュ）に対応するプラスアイコンをクリック
        click_plus_icon("4次メッシュ（500mメッシュ）")

        # 各年に対応する産業リンクを取得し、ページに遷移
        industry_url = get_industry_link_for_year(year_text)
        if industry_url:
            driver.execute_script(f"window.location.href='{industry_url}';")
            print(f"リンクに直接遷移しました: {industry_url}")

            # ページの完全な読み込みを待つ
            wait_for_page_to_load(driver)

            # -------------- DL -------------------
            # 1ページ目のcsvをDL
            download_files_from_page(tmp_dir)
            # ページ数を動的に取得
            total_pages = get_total_pages()
            # すべてのページを順番に処理
            for page_number in range(2, total_pages + 1):
                navigate_to_next_page(page_number)
                download_files_from_page(tmp_dir)

            # ダウンロードしたファイルを年度ごとのフォルダに移動
            move_files_to_year_folder(clean_year, tmp_dir)

            print(f"{year_text}年すべてのCSVファイルがダウンロードされました。")
            # -------------- 元のリンクへ移動する -------------------
            # ページが読み込まれたら、元のURLに戻る
            print(f"{year_text}年の処理が完了し、元のページに戻ります。次の年に進みます。")
            driver.execute_script(f"window.location.href='{url}';")  # 元のページに戻る
            wait_for_page_to_load(driver)

        # ./tmp削除
        clear_tmp_folder(tmp_dir)

except Exception as e:
    print(f"エラーが発生しました: {e}")

finally:
    driver.quit()  # 最後に必ずWebDriverを終了
