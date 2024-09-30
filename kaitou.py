import multiprocessing
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

# ダウンロード先のフォルダパス
download_dir = os.path.join(os.getcwd(), "downloads")


# フォルダが存在しない場合は作成
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def convert_txt_to_csv(file_path, save_path):
    """
    Converts a TXT file to a CSV file and saves it with UTF-8 encoding (with BOM).
    Assumes the encoding is Shift-JIS for the input TXT.
    """
    df = pd.read_csv(file_path, encoding="shift_jis")  # Assuming Shift-JIS encoding
    df.to_csv(save_path, encoding="utf-8-sig", index=False)  # Save as UTF-8 with BOM


def unzip_file(zip_file_path, origin_dir):
    """
    Unzips a single ZIP file into the origin_dir.
    """
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(origin_dir)


def process_zip_to_csv(zip_file_path, origin_dir, csv_dir):
    """
    Extracts a ZIP file and converts all TXT files to CSV within a specific year's folder.
    """
    # Unzip the file
    unzip_file(zip_file_path, origin_dir)

    # Convert TXT files to CSV
    for txt_file in os.listdir(origin_dir):
        if txt_file.endswith(".txt"):
            txt_file_path = os.path.join(origin_dir, txt_file)
            # Save CSV in the corresponding year's CSV directory
            csv_file_path = os.path.join(csv_dir, f"{os.path.splitext(txt_file)[0]}.csv")
            convert_txt_to_csv(txt_file_path, csv_file_path)


def unzip_and_convert_to_csv_parallel(download_dir):
    """
    Unzips all ZIP files in parallel and converts extracted TXT files to CSV, organizing by year.
    Dynamically uses half the CPU cores for parallelization.
    """
    # Loop through all directories in the downloads folder
    for year in os.listdir(download_dir):
        year_dir = os.path.join(download_dir, year)

        # Ensure it is a directory
        if os.path.isdir(year_dir):
            zip_dir = os.path.join(year_dir, "zip")
            origin_dir = os.path.join(year_dir, "txt_origin")
            csv_dir = os.path.join(year_dir, "csv")

            # Create necessary directories if not exist
            create_directory_if_not_exists(origin_dir)
            create_directory_if_not_exists(csv_dir)

            zip_files = [os.path.join(zip_dir, f) for f in os.listdir(zip_dir) if f.endswith(".zip")]

            # Dynamically set the number of workers based on CPU cores
            num_cores = multiprocessing.cpu_count()
            max_workers = max(1, num_cores // 2)

            # Print the number of CPU cores being used
            print(f"並列化に使用するCPU数: {max_workers} / {num_cores} cores")

            # Using ThreadPoolExecutor to process files in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(process_zip_to_csv, zip_file, origin_dir, csv_dir): zip_file
                    for zip_file in zip_files
                }

                for future in tqdm(
                    as_completed(futures), total=len(futures), desc=f"{year}年のZIPファイルの解凍とCSV変換", unit="file"
                ):
                    zip_file_path = futures[future]
                    try:
                        future.result()
                        print(f"{os.path.basename(zip_file_path)} の処理が完了しました")
                    except Exception as exc:
                        print(f"{os.path.basename(zip_file_path)} の処理中に例外が発生しました: {exc}")

            # Clean up the origin and zip directories after processing
            clean_up_directories([origin_dir, zip_dir])


def clean_up_directories(dirs_to_remove):
    """不要なディレクトリを削除"""
    for directory in dirs_to_remove:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            print(f"削除しました: {directory}")


# ステップ1: ZIPファイルを解凍してtxt_originに保存 (並列処理)
unzip_and_convert_to_csv_parallel(download_dir)

print("処理が完了し、csvディレクトリのみが残りました。")
