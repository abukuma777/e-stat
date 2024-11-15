## 実行方法

1. **download_economic_census_activity.py**
   - `download_economic_census_activity.py`は、指定された年度ごとのZIPファイルをダウンロードするスクリプトです。[経済センサス－活動調査](https://www.e-stat.go.jp/gis/statmap-search?page=1&type=1&toukeiCode=00200553)データを自動で取得し、保存先に年度別に整理されたZIPファイルをダウンロードします。

   実行コマンド:
   ```bash
   python download_economic_census_activity.py
   python download_population_census_mesh.py
   ```

2. **kaitou.py**
   - `kaitou.py`は、`download_economic_census_activity.py`でダウンロードされたZIPファイルを解凍し、必要なデータを処理・変換します。具体的には、ZIPファイル内のテキストデータをCSVに変換し、整理されたデータを使える形式に変換します。

   実行コマンド:
   ```bash
   python kaitou.py
   ```

---

### 説明

1. `download_economic_census_activity.py`を実行することで、年度ごとのデータがZIP形式でダウンロードされます。
2. `kaitou.py`を実行することで、ダウンロードしたZIPファイルを解凍し、テキストデータをCSV形式に変換します。
