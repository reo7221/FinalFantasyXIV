import os
import glob
import datetime
import re

# 設定セクション
LOG_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(LOG_DIR, "merge_logs")
MAX_SIZE = 0.5 * 1000 * 1000 * 1000  # 2.5 GB
BUFFER_SIZE = 16 * 1024 * 1024  # 4MBのバッファサイズ

os.makedirs(OUTPUT_DIR, exist_ok=True)


def extract_dates_from_filename(filename):
    """ファイル名から日付を抽出する関数"""
    matches = re.findall(r'_(\d{8})', filename)
    return [datetime.datetime.strptime(date, '%Y%m%d') for date in matches]


def merge_file(log_file, output_file_path):
    """個別のログファイルを結合する関数"""
    with open(log_file, 'r', encoding='utf-8', errors='replace') as infile:
        with open(output_file_path, 'a', encoding='utf-8', errors='replace') as outfile:
            while True:
                chunk = infile.read(BUFFER_SIZE)
                if not chunk:
                    break
                outfile.write(chunk)


def initialize_merge_variables():
    """結合処理用の変数を初期化する関数"""
    return {
        "current_size": 0,
        "output_file_index": 1,
        "current_output_file_path": None,
        "min_date_for_current_part": None,
        "max_date_for_current_part": None,
        "final_files": [],
    }


def handle_new_output_file(output_directory, output_file_index):
    """新しい出力ファイルのパスを生成する関数"""
    return os.path.join(output_directory, f"merge_log_temp_part{output_file_index}.log")


def merge_logs(log_directory, output_directory, max_size):
    """ログファイルを結合するメイン関数"""
    print("\n----- ログファイルの結合を開始します -----")

    log_files = glob.glob(os.path.join(log_directory, "*.log"))
    total_files = len(log_files)

    if total_files == 0:
        print("エラー: 対象のログファイルが見つかりません。")
        return []

    print(f"処理するファイル数: {total_files}\n")

    # 結合処理用の変数を初期化
    merge_vars = initialize_merge_variables()
    existing_files = set(os.listdir(output_directory))  # 既存ファイル名をキャッシュ

    # ファイル数の桁数を計算
    total_files_digits = len(str(total_files))

    for file_counter, log_file in enumerate(log_files, start=1):
        file_size = os.path.getsize(log_file)
        dates = extract_dates_from_filename(log_file)

        if not dates:
            print(f"警告: '{log_file}' から日付情報を抽出できません。")
            continue

        file_min_date = min(dates)
        file_max_date = max(dates)

        # 日付の最小値と最大値を更新
        merge_vars["min_date_for_current_part"] = (
            file_min_date if merge_vars["min_date_for_current_part"] is None
            else min(merge_vars["min_date_for_current_part"], file_min_date)
        )
        merge_vars["max_date_for_current_part"] = (
            file_max_date if merge_vars["max_date_for_current_part"] is None
            else max(merge_vars["max_date_for_current_part"], file_max_date)
        )

        # 出力ファイルの初期化
        if merge_vars["current_output_file_path"] is None:
            merge_vars["current_output_file_path"] = handle_new_output_file(output_directory, merge_vars["output_file_index"])

        # サイズを確認して新しいファイルに切り替え
        if merge_vars["current_size"] + file_size > max_size:
            if merge_vars["current_size"] > 0:
                merge_vars["final_files"].append((merge_vars["current_output_file_path"],
                                                  merge_vars["min_date_for_current_part"],
                                                  merge_vars["max_date_for_current_part"]))

            merge_vars["output_file_index"] += 1
            merge_vars["current_output_file_path"] = handle_new_output_file(output_directory, merge_vars["output_file_index"])
            merge_vars["current_size"] = 0
            merge_vars["min_date_for_current_part"] = file_min_date
            merge_vars["max_date_for_current_part"] = file_max_date

        # 進捗表示
        print(f"[{file_counter:>{total_files_digits}}/{total_files}] 結合中: {log_file}")

        try:
            merge_file(log_file, merge_vars["current_output_file_path"])
            merge_vars["current_size"] += file_size
        except Exception as e:
            print(f"エラー: '{log_file}' の結合に失敗しました。エラー: {e}")

    # 最後の出力ファイルの情報を保存
    if merge_vars["current_size"] > 0 and merge_vars["current_output_file_path"] is not None:
        merge_vars["final_files"].append((merge_vars["current_output_file_path"],
                                          merge_vars["min_date_for_current_part"],
                                          merge_vars["max_date_for_current_part"]))

    print("----- ログファイルの結合が完了しました -----\n")

    # 結合したファイルをリネーム
    for temp_file, min_date, max_date in merge_vars["final_files"]:
        final_output_name = f"merge_log_{min_date.strftime('%Y%m%d')}_{max_date.strftime('%Y%m%d')}.log"
        final_output_path = os.path.join(output_directory, final_output_name)

        # 名前の衝突を避ける処理
        count = 1
        while final_output_name in existing_files:
            count += 1
            final_output_name = f"merge_log_{min_date.strftime('%Y%m%d')}_{max_date.strftime('%Y%m%d')}_{count}.log"
            final_output_path = os.path.join(output_directory, final_output_name)

        os.rename(temp_file, final_output_path)

    return [os.path.join(output_directory, f) for f in os.listdir(output_directory) if f.startswith("merge_log_")]


if __name__ == "__main__":
    final_output_files = merge_logs(LOG_DIR, OUTPUT_DIR, MAX_SIZE)

    if final_output_files:
        print("結合されたログファイル:")
        for file in final_output_files:
            print(f"\t{file}")

    input("\n処理が完了しました。何かキーを押して終了してください。")
