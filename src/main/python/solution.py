import json
import os
import time
import zipfile
from datetime import datetime, timezone

ZIP_FILE_PATH = 'D:\\test-read-file\\records.zip'
UNZIP_FILE_PATH = 'D:\\test-read-file\\'
INPUT_FOLDER_PATH = 'D:\\test-read-file\\records\\'
OUTPUT_FOLDER_PATH = 'D:\\test-read-file\\output\\'
KEYWORD_MATCH = '张三'


def check_num():
    if not os.path.exists(INPUT_FOLDER_PATH):
        os.makedirs(INPUT_FOLDER_PATH)
    folder = os.listdir(INPUT_FOLDER_PATH)
    return len(folder) == 1001


def unzip():
    with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(UNZIP_FILE_PATH)


def get_long_list_map():
    timestamp_map = {}
    count = 0
    for i in range(1):
        file_path = INPUT_FOLDER_PATH + "/record_" + str(i) + ".txt"
        with open(file_path, 'r', encoding='utf-8') as reader:
            for line in reader:
                try:
                    json_object = json.loads(line.strip())
                    name = json_object["name"]
                    timestamp = json_object["timestamp"]
                    if name == KEYWORD_MATCH:
                        count += 1
                        if timestamp not in timestamp_map:
                            timestamp_map[timestamp] = []
                        timestamp_map[timestamp].append(line.strip())
                except json.JSONDecodeError:
                    pass
    if not timestamp_map:
        return None
    print(f"size and count is ===={len(timestamp_map)}---{count}")
    return timestamp_map


def extracted_file_to_path(timestamp_map):
    os.makedirs(OUTPUT_FOLDER_PATH, exist_ok=True)
    new_timestamp_map = {}
    for timestamp, values in timestamp_map.items():
        date = datetime.fromtimestamp(timestamp / 1000, timezone.utc).astimezone().date()
        new_key = str(date)
        if new_key not in new_timestamp_map:
            new_timestamp_map[new_key] = []
        new_timestamp_map[new_key].extend(values)
    for dateTime, lines in new_timestamp_map.items():
        output_file_path = OUTPUT_FOLDER_PATH + "/" + dateTime + ".txt"
        with open(output_file_path, 'w', encoding='utf-8') as writer:
            lines.sort(key=lambda line: json.loads(line)["timestamp"])
            for line in lines:
                writer.write(line)
                writer.write("\n")


if __name__ == '__main__':

    start_time = time.time()

    if not check_num():
        unzip()

    un_zip_time = time.time()
    print(f"unZipTime =========> {un_zip_time - start_time}")

    timestamp_map = get_long_list_map()
    match_file_time = time.time()
    print(f"matchFileTime =========> {match_file_time - un_zip_time}")

    if not timestamp_map:
        exit()

    extracted_file_to_path(timestamp_map)
    extracted_file_to_path_time = time.time()
    print(f"extractedFileToPathTime =========> {extracted_file_to_path_time - match_file_time}")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"总体耗时 =========> {total_time}")

    print(
        "解题思路: 1.下载解压文件  2.遍历每个解压后的文件  3.拿着关键字去匹配  4.将同一天分组以年月日为key  5.写入新的文件")
