import os
import csv

def is_header(row1, row2):
    def is_number(s):
        try:
            float(s)
            return True
        except:
            return False

    if not row1 or not row2:
        return False

    row1_num = sum(is_number(x) for x in row1)
    row2_num = sum(is_number(x) for x in row2)

    # 第一行数字明显更少 → 认为是表头
    return row1_num < row2_num

def count_csv_rows(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.reader(f)

        try:
            first = next(reader)
        except StopIteration:
            return 0

        try:
            second = next(reader)
        except StopIteration:
            return 1

        has_header = is_header(first, second)

        count = 2  # 已读两行
        for _ in reader:
            count += 1

        return count - 1 if has_header else count


def count_all_csv(directory='E:\wos_spider\WOS_Exported_Files'):
    total = 0

    for name in os.listdir(directory):
        if name.lower().endswith('.csv'):
            path = os.path.join(directory, name)
            rows = count_csv_rows(path)
            print(f"{name}: {rows} 行")
            total += rows

    print(f"\n总数据条目数：{total} 行")


if __name__ == "__main__":
    count_all_csv()
