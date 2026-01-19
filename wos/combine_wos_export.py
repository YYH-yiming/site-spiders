# wos_merge.py
import os
import pandas as pd
import re
from datetime import datetime
import os
import pandas as pd
import re
from tqdm import tqdm
from openpyxl import Workbook


# ------------------------------------------------------------
# 强兼容 Excel 读取函数（不会再出现 engine 错误）
# ------------------------------------------------------------
def read_excel_safely(file_path):
    try:
        return pd.read_excel(file_path, engine="openpyxl")
    except Exception:
        pass

    try:
        return pd.read_excel(file_path, engine="xlrd")
    except Exception:
        pass

    try:
        return pd.read_excel(file_path, engine="pyxlsb")
    except Exception:
        pass

    return None


# ------------------------------------------------------------
# CSV 总行数统计（不含表头）
# ------------------------------------------------------------
def count_csv_rows(csv_path):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        total = sum(1 for _ in f) - 1
    return max(total, 0)


# ------------------------------------------------------------
# 主函数：Excel → CSV（不会爆内存）
# ------------------------------------------------------------
def merge_wos_exports_to_csv(input_folder, output_csv,
                             delete_originals=False, match_savedrecs=True):

    print("\n--- 开始执行 Excel → CSV 合并任务 ---\n")

    output_dir = os.path.dirname(output_csv)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 过滤文件
    def is_wos_file(name):
        n = name.lower()
        if not (n.endswith(".xls") or n.endswith(".xlsx")):
            return False
        if match_savedrecs:
            return re.match(r"savedrecs.*", name, re.IGNORECASE) is not None
        return True

    try:
        files = [f for f in os.listdir(input_folder) if is_wos_file(f)]
        files.sort()
    except FileNotFoundError:
        print(f"❌ 文件夹不存在：{input_folder}")
        return

    if not files:
        print("⚠ 未找到 Excel 文件。")
        return

    print(f"找到 {len(files)} 个 Excel 文件，开始写 CSV...\n")

    first_write = True
    files_to_delete = []

    # --------------------------------------------------------
    # Excel → CSV（逐文件，不爆内存）
    # --------------------------------------------------------
    for file in tqdm(files, desc="读取 Excel 并写入 CSV",
                     dynamic_ncols=True, colour="green", leave=False):

        file_path = os.path.join(input_folder, file)

        df = read_excel_safely(file_path)
        if df is None:
            tqdm.write(f"❌ 无法读取：{file}")
            continue

        tqdm.write(f"读取 {file}（{len(df)} 行）")

        df.to_csv(
            output_csv,
            mode='w' if first_write else 'a',
            header=first_write,
            index=False,
            encoding="utf-8-sig"
        )

        first_write = False
        files_to_delete.append(file_path)
        del df

    print("\n✔ 所有 Excel 已写入 CSV！")

    # --------------------------------------------------------
    # 统计 CSV 行数
    # --------------------------------------------------------
    total_rows = count_csv_rows(output_csv)

    print(f"\n📊 CSV 总数据行数（不含表头）：{total_rows}\n")

    # --------------------------------------------------------
    # 删除原 Excel（可选）
    # --------------------------------------------------------
    if delete_originals:
        print("正在删除原 Excel 文件...")
        for f in files_to_delete:
            try:
                os.remove(f)
                print(f"已删除：{os.path.basename(f)}")
            except:
                print(f"⚠ 删除失败：{os.path.basename(f)}")

    print("\n--- CSV 合并完成 ---\n")

    return total_rows


# ------------------------------------------------------------
# 独立的 CSV → XLSX 函数（可选调用）
# ------------------------------------------------------------

def csv_to_xlsx(csv_file, xlsx_file):
    print("\n--- 开始执行 CSV → XLSX ---\n")

    # 获取总行数
    total_rows = count_csv_rows(csv_file)
    print(f"CSV 总行数：{total_rows}")

    wb = Workbook()
    ws = wb.active

    with open(csv_file, "r", encoding="utf-8-sig") as f:

        header = next(f).strip().split(",")
        ws.append(header)

        for line in tqdm(
                f,
                total=total_rows,
                desc="写入 XLSX",
                dynamic_ncols=True,
                colour="yellow",
                leave=False):
            ws.append(line.strip().split(","))

    wb.save(xlsx_file)
    print(f"\n✔ XLSX 文件已生成：{xlsx_file}\n")


# --- 示例调用 ---
if __name__ == '__main__':
    # 1) 默认行为（可省略参数） => 只匹配 savedrecs*
    # merge_wos_exports(INPUT_FOLDER, OUTPUT_FILE, delete_originals=DELETE_ORIGINALS)

    # 合并后删除源文件
    DELETE_ORIGINALS = True
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # 2) 合并所有 xls / xlsx

    
    # INPUT_FOLDER = r'E:\wos_spider\WOS_Exported_Files'  
    # OUTPUT_FILE = r'E:\wos_spider\WOS_Exported_Files\File_From_WOS_Exported_Merge_{}.csv'.format(timestamp)
    # merge_wos_exports_to_csv(
    #     INPUT_FOLDER,
    #     OUTPUT_FILE,
    #     delete_originals=DELETE_ORIGINALS,
    #     match_savedrecs=False
    # )



    INPUT_FOLDER = r'C:\Users\admin\Downloads' 

    OUTPUT_FILE = r'E:\wos_spider\WOS_Exported_Files\WOS_Merged_Results_Final__sub_{}.csv'.format(timestamp)
    merge_wos_exports_to_csv(
        INPUT_FOLDER,
        OUTPUT_FILE,
        delete_originals=DELETE_ORIGINALS,
        match_savedrecs=True
    )

    #csv 2 xlsx
    # csv_to_xlsx_stream(OUTPUT_CSV, OUTPUT_XLSX)
