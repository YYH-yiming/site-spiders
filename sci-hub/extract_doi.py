import pandas as pd
import os

def extract_doi_column(input_file, output_file):
    """
    读取 CSV 文件，提取 DOI 列，并保存为无表头的纯文本 CSV。
    """
    
    # 1. 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 找不到文件 '{input_file}'")
        return

    try:
        # 2. 读取 CSV 文件
        # encoding='utf-8-sig' 可以很好地处理中文路径或由 Excel 生成的 CSV
        df = pd.read_csv(input_file, encoding='utf-8-sig')

        # 3. 检查 'DOI' 列是否存在
        target_col = 'DOI'
        if target_col not in df.columns:
            # 尝试查找不区分大小写的匹配 (例如 'doi', 'Doi')
            found = False
            for col in df.columns:
                if col.lower() == 'doi':
                    target_col = col
                    found = True
                    print(f"提示: 原表中未找到大写的 'DOI'，已自动匹配到列名 '{target_col}'")
                    break
            
            if not found:
                print(f"错误: CSV 文件中未找到名为 'DOI' 的列。")
                print(f"现有列名: {list(df.columns)}")
                return

        # 4. 提取列并去除空值 (NaN)
        doi_series = df[target_col].dropna()

        # 也可以选择去除首尾空格，防止数据不干净
        doi_series = doi_series.astype(str).str.strip()

        # 5. 保存结果
        # index=False: 不保存行号 (0, 1, 2...)
        # header=False: 不保存列名 (DOI)
        doi_series.to_csv(output_file, index=False, header=False, encoding='utf-8')

        print(f"成功！已提取 {len(doi_series)} 个 DOI。")
        print(f"文件已保存为: {output_file}")

    except Exception as e:
        print(f"发生错误: {e}")

# --- 使用示例 ---
if __name__ == "__main__":
    # 在这里修改你的文件名
    input_csv = r'E:\产出\爬虫代码备份\sci-hub\合成生物学_关键词检索(1).csv'      # 输入的文件名
    output_csv = 'doi_output.csv'     # 输出的文件名
    
    extract_doi_column(input_csv, output_csv)