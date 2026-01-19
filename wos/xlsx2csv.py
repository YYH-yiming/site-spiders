import pandas as pd

def xlsx_to_csv(xlsx_path, csv_path):
    # 读取 xlsx（只有一列时，pandas 会自动处理）
    df = pd.read_excel(xlsx_path, header=None)

    # 直接保存为 csv（不加索引、不加表头）
    df.to_csv(csv_path, index=False, header=False, encoding="utf-8-sig")

if __name__ == "__main__":
    # 修改为你的文件路径
    xlsx_file = "E:\wos_spider\合成生物学关键词-0115.xlsx"
    csv_file = "合成生物学关键词-0115.csv"

    xlsx_to_csv(xlsx_file, csv_file)
    print("转换完成！")
