import pandas as pd
import os

# ================= 配置区域 =================
# 1. 输入文件路径
input_file = r'E:\wos_spider\期刊列表_2025中科院分区1区期刊与目前爬取论文对比.xlsx'

# 2. 输出文件名称
output_match = 'Result_B_in_A.csv'      # B中有的，且A里也有 (交集)
output_rest  = 'Result_B_not_in_A.csv'  # B中有的，但A里没有 (B剩下的)

# ================= 主逻辑 =================

def split_b_based_on_a(file_path):
    print(f"正在读取文件: {file_path} ...")
    
    # 1. 读取文件
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, header=None)
        else:
            df = pd.read_excel(file_path, header=None, engine='openpyxl')
    except Exception as e:
        print(f"读取失败: {e}")
        return

    if df.shape[1] < 2:
        print("错误：文件至少需要两列数据（A列和B列）")
        return

    # 2. 获取数据
    # A列：作为参考标准 (Reference)
    col_a_raw = df[0].dropna().astype(str).tolist()
    # B列：作为被拆分的对象 (Target)
    col_b_raw = df[1].dropna().astype(str).tolist()

    # 3. 建立 A 列的“指纹库”
    # 我们要判断 B 里的东西是否在 A 里，所以要把 A 做成集合
    set_a = set(x.strip().lower() for x in col_a_raw)

    # 4. 分类容器
    list_match_b = [] # B列中匹配到的
    list_rest_b  = [] # B列中剩下的

    print("正在筛选 B 列数据...")
    
    # 遍历 B 列的每一项
    for item in col_b_raw:
        # 清洗当前 B 列的数据用于比对
        check_item = item.strip().lower()
        
        # 判断：这个 B 列的词，在 A 列里有吗？
        if check_item in set_a:
            list_match_b.append(item)  # 有 -> 放入交集
        else:
            list_rest_b.append(item)   # 没有 -> 放入剩余

    # 5. 保存结果
    print(f"正在保存文件...")
    try:
        # 保存交集
        pd.Series(list_match_b).to_csv(output_match, index=False, header=['B_Intersect_A'], encoding='utf-8-sig')
        print(f"✅ 文件1 (交集): {output_match} - 共 {len(list_match_b)} 条")

        # 保存剩余
        pd.Series(list_rest_b).to_csv(output_rest, index=False, header=['B_Rest'], encoding='utf-8-sig')
        print(f"✅ 文件2 (剩余): {output_rest}  - 共 {len(list_rest_b)} 条")
        
        # 验证总数
        print(f"--------------------------------------------------")
        print(f"验证: {len(list_match_b)} + {len(list_rest_b)} = {len(list_match_b) + len(list_rest_b)}")
        print(f"原 B 列总数: {len(col_b_raw)}")
        if (len(list_match_b) + len(list_rest_b)) == len(col_b_raw):
            print("结果正确！数据守恒。")
        else:
            print("警告：数据总数不匹配，请检查。")

    except Exception as e:
        print(f"保存失败: {e}")

# ================= 执行 =================
if __name__ == '__main__':
    if os.path.exists(input_file):
        split_b_based_on_a(input_file)
    else:
        print(f"找不到文件: {input_file}")