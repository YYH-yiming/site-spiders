import pandas as pd
import os
import glob
import re
import time
import json

# ================= 配置区域 =================
# 请务必修改为您实际的文件夹路径
input_folder = r'E:\wos_spider\WOS_Exported_Files'   # 输入文件夹
output_folder = r'E:\wos_spider\jiangsu_2025' # 输出文件夹

output_file_name = 'Jiangsu_2024_Final.csv'               # 结果数据文件
log_file_name = 'process_log.json'                        # 进度记录文件

CHUNK_SIZE = 3000                                        # 每次内存处理的行数
# ===========================================

def get_jiangsu_regex():
    """
    构建江苏省及下辖13个地级市的正则匹配模式。
    """
    cities = [
        "Jiangsu",      # 省名
        "Nanjing",      # 南京
        "Suzhou",       # 苏州
        "Wuxi",         # 无锡
        "Xuzhou",       # 徐州
        "Changzhou",    # 常州
        "Nantong",      # 南通
        "Lianyungang",  # 连云港
        "Huai'an", "Huaian", # 淮安
        "Yancheng",     # 盐城
        "Yangzhou",     # 扬州
        "Zhenjiang",    # 镇江
        "Taizhou",      # 泰州
        "Suqian"        # 宿迁
    ]
    # 拼接正则: (?i)(Jiangsu|Nanjing|Suzhou|...)
    return r'(?i)(' + '|'.join(cities) + ')'

def clean_address(text):
    """
    清除地址中的 [Author Name] 部分，避免人名干扰。
    """
    if not isinstance(text, str):
        return ""
    return re.sub(r'\[.*?\]', '', text)

def load_progress(log_path):
    """读取进度记录"""
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"current_file": None, "processed_rows": 0}
    return {"current_file": None, "processed_rows": 0}

def save_progress(log_path, file_name, rows_count):
    """保存进度记录"""
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump({"current_file": file_name, "processed_rows": rows_count}, f)

def process_wos_data():
    # 1. 初始化路径
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, output_file_name)
    log_path = os.path.join(output_folder, log_file_name)
    
    # 2. 获取文件列表并排序 (关键：必须排序以保证顺序一致)
    all_files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    print(f"发现 {len(all_files)} 个CSV文件，准备处理...")
    
    # 3. 加载上次进度
    progress = load_progress(log_path)
    last_file = progress.get('current_file')
    last_rows = progress.get('processed_rows', 0)
    
    # 标记：是否找到断点文件
    found_resume_point = False if last_file else True 
    # 标记：是否首次写入（控制表头）
    is_first_write = not os.path.exists(output_path)
    
    # 准备正则
    jiangsu_pattern = get_jiangsu_regex()
    total_saved = 0
    start_time = time.time()

    # 4. 循环处理文件
    for i, file_path in enumerate(all_files):
        file_name = os.path.basename(file_path)
        
        # --- 跳过已完成的文件 ---
        if not found_resume_point:
            if file_name == last_file:
                found_resume_point = True
            else:
                print(f"[跳过] {file_name} (上次已完成)")
                continue

        print(f"[{i+1}/{len(all_files)}] 正在处理: {file_name}")
        
        # --- 计算跳过行数 ---
        skip_rows = last_rows if (file_name == last_file) else 0
        current_file_processed_rows = skip_rows 
        
        try:
            # 构造读取迭代器
            if skip_rows > 0:
                print(f"恢复模式：跳过前 {skip_rows} 行...")
                # range(1, skip_rows + 1) 跳过数据行但保留表头
                reader = pd.read_csv(
                    file_path, 
                    chunksize=CHUNK_SIZE, 
                    encoding='utf-8', 
                    on_bad_lines='skip',
                    skiprows=range(1, skip_rows + 1) 
                )
            else:
                reader = pd.read_csv(
                    file_path, 
                    chunksize=CHUNK_SIZE, 
                    encoding='utf-8', 
                    on_bad_lines='skip'
                )
            
            # --- 分块处理 ---
            for chunk in reader:
                # 1. 筛选年份 (2024)
                if 'Publication Year' in chunk.columns:
                    chunk['Publication Year'] = pd.to_numeric(chunk['Publication Year'], errors='coerce')
                    year_mask = chunk['Publication Year'] == 2024
                else:
                    year_mask = False

                # 2. 筛选地址 (去除人名后匹配江苏城市)
                if 'Addresses' in chunk.columns:
                    clean_addrs = chunk['Addresses'].astype(str).map(clean_address)
                    addr_mask = clean_addrs.str.contains(jiangsu_pattern, regex=True, na=False)
                else:
                    addr_mask = False
                
                # 3. 综合筛选
                filtered_data = chunk[year_mask & addr_mask]

                # 4. 写入结果
                if not filtered_data.empty:
                    filtered_data.to_csv(
                        output_path, 
                        mode='a', 
                        index=False, 
                        header=is_first_write, 
                        encoding='utf-8-sig'
                    )
                    is_first_write = False
                    total_saved += len(filtered_data)
                
                # 5. 更新进度记录
                current_file_processed_rows += len(chunk)
                save_progress(log_path, file_name, current_file_processed_rows)

        except Exception as e:
            print(f"读取文件出错 {file_name}: {e}")
            continue

    # 5. 完成收尾
    print("="*40)
    print(f"所有处理完成！")
    print(f"⏱总耗时: {time.time() - start_time:.1f} 秒")
    print(f"共提取论文: {total_saved} 篇")
    print(f"结果已保存: {output_path}")
    
    # 清理进度文件（可选）
    if os.path.exists(log_path):
        os.remove(log_path)
        print("已清理进度文件。")

if __name__ == '__main__':
    process_wos_data()