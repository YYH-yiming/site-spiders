import pandas as pd
import os
import glob
import json
import pickle
import shutil

# ================= 配置区域 =================
# 1. 文件夹路径
FILES_FOLDER = 'files'               # 原始数据文件夹
TIER1_FILE = '中科院1区期刊.csv'      # 1区期刊名单
TEMP_FOLDER = 'temp_results'         # [新增] 用于存放中间结果的临时文件夹
CHECKPOINT_FILE = 'checkpoint.json'  # [新增] 用于记录进度的文件

# 2. 输出文件名
OUTPUT_JOURNAL_CSV = '结果_各期刊统计.csv'
OUTPUT_SUMMARY_CSV = '结果_分区统计.csv'

# 3. CSV列名 (请修改为你实际的列名)
COL_JOURNAL = 'Journal Name'         
COL_YEAR = 'Publication Year'        
TIER1_COL_NAME = 'Journal Name'      

# 4. 参数
CHUNK_SIZE = 50000                 
# ============================================

def normalize_name(name):
    """标准化期刊名"""
    if pd.isna(name):
        return ""
    return str(name).strip().lower()

def load_checkpoint():
    """加载进度记录"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {
        "finished_files": [],       # 已经彻底处理完的文件列表
        "current_file": None,       # 当前正在处理的文件
        "processed_chunks": 0       # 当前文件已经处理了多少个块
    }

def save_checkpoint(state):
    """保存进度记录"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def main():
    # 0. 初始化环境
    if not os.path.exists(TEMP_FOLDER):
        os.makedirs(TEMP_FOLDER)

    print("Step 1: 加载1区期刊名单...")
    try:
        tier1_df = pd.read_csv(TIER1_FILE)
        tier1_set = set(tier1_df[TIER1_COL_NAME].apply(normalize_name))
        print(f" - 已加载 {len(tier1_set)} 个1区期刊。")
    except Exception as e:
        print(f"Error: 读取1区文件失败: {e}")
        return

    # 1. 获取所有CSV文件并排序（排序很重要，保证顺序一致）
    all_files = sorted(glob.glob(os.path.join(FILES_FOLDER, '*.csv')))
    print(f"Step 2: 发现 {len(all_files)} 个数据文件。")

    # 2. 加载断点状态
    state = load_checkpoint()
    finished_files = set(state['finished_files'])
    current_file_record = state['current_file']
    processed_chunks_record = state['processed_chunks']

    print(f"Step 3: 检查断点...")
    if current_file_record:
        print(f" - 上次中断于文件: {current_file_record}, 第 {processed_chunks_record} 块")
    else:
        print(" - 无中断记录，从头开始。")

    # 3. 遍历文件
    for file_path in all_files:
        file_name = os.path.basename(file_path)

        # case A: 文件已经完全处理过 -> 跳过
        if file_name in finished_files:
            print(f" [跳过] 已完成文件: {file_name}")
            continue

        print(f" -> 正在处理文件: {file_name} ...")
        
        # 准备读取
        try:
            reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False)
            
            chunk_idx = 0
            for chunk in reader:
                chunk_idx += 1
                
                # case B: 如果是上次中断的文件，需要跳过已经处理过的块
                if file_name == current_file_record and chunk_idx <= processed_chunks_record:
                    # 仅在刚开始打印一次跳过信息，避免刷屏
                    if chunk_idx % 10 == 0:
                        print(f"    ...跳过已处理块 {chunk_idx}", end='\r')
                    continue

                # --- 核心处理逻辑 (同之前) ---
                chunk[COL_YEAR] = pd.to_numeric(chunk[COL_YEAR], errors='coerce')
                chunk = chunk.dropna(subset=[COL_YEAR])
                chunk['temp_journal_key'] = chunk[COL_JOURNAL].apply(normalize_name)
                
                mask_new = (chunk[COL_YEAR] >= 2021) & (chunk[COL_YEAR] <= 2026)
                mask_old = (chunk[COL_YEAR] <= 2020)
                
                chunk['cnt_total'] = 1
                chunk['cnt_2021_2026'] = mask_new.astype(int)
                chunk['cnt_2020_before'] = mask_old.astype(int)
                
                chunk_summary = chunk.groupby('temp_journal_key')[[
                    'cnt_total', 'cnt_2021_2026', 'cnt_2020_before'
                ]].sum().reset_index()

                # --- 保存中间结果 ---
                # 将这个块的结果保存为独立文件
                temp_filename = f"{file_name}_chunk_{chunk_idx}.pkl"
                temp_path = os.path.join(TEMP_FOLDER, temp_filename)
                chunk_summary.to_pickle(temp_path)

                # --- 更新并保存状态 ---
                state['current_file'] = file_name
                state['processed_chunks'] = chunk_idx
                save_checkpoint(state)
                
                print(f"    块 {chunk_idx} 处理完成并保存.", end='\r')

            # --- 文件处理完成 ---
            print(f"\n    文件 {file_name} 全部完成。")
            state['finished_files'].append(file_name)
            state['current_file'] = None # 重置当前文件
            state['processed_chunks'] = 0
            save_checkpoint(state)

        except Exception as e:
            print(f"\n[错误] 处理文件 {file_name} 时发生异常: {e}")
            print("程序已停止，修复错误后重新运行即可从断点继续。")
            return

    # ================= 聚合阶段 =================
    print("\nStep 4: 所有文件处理完毕，开始合并临时结果...")
    
    # 获取temp文件夹下所有pkl文件
    temp_files = glob.glob(os.path.join(TEMP_FOLDER, '*.pkl'))
    if not temp_files:
        print("没有找到中间结果文件，无法生成报告。")
        return

    # 1. 收集所有小块的统计数据
    all_summaries = []
    total_parts = len(temp_files)
    for i, pkl_file in enumerate(temp_files):
        try:
            df = pd.read_pickle(pkl_file)
            all_summaries.append(df)
            if i % 100 == 0:
                print(f" - 合并进度: {i}/{total_parts}", end='\r')
        except:
            print(f"警告：无法读取临时文件 {pkl_file}")

    print("\n - 正在进行最终GroupBy聚合...")
    # 2. 合并为一个大表并再次GroupBy
    combined_df = pd.concat(all_summaries, ignore_index=True)
    final_journal_df = combined_df.groupby('temp_journal_key').sum().reset_index()
    
    final_journal_df.columns = ['期刊名(标准化)', '总论文数', '2021-2026数量', '2020及以前数量']
    
    # 输出表1
    final_journal_df.to_csv(OUTPUT_JOURNAL_CSV, index=False, encoding='utf-8-sig')
    print(f" [输出1] 各期刊统计已保存至: {OUTPUT_JOURNAL_CSV}")

    # 3. 分区统计逻辑
    print("Step 5: 进行分区统计...")
    final_journal_df['是否1区'] = final_journal_df['期刊名(标准化)'].apply(lambda x: '1区' if x in tier1_set else '其余')
    
    tier_summary = final_journal_df.groupby('是否1区')[[
        '总论文数', '2021-2026数量', '2020及以前数量'
    ]].sum().reset_index()
    
    # 输出表2
    tier_summary.to_csv(OUTPUT_SUMMARY_CSV, index=False, encoding='utf-8-sig')
    print(f" [输出2] 分区统计已保存至: {OUTPUT_SUMMARY_CSV}")

    print("\n--- 任务全部完成 ---")
    
    # 可选：询问是否删除临时文件
    # shutil.rmtree(TEMP_FOLDER) 
    # if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)

if __name__ == '__main__':
    main()