# -*- coding: utf-8 -*-
# Web of Science (WOS) 批量关键词 + 范围导出脚本
# 修改版：支持 CSV 读取，支持首页及结果页连续搜索

import os
import glob
import time
import random
import csv
import io
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys  # 新增：用于模拟键盘操作
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException
)

from combine_wos_export import merge_wos_exports

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='ignore')
# ----------------------
# 配置参数
# ----------------------

WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/smart-search'
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'WOS_Exported_Files')
# ❗ 请确保此路径正确
CHROME_DOWNLOAD_DIR = 'C:\\Users\\Administrator\\Downloads' 
OUTPUT_FILE = r'E:\实习\学位中心实习\wos_spider\WOS_Exported_Files\WOS_Merged_Results_Final.xlsx' 
MAX_EXPORT_PER_CHUNK = 1000 

# CSV 文件路径 (请在脚本同目录下创建 keywords.csv，第一列为关键词，无表头)
CSV_FILE_PATH = 'E:\实习\学位中心实习\wos_spider\期刊列表.csv' 

WAIT_TIMEOUT = 30
PAUSE_TIME = 5
DOWNLOAD_WAIT_TIME = 10

# ----------------------
# XPATH 定义 (更新)
# ----------------------

# 两个页面共用的输入框 ID
XPATH_INPUT_COMMON = '//input[@id="composeQuerySmartSearch"]'

# 首页的搜索按钮 (aria-label="Submit your question")
XPATH_SEARCH_BTN_HOME = '//button[@aria-label="Submit your question"]'

# 结果页的搜索按钮 (aria-label="Search")
XPATH_SEARCH_BTN_RESULT = '//button[@aria-label="Search"]'

XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]'
XPATH_EXPORT_BUTTON = '//button[@id="export-trigger-btn"]'
XPATH_EXPORT_TO_EXCEL = '//button[@id="exportToExcelButton"]'
XPATH_CONTENT_DROPDOWN_BUTTON = '//wos-select/button[@aria-haspopup="listbox"]'
XPATH_CONTENT_FULL_RECORD_OPTION = '//div[@aria-label="Full Record"]'
XPATH_FINAL_EXPORT_BUTTON = '//button[@id="exportButton"]'

# =====================================================
# 工具函数：读取 CSV
# =====================================================
def read_keywords(csv_path):
    keywords = []
    if not os.path.exists(csv_path):
        print(f"[错误] 未找到 CSV 文件: {csv_path}")
        return []
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                keywords.append(row[0].strip())
    print(f"已加载 {len(keywords)} 个关键词: {keywords}")
    return keywords

# =====================================================
# 浏览器启动
# =====================================================
def setup_driver(download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=chrome_options)
    print("已连接到手动启动的 Chrome")
    return driver

# =====================================================
# 伪装真人输入
# =====================================================
def human_type(driver, element, text):
    actions = ActionChains(driver)
    # 点击元素确保激活
    actions.click(element).perform()
    time.sleep(0.2)
    
    for c in str(text):
        actions.send_keys(c)
        actions.pause(random.uniform(0.1, 0.3))
    actions.perform()

# =====================================================
# 核心逻辑：智能搜索 (兼容首页和结果页)
# =====================================================
def perform_search(driver, wait, keyword):
    print(f"\n[搜索操作] 正在检索: {keyword}")
    
    # 1. 定位输入框 (两个页面 ID 相同)
    try:
        search_box = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_INPUT_COMMON)))
    except Exception:
        print("[错误] 无法找到搜索框，可能页面未加载完成。")
        return False

    # 2. 清空输入框 (关键步骤：处理 Angular/Result Page 残留)
    # 使用 Ctrl+A -> Backspace，比 .clear() 更可靠
    print("[搜索操作] 清理旧关键词...")
    search_box.click()
    time.sleep(0.5)
    # 发送 Ctrl+A (全选)
    search_box.send_keys(Keys.CONTROL, "a") 
    time.sleep(0.5)
    # 发送 Delete/Backspace
    search_box.send_keys(Keys.BACKSPACE)
    time.sleep(1)

    # 3. 输入新关键词
    print(f"[搜索操作] 输入新关键词: {keyword}")
    human_type(driver, search_box, keyword)
    time.sleep(2) # 等待输入生效，按钮变亮

    # 4. 判断当前是“首页”还是“结果页”，点击对应按钮
    try:
        # 尝试寻找首页按钮
        home_btn = driver.find_elements(By.XPATH, XPATH_SEARCH_BTN_HOME)
        
        # 尝试寻找结果页按钮
        result_btn = driver.find_elements(By.XPATH, XPATH_SEARCH_BTN_RESULT)

        if home_btn and home_btn[0].is_displayed():
            print("[页面识别] 检测到【首页】搜索按钮，点击中...")
            home_btn[0].click()
        elif result_btn and result_btn[0].is_displayed():
            print("[页面识别] 检测到【结果页】搜索按钮，点击中...")
            result_btn[0].click()
        else:
            print("[严重错误] 未找到任何已知的搜索按钮！")
            return False
            
    except Exception as e:
        print(f"[搜索操作] 点击搜索按钮失败: {e}")
        return False

    # 5. 等待搜索结果加载 (给页面一点时间刷新)
    print("[等待] 等待结果页面加载...")
    time.sleep(PAUSE_TIME + 5)
    return True

# =====================================================
# 获取总记录数
# =====================================================
def get_total_records(wait):
    print("[信息] 正在获取总记录数...")
    try:
        # 增加一点强制等待，防止读取到上一次搜索的缓存数字
        time.sleep(2)
        element = wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_TOTAL_RECORDS_COUNT)))
        text = element.text.replace(',', '').strip()
        total = int("".join(filter(str.isdigit, text)))
        print(f"[成功] 总记录数：{total}")
        return total
    except Exception as e:
        print(f"[错误] 无法获取总记录数 (可能结果为0): {e}")
        return 0

# =====================================================
# 强制设置范围 (Angular Hack)
# =====================================================
def force_set_range(driver, start_record, end_record):
    js_set_value = """
        function set_input(el, val){
            el.value = val;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new Event('blur', { bubbles: true }));
        }
        var start_el = document.querySelector('input[name="markFrom"]');
        var end_el = document.querySelector('input[name="markTo"]');
        if(start_el && end_el) {
            set_input(start_el, arguments[0]);
            set_input(end_el, arguments[1]);
        }
    """
    driver.execute_script(js_set_value, str(start_record), str(end_record))
    time.sleep(1.5)

# =====================================================
# 导出单个块
# =====================================================
def export_record_range(driver, wait, keyword, chunk_index, start_record, end_record):
    print(f"   >>> 导出块 {chunk_index}: {start_record} - {end_record}")
    try:
        # 1. 导出主按钮
        export_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_BUTTON)))
        ActionChains(driver).move_to_element(export_button).click(export_button).perform()
        time.sleep(2)

        # 2. Excel 选项
        excel_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_TO_EXCEL)))
        ActionChains(driver).move_to_element(excel_button).click(excel_button).perform()
        time.sleep(4) # 等模态框

        # 3. 选中 Records from Radio
        records_mat = wait.until(EC.presence_of_element_located((By.XPATH, "//mat-radio-button[contains(., 'Records from')]")))
        native_input = records_mat.find_element(By.CSS_SELECTOR, "input[type='radio']")
        driver.execute_script("arguments[0].click();", native_input)
        time.sleep(1)

        # 4. 输入范围
        force_set_range(driver, start_record, end_record)

        # 5. 下拉选全记录 (如果页面已经记住了 Full Record，这里可能会报错，建议加 try-except 忽略)
        try:
            dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_DROPDOWN_BUTTON)))
            dropdown.click()
            time.sleep(1)
            full_record = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_FULL_RECORD_OPTION)))
            full_record.click()
            time.sleep(1)
        except:
            print("   [提示] 可能已默认选中 Full Record 或下拉框未出现，继续尝试导出...")

        # 6. 最终导出
        final_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_FINAL_EXPORT_BUTTON)))
        final_btn.click()
        
        # 7. 模拟等待下载 (不进行重命名，以免逻辑太复杂，用户需自行整理)
        # 如果需要重命名，可将原来代码中的 wait_for_download_and_rename 复制回来调用
        time.sleep(5) 
        
        # 关闭弹窗 (导出后弹窗通常会自动关闭，但如果卡住需要处理，这里假设自动关闭)
        return True

    except Exception as e:
        print(f"   [导出失败] 块 {chunk_index}: {e}")
        # 按 ESC 防止弹窗卡死
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(2)
        return False

# =====================================================
# 主任务
# =====================================================
def main_task():
    # 1. 读取关键词
    keywords = read_keywords(CSV_FILE_PATH)
    if not keywords:
        print("请在目录下创建 keywords.csv 并填入关键词")
        return

    # 2. 启动浏览器
    driver = setup_driver(DOWNLOAD_DIR)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    # 3. 打开 WOS 首页 (仅第一次需要)
    try:
        driver.get(WOS_URL_ROOT)
        print("\n请手动登录 Web of Science，并在页面加载完成后按 Enter 开始全自动运行...")
        input()
    except Exception as e:
        print(f"启动失败: {e}")
        return

    # 4. 循环处理每个关键词
    for kw_idx, keyword in enumerate(keywords, 1):
        print(f"\n{'='*50}")
        print(f"正在处理第 {kw_idx}/{len(keywords)} 个关键词: 【{keyword}】")
        print(f"{'='*50}")

        # --- A. 执行搜索 (处理首页/结果页差异) ---
        search_success = perform_search(driver, wait, keyword)
        if not search_success:
            print(f"[跳过] 搜索 {keyword} 失败，进入下一个。")
            continue

        # --- B. 获取记录数 ---
        total_records = get_total_records(wait)
        if total_records == 0:
            print(f"[跳过] 关键词 {keyword} 结果为 0。")
            continue

        # --- C. 分块导出 ---
        start_record = 1
        chunk_index = 1
        
        while start_record <= total_records:
            end_record = min(start_record + MAX_EXPORT_PER_CHUNK - 1, total_records)
            
            # 调用导出函数
            success = export_record_range(driver, wait, keyword, chunk_index, start_record, end_record)
            
            if success:
                print(f"   [完成] 块 {chunk_index} 导出成功。")
            
            # 准备下一块
            start_record = end_record + 1
            chunk_index += 1
            
            # 块之间休息，防封
            if start_record <= total_records:
                time.sleep(random.uniform(5, 10))

        print(f"\n[完成] 关键词 {keyword} 所有数据导出完毕。")
        time.sleep(3) # 词与词之间的间隔

    print("\n所有关键词处理完毕！")

if __name__ == "__main__":
    main_task()
    merge_wos_exports(CHROME_DOWNLOAD_DIR, OUTPUT_FILE, delete_originals=True)