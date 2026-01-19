# -*- coding: utf-8 -*-
# Web of Science (WOS) 高级检索 + 批量导出脚本 (Advanced Search 健壮版)

import os
import re
import glob
import time
import random
import csv
import io
import sys
import json
import logging
import traceback
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException
)

# 尝试导入合并脚本
try:
    from combine_wos_export import merge_wos_exports_to_csv
except ImportError:
    def merge_wos_exports_to_csv(a, b, delete_originals=False): 
        print("[警告] 未找到合并脚本，跳过合并步骤。")

# =====================================================
# 全局配置参数 (请核对路径)
# =====================================================
WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/advanced-search'

# 您的工作目录
WORK_DIR = r'E:\wos_spider'
# 结果保存目录
DOWNLOAD_DIR = os.path.join(WORK_DIR, 'WOS_Exported_Files')
# Chrome 默认下载路径 (必须准确)
CHROME_DOWNLOAD_DIR = r'C:\Users\admin\Downloads' 

# [重要] 年份限制 (留空则不限制)
# 格式示例: "2023" 或 "2020-2024"
TARGET_YEAR = "2000-2019"

# 输入文件和输出文件
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(DOWNLOAD_DIR, f'WOS_Merged_Results_Final_{timestamp}.csv')
CSV_FILE_PATH = os.path.join(WORK_DIR, '期刊列表2000_2019.csv')
STATE_FILE_PATH = os.path.join(DOWNLOAD_DIR, 'wos_spider_state.json')

# 爬虫参数
MAX_EXPORT_PER_CHUNK = 1000
WAIT_TIMEOUT = 90
PAUSE_TIME = 5

# =====================================================
# XPATH 定义
# =====================================================
# 高级检索输入框
XPATH_ADVANCED_INPUT = '//textarea[@id="advancedSearchInputArea"]'
# 高级检索 Search 按钮
XPATH_ADVANCED_SEARCH_BTN = '//button[@data-ta="run-search"]'
# 高级检索 Clear 按钮
XPATH_ADVANCED_CLEAR_BTN = '//button[@data-ta="clear-search"]'

# [新增] 高级检索报错/无结果提示框 (基于你提供的 UI)
# class="search-error error-code light-red-bg ng-star-inserted"
XPATH_ADVANCED_ERROR_ALERT = '//div[contains(@class, "search-error") and contains(@class, "error-code")]'

# 结果页元素
XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]'
XPATH_EXPORT_BUTTON = '//button[@id="export-trigger-btn"]'
XPATH_EXPORT_TO_EXCEL = '//button[@id="exportToExcelButton"]'
XPATH_CONTENT_DROPDOWN_BUTTON = '//wos-select/button[@aria-haspopup="listbox"]'
XPATH_CONTENT_FULL_RECORD_OPTION = '//div[@aria-label="Full Record"]'
XPATH_FINAL_EXPORT_BUTTON = '//button[@id="exportButton"]'

# =====================================================
# 日志系统配置
# =====================================================
def setup_logger(log_dir):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_subdir = os.path.join(log_dir, 'logs')
    if not os.path.exists(log_subdir):
        os.makedirs(log_subdir)
        
    log_filename = os.path.join(log_subdir, f'spider_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger = logging.getLogger('wos_spider')
    logger.setLevel(logging.INFO)
    if logger.handlers:
        logger.handlers.clear()
    
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = logging.getLogger('wos_spider_init') 

# =====================================================
# 状态管理函数
# =====================================================
def load_state():
    if not os.path.exists(STATE_FILE_PATH):
        return {"kw_index": 0, "start_record": 1}
    try:
        with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return {"kw_index": 0, "start_record": 1}
            data = json.loads(content)
            logger.info(f"检测到断点：第 {data.get('kw_index', 0)+1} 个关键词，记录起始 {data.get('start_record', 1)}")
            return data
    except Exception as e:
        logger.warning(f"读取状态文件失败，将从头开始: {e}")
        return {"kw_index": 0, "start_record": 1}

def save_state(kw_index, start_record):
    data = {"kw_index": kw_index, "start_record": start_record}
    try:
        with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

# =====================================================
# 辅助功能函数
# =====================================================
def read_keywords(csv_path):
    keywords = []
    if not os.path.exists(csv_path):
        logger.error(f"未找到 CSV 文件: {csv_path}")
        return []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                keywords.append(row[0].strip())
    logger.info(f"已加载 {len(keywords)} 个关键词")
    return keywords

def setup_driver(download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def human_type(driver, element, text):
    actions = ActionChains(driver)
    actions.click(element).perform()
    time.sleep(0.2)
    for c in str(text):
        actions.send_keys(c)
        actions.pause(random.uniform(0.05, 0.15))
    actions.perform()

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
    time.sleep(1)

# =====================================================
# 核心业务逻辑 (已增强错误处理)
# =====================================================
def perform_search(driver, wait, keyword):
    """
    在高级检索页面输入并检索，同时处理成功跳转和页面报错两种情况
    """
    safe_keyword = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), keyword)
    
    if TARGET_YEAR:
        search_query = f'SO="{safe_keyword}" AND PY=({TARGET_YEAR})'
    else:
        search_query = f'SO="{safe_keyword}"'
        
    logger.info(f"正在构建检索式: {search_query}")

    # 1. 确保在高级检索页面
    if "advanced-search" not in driver.current_url:
        logger.info("跳转回 Advanced Search...")
        driver.get(WOS_URL_ROOT)
        time.sleep(3)

    try:
        # 2. 定位输入框
        search_box = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_ADVANCED_INPUT)))
        
        # 尝试清空
        try:
            clear_btn = driver.find_elements(By.XPATH, XPATH_ADVANCED_CLEAR_BTN)
            if clear_btn and clear_btn[0].is_enabled():
                clear_btn[0].click()
                time.sleep(0.5)
        except: pass
        
        search_box.click()
        search_box.send_keys(Keys.CONTROL, "a")
        search_box.send_keys(Keys.BACKSPACE)
        time.sleep(0.5)

        # 3. 输入内容
        human_type(driver, search_box, search_query)
        time.sleep(1)

        # 4. 点击搜索
        try:
            search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_ADVANCED_SEARCH_BTN)))
            search_btn.click()
            logger.info("点击 Search 按钮，等待响应...")
        except Exception as e:
            logger.error(f"点击搜索按钮失败: {e}")
            return False

        time.sleep(2)
        
        # ==============================================================
        # [健壮性处理] 核心修改：竞态等待 (Race Wait)
        # 同时监控：结果页总数 (成功) OR 错误提示框 (失败)
        # ==============================================================
        
        def check_search_result_or_error(d):
            # 1. 检查错误/无结果提示框 (优先级高，因为可能不跳转)
            # 对应 HTML: <div class="search-error error-code ...">
            error_alerts = d.find_elements(By.XPATH, XPATH_ADVANCED_ERROR_ALERT)
            if error_alerts and error_alerts[0].is_displayed():
                return "ERROR_ALERT"
            
            # 2. 检查结果页面的总记录数 (表示成功跳转)
            success_flags = d.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
            if success_flags and success_flags[0].is_displayed():
                return "SUCCESS"
                
            return False

        time.sleep(5)
        try:
            # 轮询检测，直到超时
            status = WebDriverWait(driver, WAIT_TIMEOUT).until(check_search_result_or_error)

            if status == "ERROR_ALERT":
                logger.warning(f" >>> [跳过] WOS 提示无结果或检索式错误: {search_query}")
                # 【关键步骤】报错后，页面会有红框，最好刷新一下清理环境，
                # 否则残留的红框可能影响下一次输入的定位，或者导致误判
                try:
                    logger.info("刷新页面以清除错误状态...")
                    driver.refresh()
                    time.sleep(3)
                except: pass
                return False

            elif status == "SUCCESS":
                logger.info(" >>> [成功] 结果页面已加载")
                return True

        except TimeoutException:
            logger.error(f" >>> [失败] 搜索响应超时 ({WAIT_TIMEOUT}秒)，既无结果也无报错")
            # 超时后刷新，防止页面死锁
            try:
                driver.refresh()
            except: pass
            return False

    except Exception as e:
        logger.error(f"检索过程异常: {e}")
        return False

def get_total_records(wait):
    try:
        element = wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_TOTAL_RECORDS_COUNT)))
        text = element.text.replace(',', '').strip()
        total = int("".join(filter(str.isdigit, text)))
        logger.info(f"总记录数：{total}")
        return total
    except Exception as e:
        logger.warning(f"无法获取总记录数: {e}")
        return 0

def export_record_range(driver, wait, keyword, chunk_index, start_record, end_record):
    logger.info(f" >>> 正在导出块 {chunk_index} (记录 {start_record} - {end_record})")
    try:
        export_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_BUTTON)))
        driver.execute_script("arguments[0].click();", export_button)
        time.sleep(1) 

        excel_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_TO_EXCEL)))
        driver.execute_script("arguments[0].click();", excel_button)
        
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "app-export-out-details")))
        time.sleep(1) 

        records_mat = wait.until(EC.presence_of_element_located((By.XPATH, "//mat-radio-button[contains(., 'Records from')]")))
        native_input = records_mat.find_element(By.CSS_SELECTOR, "input[type='radio']")
        driver.execute_script("arguments[0].click();", native_input)
        time.sleep(0.5)

        force_set_range(driver, start_record, end_record)

        try:
            dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_DROPDOWN_BUTTON)))
            dropdown.click()
            time.sleep(0.5)
            full_record = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_FULL_RECORD_OPTION)))
            full_record.click()
            time.sleep(0.5)
        except: pass 

        final_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_FINAL_EXPORT_BUTTON)))
        final_btn.click()
        
        logger.info(" >>> 等待导出窗口关闭...")
        start_wait_time = time.time()
        try:
            wait.until(EC.invisibility_of_element_located((By.TAG_NAME, "app-export-out-details")))
            elapsed = time.time() - start_wait_time
            logger.info(f" >>> [成功] 窗口已关闭 (耗时 {elapsed:.2f}s)")
            time.sleep(2) 
            return True
        except TimeoutException:
            logger.warning(" >>> [超时] 强制按ESC...")
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(3)
            return False
            
    except Exception as e:
        logger.error(f" >>> [失败] 块 {chunk_index} 导出出错: {e}")
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(2)
        return False

# =====================================================
# 主任务
# =====================================================
def main_task():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore', line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='ignore', line_buffering=True)
    
    global logger
    logger = setup_logger(DOWNLOAD_DIR)
    logger.info(f"=== 脚本启动 (Advanced Search + 健壮性增强版) ===")

    keywords = read_keywords(CSV_FILE_PATH)
    if not keywords: return

    state = load_state()
    start_kw_index = state['kw_index']
    resume_record_start = state['start_record']

    try:
        driver = setup_driver(DOWNLOAD_DIR)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        logger.info("浏览器连接成功")
    except Exception as e:
        logger.critical(f"浏览器连接失败: {e}")
        return

    # 初始访问
    if start_kw_index == 0:
        try:
            driver.get(WOS_URL_ROOT)
            logger.info("请手动登录 WOS。登录完成且看到 Advanced Search 页面后，按 Enter 键开始...")
            input()
        except: pass
    else:
        logger.info("断点模式启动，请确保浏览器在 WOS 页面，按 Enter 继续...")
        input()

    try:
        for idx, keyword in enumerate(keywords):
            if idx < start_kw_index:
                continue
            
            logger.info(f"{'='*40}")
            logger.info(f"进度: {idx+1}/{len(keywords)} - 期刊: 【{keyword}】")
            logger.info(f"{'='*40}")

            # 搜索 (包含错误处理)
            if resume_record_start == 1:
                if not perform_search(driver, wait, keyword):
                    logger.warning(f"搜索 {keyword} 失败或无结果，保存跳过状态")
                    save_state(idx + 1, 1)
                    continue
            else:
                logger.info(f"断点恢复 (记录 {resume_record_start})，重新执行搜索以进入结果页...")
                if not perform_search(driver, wait, keyword):
                     logger.warning(f"断点恢复搜索 {keyword} 失败")
                     continue

            # 获取数量
            total_records = get_total_records(wait)
            if total_records == 0:
                logger.warning(f"关键词 {keyword} 结果为 0，跳过")
                save_state(idx + 1, 1)
                # 即使无结果，也要尝试回到高级检索页
                try: driver.get(WOS_URL_ROOT)
                except: pass
                continue

            current_start_record = resume_record_start if idx == start_kw_index else 1
            if idx != start_kw_index: resume_record_start = 1
            
            chunk_index = (current_start_record // MAX_EXPORT_PER_CHUNK) + 1

            while current_start_record <= total_records:
                end_record = min(current_start_record + MAX_EXPORT_PER_CHUNK - 1, total_records)
                success = export_record_range(driver, wait, keyword, chunk_index, current_start_record, end_record)
                
                if success:
                    next_start = end_record + 1
                    save_state(idx, next_start)
                    current_start_record = next_start
                    chunk_index += 1
                    if current_start_record <= total_records:
                        time.sleep(random.uniform(2, 4)) 
                else:
                    logger.error("导出失败，等待 10秒后重试当前块...")
                    time.sleep(10)
            
            logger.info(f"关键词 {keyword} 完成")
            save_state(idx + 1, 1)
            start_kw_index = -1 
            
            # 一个词完成后，必须回到高级检索页，否则下一次循环找不到输入框
            try:
                logger.info("返回 Advanced Search 准备下一轮...")
                driver.get(WOS_URL_ROOT)
                time.sleep(2)
            except: pass

        logger.info("所有关键词处理完毕！")
        if os.path.exists(STATE_FILE_PATH):
            os.remove(STATE_FILE_PATH)
            
    except KeyboardInterrupt:
        logger.info("用户中断 (Ctrl+C)，进度已保存")
    except Exception as e:
        logger.critical("发生未捕获异常")
        logger.error(traceback.format_exc())
    finally:
        logger.info("尝试合并文件...")
        try:
            merge_wos_exports_to_csv(CHROME_DOWNLOAD_DIR, OUTPUT_FILE, delete_originals=True)
            logger.info(f"合并完成: {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"合并失败: {e}")

if __name__ == "__main__":
    main_task()