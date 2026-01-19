# -*- coding: utf-8 -*-
# Web of Science (WOS) 批量关键词 + 范围导出脚本 (智能等待 + 日志记录版 + 健壮性增强)

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
# WOS 入口链接
WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/smart-search'

# 您的工作目录
WORK_DIR = r'E:\wos_spider'
# 结果保存目录
DOWNLOAD_DIR = os.path.join(WORK_DIR, 'WOS_Exported_Files')
# Chrome 默认下载路径 (必须准确)
CHROME_DOWNLOAD_DIR = r'C:\Users\admin\Downloads' 

# 输入文件和输出文件
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(DOWNLOAD_DIR, f'WOS_Merged_Results_Final_{timestamp}.csv')
CSV_FILE_PATH = os.path.join(WORK_DIR, '期刊列表.csv')
STATE_FILE_PATH = os.path.join(DOWNLOAD_DIR, 'wos_spider_state.json')

# 爬虫参数
# 一次导出多少
MAX_EXPORT_PER_CHUNK = 1000
WAIT_TIMEOUT = 40
PAUSE_TIME = 5

# =====================================================
# XPATH 定义
# =====================================================
XPATH_INPUT_COMMON = '//input[@id="composeQuerySmartSearch"]'
XPATH_SEARCH_BTN_HOME = '//button[@aria-label="Submit your question"]'
XPATH_SEARCH_BTN_RESULT = '//button[@aria-label="Search"]'
XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]'
XPATH_EXPORT_BUTTON = '//button[@id="export-trigger-btn"]'
XPATH_EXPORT_TO_EXCEL = '//button[@id="exportToExcelButton"]'
XPATH_CONTENT_DROPDOWN_BUTTON = '//wos-select/button[@aria-haspopup="listbox"]'
XPATH_CONTENT_FULL_RECORD_OPTION = '//div[@aria-label="Full Record"]'
XPATH_FINAL_EXPORT_BUTTON = '//button[@id="exportButton"]'

# [新增] 搜索错误提示框 (红色报错条)
XPATH_SEARCH_ERROR_ALERT = '//div[contains(@class, "error-code") and @role="alert"]'

# =====================================================
# 日志系统配置
# =====================================================
def setup_logger(log_dir):
    """配置日志系统：同时输出到控制台和文件"""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # 注意：这里修正了路径分隔符，确保 logs 文件夹存在
    log_subdir = os.path.join(log_dir, 'logs')
    if not os.path.exists(log_subdir):
        os.makedirs(log_subdir)
        
    log_filename = os.path.join(log_subdir, f'spider_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger = logging.getLogger('wos_spider')
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        logger.handlers.clear()
    
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s', 
        datefmt='%H:%M:%S'
    )
    
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
# 核心业务逻辑 (已更新)
# =====================================================
def perform_search(driver, wait, keyword):
    
    query = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), keyword)
    search_query = f'SO={query}'
    logger.info(f"正在检索: {search_query}")

    old_records_element = None
    try:
        elements = driver.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
        if elements:
            old_records_element = elements[0]
            logger.info("页面存在旧检索总数，等待搜索后刷新")
    except:
        pass

    try:
        search_box = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_INPUT_COMMON)))
        
        # 清空输入框
        search_box.click()
        time.sleep(0.2)
        search_box.send_keys(Keys.CONTROL, "a") 
        time.sleep(0.2)
        search_box.send_keys(Keys.BACKSPACE)
        time.sleep(0.5)

        human_type(driver, search_box, search_query)
        time.sleep(1)

        try:
            home_btn = driver.find_elements(By.XPATH, XPATH_SEARCH_BTN_HOME)
            result_btn = driver.find_elements(By.XPATH, XPATH_SEARCH_BTN_RESULT)

            if home_btn and home_btn[0].is_displayed():
                home_btn[0].click()
            elif result_btn and result_btn[0].is_displayed():
                result_btn[0].click()
            else:
                logger.error("未找到搜索按钮！")
                return False
        except Exception as e:
            logger.error(f"点击搜索按钮失败: {e}")
            return False

        if old_records_element:
            try:
                WebDriverWait(driver, 10).until(EC.staleness_of(old_records_element))
                logger.info("已刷新")
            except TimeoutException:
                logger.warning("未刷新")
        
        time.sleep(3)
        # =======================================================
        # [核心修改] 状态监控：竞态等待 (Race Wait)
        # 只要出现"报错"、"成功记录数"或"无结果"中的任意一个，就停止等待
        # =======================================================
        logger.info("等待检索响应 (成功结果 OR 报错提示)...")
        
        def check_search_status(d):
            # 1. 检查失败信号 (红色报错条)
            error_alerts = d.find_elements(By.XPATH, XPATH_SEARCH_ERROR_ALERT)
            if error_alerts and error_alerts[0].is_displayed():
                return "ERROR"
            
            
            # 2. 检查成功信号 (结果总数)
            success_flags = d.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
            if success_flags and success_flags[0].is_displayed():
                return "SUCCESS"
            
            # 3. 检查无结果提示 (No records found)
            # 这种情况下页面不会报错，但也没有总数，需要单独识别
            no_records = d.find_elements(By.XPATH, "//div[contains(text(), 'No records match your query')]")
            if no_records and no_records[0].is_displayed():
                return "NO_RECORDS"

            return False # 继续等待

        try:
            time.sleep(10)
            # 循环检测状态，直到 WAIT_TIMEOUT (40秒)
            status = WebDriverWait(driver, WAIT_TIMEOUT).until(check_search_status)

            if status == "ERROR":
                logger.warning(f" >>> [跳过] 检测到 WOS 语法报错: {keyword}")
                # 必须刷新页面，否则报错条会挡住下一次搜索
                try:
                    driver.refresh()
                    time.sleep(3) 
                except: pass
                return False

            elif status == "NO_RECORDS":
                logger.warning(f" >>> [跳过] 关键词 {keyword} 搜索成功但无结果。")
                return False 

            elif status == "SUCCESS":
                logger.info(" >>> [成功] 结果页面已加载")
                return True

        except TimeoutException:
            logger.error(f" >>> [失败] 搜索响应超时 ({WAIT_TIMEOUT}秒内未检测到明确状态)")
            # 超时后刷新一下，防止页面卡死
            try:
                driver.refresh()
                time.sleep(3)
            except: pass
            return False

    except Exception as e:
        logger.error(f"搜索过程异常: {e}")
        return False

def get_total_records(wait):
    try:
        # 这里的等待时间可以缩短了，因为 perform_search 已经确认页面加载好了
        element = wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_TOTAL_RECORDS_COUNT)))
        text = element.text.replace(',', '').strip()
        total = int("".join(filter(str.isdigit, text)))
        logger.info(f"总记录数：{total}")
        return total
    except Exception as e:
        logger.warning(f"无法获取总记录数 (可能为0或元素未加载): {e}")
        return 0

def export_record_range(driver, wait, keyword, chunk_index, start_record, end_record):
    """
    导出单个块，使用智能等待检测窗口消失
    """
    logger.info(f" >>> 正在导出块 {chunk_index} (记录 {start_record} - {end_record})")
    try:
        # 1. 导出主按钮
        export_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_BUTTON)))
        driver.execute_script("arguments[0].click();", export_button)
        time.sleep(1) 

        # 2. Excel 选项
        excel_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_TO_EXCEL)))
        driver.execute_script("arguments[0].click();", excel_button)
        
        # 3. 等待弹窗出现
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "app-export-out-details")))
        time.sleep(1) 

        # 4. 选中 Records from Radio
        records_mat = wait.until(EC.presence_of_element_located((By.XPATH, "//mat-radio-button[contains(., 'Records from')]")))
        native_input = records_mat.find_element(By.CSS_SELECTOR, "input[type='radio']")
        driver.execute_script("arguments[0].click();", native_input)
        time.sleep(0.5)

        # 5. 输入范围
        force_set_range(driver, start_record, end_record)

        # 6. 下拉选全记录
        try:
            dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_DROPDOWN_BUTTON)))
            dropdown.click()
            time.sleep(0.5)
            full_record = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_FULL_RECORD_OPTION)))
            full_record.click()
            time.sleep(0.5)
        except:
            pass 

        # 7. 最终导出
        final_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_FINAL_EXPORT_BUTTON)))
        final_btn.click()
        
        # ==========================================
        # 智能等待核心逻辑
        # ==========================================
        logger.info(" >>> 等待导出窗口关闭...")
        start_wait_time = time.time()
        
        try:
            # 等待 app-export-out-details 元素不可见
            wait.until(EC.invisibility_of_element_located((By.TAG_NAME, "app-export-out-details")))
            
            elapsed = time.time() - start_wait_time
            logger.info(f" >>> [成功] 窗口已关闭 (耗时 {elapsed:.2f}s)，下载已触发")
            
            time.sleep(2) 
            return True

        except TimeoutException:
            logger.warning(" >>> [超时] 等待窗口关闭超过30秒，尝试按ESC...")
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(2)
            return True
            
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
    logger.info("=== 脚本启动 ===")

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

    if start_kw_index == 0:
        try:
            driver.get(WOS_URL_ROOT)
            logger.info("请手动登录 WOS。页面加载完毕后，按 Enter 键开始...")
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
            logger.info(f"进度: {idx+1}/{len(keywords)} - 关键词: 【{keyword}】")
            logger.info(f"{'='*40}")

            # 搜索
            if not perform_search(driver, wait, keyword):
                logger.warning(f"搜索 {keyword} 失败或被跳过")
                save_state(idx + 1, 1)
                continue

            # 获取数量
            total_records = get_total_records(wait)
            if total_records == 0:
                logger.warning(f"关键词 {keyword} 结果为 0，跳过")
                save_state(idx + 1, 1)
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
            time.sleep(2)

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