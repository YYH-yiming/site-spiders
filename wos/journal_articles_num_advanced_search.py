# -*- coding: utf-8 -*-
# Web of Science (WOS) 期刊发文量统计脚本 (高级检索版)
# 功能：读取期刊列表 -> 构造 SO=xxx AND PY=xxx -> 统计文章总数 -> 输出 JSON 和 CSV

import os
import re
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
from selenium.common.exceptions import TimeoutException

# =====================================================
# 全局配置参数
# =====================================================
# [修改] WOS 高级检索入口
WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/advanced-search'

# 工作目录
WORK_DIR = r'E:\wos_spider'
# 结果保存目录
RESULT_DIR = os.path.join(WORK_DIR, 'WOS_Count_Results_advanced_search')

# 输入文件 (第一列为期刊名)
CSV_FILE_PATH = os.path.join(WORK_DIR, "E:\wos_spider\dowanload_papers_20260109.csv")

# [新增] 年份限制 (留空则不限制)
# 格式示例: "2023" 或 "2020-2024"
TARGET_YEAR = "2020-2025"

# 输出文件
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_JSON = os.path.join(RESULT_DIR, f'journal_counts_{timestamp}.json')
OUTPUT_CSV = os.path.join(RESULT_DIR, f'journal_counts_summary_{timestamp}.csv')
STATE_FILE_PATH = os.path.join(RESULT_DIR, 'wos_count_state.json')

# 爬虫参数
WAIT_TIMEOUT = 30

# =====================================================
# XPATH 定义 (适配高级检索)
# =====================================================
# 高级检索输入框
XPATH_ADVANCED_INPUT = '//textarea[@id="advancedSearchInputArea"]'
# 搜索按钮
XPATH_ADVANCED_SEARCH_BTN = '//button[@data-ta="run-search"]'
# 清除按钮
XPATH_ADVANCED_CLEAR_BTN = '//button[@data-ta="clear-search"]'
# 错误/无结果提示条
XPATH_ADVANCED_ERROR_ALERT = '//div[contains(@class, "search-error") and contains(@class, "error-code")]'

# 结果数字 (跳转后的页面)
XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]'

# =====================================================
# 日志系统配置
# =====================================================
def setup_logger(log_dir):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, f'count_spider_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger = logging.getLogger('wos_counter')
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
# 状态管理
# =====================================================
def load_state():
    """读取断点，同时读取已经保存的统计结果"""
    if not os.path.exists(STATE_FILE_PATH):
        return {"kw_index": 0, "results": {}}
    try:
        with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return {"kw_index": 0, "results": {}}
            data = json.loads(content)
            logger.info(f"检测到断点：第 {data.get('kw_index', 0)+1} 个期刊，已统计 {len(data.get('results', {}))} 条数据")
            return data
    except Exception as e:
        logger.warning(f"读取状态文件失败，将从头开始: {e}")
        return {"kw_index": 0, "results": {}}

def save_state(kw_index, results):
    """保存当前进度"""
    data = {"kw_index": kw_index, "results": results}
    try:
        with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

# =====================================================
# 辅助功能
# =====================================================
def read_journals(csv_path):
    journals = []
    if not os.path.exists(csv_path):
        logger.error(f"未找到 CSV 文件: {csv_path}")
        return []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():
                journals.append(row[0].strip())
    logger.info(f"已加载 {len(journals)} 个期刊")
    return journals

def setup_driver():
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
        actions.pause(random.uniform(0.02, 0.08)) # 打字速度
    actions.perform()

# =====================================================
# 核心业务逻辑 (Advanced Search)
# =====================================================
def perform_search_and_count(driver, wait, journal_name):
    # 1. 构造高级检索式
    # 过滤特殊字符
    safe_name = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), journal_name)
    
    if TARGET_YEAR:
        # 使用 SO="Name" AND PY=Year
        search_query = f'SO="{safe_name}" AND PY={TARGET_YEAR}'
    else:
        search_query = f'SO="{safe_name}"'
    
    logger.info(f"检索式: {search_query}")

    # 2. 确保在 Advanced Search 页面
    if "advanced-search" not in driver.current_url:
        logger.info("跳转回 Advanced Search...")
        driver.get(WOS_URL_ROOT)
        time.sleep(2)

    try:
        # 3. 定位并清空输入框
        search_box = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_ADVANCED_INPUT)))
        
        # 尝试点击 Clear 按钮
        try:
            clear_btn = driver.find_elements(By.XPATH, XPATH_ADVANCED_CLEAR_BTN)
            if clear_btn and clear_btn[0].is_enabled():
                clear_btn[0].click()
                time.sleep(0.3)
        except: pass

        # 双重保险清空
        search_box.click()
        search_box.send_keys(Keys.CONTROL, "a")
        search_box.send_keys(Keys.BACKSPACE)
        time.sleep(0.5)

        # 4. 输入检索式
        human_type(driver, search_box, search_query)
        time.sleep(0.5)

        # 5. 点击搜索
        try:
            search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_ADVANCED_SEARCH_BTN)))
            search_btn.click()
        except Exception as e:
            logger.error(f"点击搜索按钮失败: {e}")
            return -1

        time.sleep(3)
        # 6. 竞态等待：成功跳转显示数字 OR 页面报错
        # Advanced Search 如果无结果，会在当前页面弹红框，不会跳转
        
        def check_status(d):
            # 检查报错/无结果提示
            errors = d.find_elements(By.XPATH, XPATH_ADVANCED_ERROR_ALERT)
            if errors and errors[0].is_displayed():
                return "ERROR_ALERT"
            
            # 检查成功跳转后的数字
            counts = d.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
            if counts and counts[0].is_displayed():
                return "SUCCESS"
            
            return False

        time.sleep(5)
        try:
            status = WebDriverWait(driver, WAIT_TIMEOUT).until(check_status)

            if status == "ERROR_ALERT":
                # 获取错误文本，判断是“无结果”还是“语法错误”
                alert_text = driver.find_element(By.XPATH, XPATH_ADVANCED_ERROR_ALERT).text
                if "no results" in alert_text.lower():
                    logger.info(" >>> [结果] 0 篇 (无匹配)")
                    # 刷新一下去除红框，以免影响下一次
                    driver.refresh()
                    return 0
                else:
                    logger.warning(f" >>> [警告] WOS 报错: {alert_text}")
                    driver.refresh()
                    return -1 # 标记为错误

            elif status == "SUCCESS":
                # 获取数字
                element = driver.find_element(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
                text = element.text.replace(',', '').strip()
                count = int("".join(filter(str.isdigit, text)))
                logger.info(f" >>> [结果] {count} 篇")
                return count

        except TimeoutException:
            logger.error(" >>> [超时] 搜索无响应")
            driver.refresh()
            return -1

    except Exception as e:
        logger.error(f"检索异常: {e}")
        return -1

# =====================================================
# 主任务
# =====================================================
def main_task():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore', line_buffering=True)
    
    global logger
    logger = setup_logger(RESULT_DIR)
    logger.info(f"=== WOS 期刊计数 (高级检索) 启动 ===")
    logger.info(f"年份限制: {TARGET_YEAR if TARGET_YEAR else '无'}")

    journals = read_journals(CSV_FILE_PATH)
    if not journals: return

    state = load_state()
    start_index = state['kw_index']
    results_dict = state['results']

    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        logger.info("浏览器连接成功")
    except Exception as e:
        logger.critical(f"浏览器连接失败: {e}")
        return

    if start_index == 0:
        try:
            driver.get(WOS_URL_ROOT)
            logger.info("请登录 WOS。看到 Advanced Search 界面后，按 Enter 开始...")
            input()
        except: pass
    else:
        logger.info("断点模式启动。按 Enter 继续...")
        input()

    try:
        for idx, journal in enumerate(journals):
            if idx < start_index: continue
            
            logger.info(f"-"*40)
            logger.info(f"进度: {idx+1}/{len(journals)} - 期刊: 【{journal}】")
            
            # 执行核心计数逻辑
            retry_wait_seconds = 20
            while True:
                count = perform_search_and_count(driver, wait, journal)
                if count != -1:
                    # success
                    break
                else:
                    logger.warning(f"[搜索失败]，等待{retry_wait_seconds}秒后重试")
                    time.sleep(retry_wait_seconds)
                    try:
                        driver.get(WOS_URL_ROOT)
                        time.sleep(5)
                    except:
                        pass
            
            # 记录结果
            # count = -1 代表出错， count = 0 代表无结果
            results_dict[journal] = count
            
            # 保存状态
            save_state(idx + 1, results_dict)
            
            # 只有当跳转到了结果页(count > 0)，才需要退回到 Advanced Search
            # 如果是 0 或 -1，因为我们在 perform_search 内部刷新了，或者本来就还在高级检索页
            # 为了保险起见，可以统一检测 URL
            if "advanced-search" not in driver.current_url:
                driver.get(WOS_URL_ROOT)
            
            time.sleep(random.uniform(1.5, 3))

        logger.info("所有期刊统计完毕！")
        if os.path.exists(STATE_FILE_PATH):
            os.remove(STATE_FILE_PATH)

    except KeyboardInterrupt:
        logger.info("用户手动停止")
    except Exception as e:
        logger.critical(f"运行时错误: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        # =================================
        # 输出最终报告
        # =================================
        logger.info("正在生成统计报告...")
        
        valid_counts = [v for v in results_dict.values() if v > 0]
        total_sum = sum(valid_counts)
        
        # JSON
        try:
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, ensure_ascii=False, indent=4)
        except: pass

        # CSV
        try:
            with open(OUTPUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Journal Name', 'Article Count', 'Note'])
                
                for k, v in results_dict.items():
                    note = ""
                    if v == -1: note = "Error/Skipped"
                    elif v == 0: note = "No Records"
                    writer.writerow([k, v, note])
                
                writer.writerow([])
                writer.writerow(['TOTAL VALID SUM', total_sum])
            
            logger.info(f"CSV 已保存: {OUTPUT_CSV}")
            logger.info(f" >>>>>> 总发文量: {total_sum} <<<<<<")
        except Exception as e:
            logger.error(f"CSV 保存失败: {e}")

if __name__ == "__main__":
    main_task()