# -*- coding: utf-8 -*-
# Web of Science (WOS) 期刊发文量统计脚本
# 功能：读取期刊列表 -> 搜索 SO=期刊名 -> 记录文章总数 -> 输出 JSON 和 CSV

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
# WOS 入口链接
WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/smart-search'

# 工作目录 (请根据实际情况修改)
WORK_DIR = r'E:\wos_spider'
# 结果保存目录
RESULT_DIR = os.path.join(WORK_DIR, 'WOS_Count_Results_1')

# 输入文件
CSV_FILE_PATH = os.path.join(WORK_DIR, 'E:\wos_spider\Result_B_in_A _1.csv') # 只要第一列是期刊名即可

# 输出文件
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_JSON = os.path.join(RESULT_DIR, f'journal_counts_{timestamp}.json')
OUTPUT_CSV = os.path.join(RESULT_DIR, f'journal_counts_summary_{timestamp}.csv')
STATE_FILE_PATH = os.path.join(RESULT_DIR, 'wos_count_state.json')

# 爬虫参数
WAIT_TIMEOUT = 40

# =====================================================
# XPATH 定义 (保持与原脚本一致)
# =====================================================
XPATH_INPUT_COMMON = '//input[@id="composeQuerySmartSearch"]'
XPATH_SEARCH_BTN_HOME = '//button[@aria-label="Submit your question"]'
XPATH_SEARCH_BTN_RESULT = '//button[@aria-label="Search"]'
XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]'
XPATH_SEARCH_ERROR_ALERT = '//div[contains(@class, "error-code") and @role="alert"]'

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
# 状态管理函数
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
    """保存当前进度和已统计的数据"""
    data = {"kw_index": kw_index, "results": results}
    try:
        with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

# =====================================================
# 辅助功能函数
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
                # 清洗期刊名，去除多余空格
                journals.append(row[0].strip())
    logger.info(f"已加载 {len(journals)} 个期刊")
    return journals

def setup_driver():
    chrome_options = Options()
    # 保持调试端口，方便接管已登录的浏览器
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def human_type(driver, element, text):
    actions = ActionChains(driver)
    actions.click(element).perform()
    time.sleep(0.2)
    for c in str(text):
        actions.send_keys(c)
        actions.pause(random.uniform(0.03, 0.1)) # 打字稍微快一点
    actions.perform()

# =====================================================
# 核心业务逻辑
# =====================================================
def perform_search(driver, wait, journal_name):
    # 构建 SO (Publication Name) 查询
    # 如果期刊名包含特殊字符，建议加上引号，但 WOS 智能搜索通常不需要
    # 这里处理一下 AND OR NOT 避免作为逻辑词
    query_safe = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), journal_name)
    search_query = f'SO="{query_safe}"' # 加上引号更精确
    
    logger.info(f"正在检索: {search_query}")

    # 尝试检测旧结果，用于判断刷新
    old_records_element = None
    try:
        elements = driver.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT)
        if elements:
            old_records_element = elements[0]
    except: pass

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
                logger.error("未找到搜索按钮")
                return False
        except Exception as e:
            logger.error(f"点击搜索按钮失败: {e}")
            return False

        # 等待页面刷新（如果之前有结果）
        if old_records_element:
            try:
                WebDriverWait(driver, 5).until(EC.staleness_of(old_records_element))
            except: pass
        
        # 竞态等待：成功 OR 报错 OR 无结果
        def check_search_status(d):
            # 1. 报错
            if d.find_elements(By.XPATH, XPATH_SEARCH_ERROR_ALERT):
                return "ERROR"
            # 2. 成功显示数字
            if d.find_elements(By.XPATH, XPATH_TOTAL_RECORDS_COUNT):
                return "SUCCESS"
            # 3. 无结果
            if d.find_elements(By.XPATH, "//div[contains(text(), 'No records match your query')]"):
                return "NO_RECORDS"
            return False

        time.sleep(10)
        try:
            status = WebDriverWait(driver, WAIT_TIMEOUT).until(check_search_status)

            if status == "ERROR":
                logger.warning(f" >>> [错误] WOS 提示语法错误: {journal_name}")
                driver.refresh()
                time.sleep(3)
                return False

            elif status == "NO_RECORDS":
                logger.info(f" >>> [结果] 0 篇 (无匹配记录)")
                return 0 # 特殊返回值

            elif status == "SUCCESS":
                return True

        except TimeoutException:
            logger.error(f" >>> [超时] 搜索无响应")
            driver.refresh()
            return False

    except Exception as e:
        logger.error(f"搜索过程异常: {e}")
        return False

def get_total_records(wait):
    try:
        element = wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_TOTAL_RECORDS_COUNT)))
        text = element.text.replace(',', '').strip()
        total = int("".join(filter(str.isdigit, text)))
        return total
    except Exception as e:
        logger.warning(f"无法获取数字: {e}")
        return 0

# =====================================================
# 主任务
# =====================================================
def main_task():
    # 解决控制台乱码
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore', line_buffering=True)
    
    global logger
    logger = setup_logger(RESULT_DIR)
    logger.info("=== 期刊计数脚本启动 ===")

    journals = read_journals(CSV_FILE_PATH)
    if not journals: return

    # 加载状态
    state = load_state()
    start_index = state['kw_index']
    results_dict = state['results'] # 格式: {"Nature": 100, "Science": 200}

    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        logger.info("浏览器连接成功")
    except Exception as e:
        logger.critical(f"浏览器连接失败: {e}")
        return

    # 首次运行提示
    if start_index == 0:
        logger.info("请确保浏览器已登录 WOS。按 Enter 键开始...")
        input()
    else:
        logger.info("断点恢复模式。按 Enter 继续...")
        input()

    try:
        for idx, journal in enumerate(journals):
            if idx < start_index:
                continue
            
            logger.info(f"进度: {idx+1}/{len(journals)} - 期刊: 【{journal}】")
            
            # 执行搜索
            search_result = perform_search(driver, wait, journal)
            
            count = 0
            if search_result is True:
                # 搜索成功，获取数量
                count = get_total_records(wait)
                logger.info(f" >>> 文章数: {count}")
            elif search_result == 0:
                # 明确无结果
                count = 0
            else:
                # 报错或超时，标记为 -1 或保留 0，这里记录 -1 代表异常
                logger.warning(f" >>> 跳过或异常，记为 -1")
                count = -1
            
            # 更新结果字典
            results_dict[journal] = count
            
            # 保存状态
            save_state(idx + 1, results_dict)
            
            # 随机休眠防封
            time.sleep(random.uniform(2, 4))

        logger.info("所有期刊统计完毕！")
        
        # 删除状态文件（因为已经完成了）
        if os.path.exists(STATE_FILE_PATH):
            os.remove(STATE_FILE_PATH)

    except KeyboardInterrupt:
        logger.info("用户手动停止")
    except Exception as e:
        logger.critical(f"运行时发生错误: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        # =================================
        # 最终输出逻辑
        # =================================
        logger.info("正在生成最终报告...")
        
        # 1. 过滤掉异常数据 (-1) 进行求和
        valid_counts = [v for v in results_dict.values() if v > 0]
        total_sum = sum(valid_counts)
        
        # 2. 输出 JSON
        try:
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, ensure_ascii=False, indent=4)
            logger.info(f"JSON 文件已保存: {OUTPUT_JSON}")
        except Exception as e:
            logger.error(f"JSON 保存失败: {e}")

        # 3. 输出 CSV
        try:
            with open(OUTPUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Journal Name', 'Article Count']) # 表头
                
                for k, v in results_dict.items():
                    writer.writerow([k, v])
                
                writer.writerow([])
                writer.writerow(['TOTAL SUM', total_sum]) # 总和行
            
            logger.info(f"CSV 文件已保存: {OUTPUT_CSV}")
            logger.info(f" >>>>>> 所有期刊文章总和: {total_sum} <<<<<<")
            
        except Exception as e:
            logger.error(f"CSV 保存失败: {e}")

if __name__ == "__main__":
    main_task()