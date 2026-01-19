# -*- coding: utf-8 -*-
# Web of Science (WOS) 范围导出脚本（加强版 - 按记录范围导出）
# 使用你手动启动的 Chrome，并在 Chrome 默认下载目录内重命名文件。

import os
import glob
import time
import random
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException
)

# ----------------------
# 配置参数
# ----------------------

WOS_URL_ROOT = 'https://www.webofscience.com/wos/woscc/smart-search'
# 最终目标存储目录：仅用于最终提示，文件不会被移动到这里
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'WOS_Exported_Files')
# ❗ 必须根据你的系统修改：Chrome 实际下载到的临时目录（C:盘）。文件将在此目录内重命名。
CHROME_DOWNLOAD_DIR = 'C:\\Users\\Administrator\\Downloads' 
MAX_EXPORT_PER_CHUNK = 1000 # WOS 单次最大导出记录数

SEARCH_KEYWORD = "music software" # 默认搜索关键词，可根据需要修改

WAIT_TIMEOUT = 30
PAUSE_TIME = 5
DOWNLOAD_WAIT_TIME = 30

# ----------------------
# XPATH 定义
# ----------------------

XPATH_SEARCH_BOX = '//input[@id="composeQuerySmartSearch"]'
XPATH_SEARCH_BUTTON = '//button[@aria-label="Submit your question"]'

# **已修改**：总记录数 XPATH，指向包含数字的 <span> 元素
XPATH_TOTAL_RECORDS_COUNT = '//h1[contains(@class, "search-info-title")]/span[@class="brand-blue"]' 

XPATH_EXPORT_BUTTON = '//button[@id="export-trigger-btn"]'
XPATH_EXPORT_TO_EXCEL = '//button[@id="exportToExcelButton"]'

# 范围导出相关的 XPATH
# 再次修改：定位回 mat-radio-button 容器，这次将使用 ActionChains 进行点击
XPATH_RECORDS_RANGE_RADIO = '//mat-radio-button[contains(., "Records from:")]' 
# input 元素 name 属性为 markFrom/markTo
XPATH_START_INPUT = '//input[@name="markFrom"]' 
XPATH_END_INPUT = '//input[@name="markTo"]'

XPATH_CONTENT_DROPDOWN_BUTTON = '//wos-select/button[@aria-haspopup="listbox"]'
XPATH_CONTENT_FULL_RECORD_OPTION = '//div[@aria-label="Full Record"]'
XPATH_FINAL_EXPORT_BUTTON = '//button[@id="exportButton"]'


# =====================================================
# 浏览器启动：连接你手动启动的 Chrome 
# =====================================================
def setup_driver(download_dir):

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    chrome_options = Options()

    # 关键：连接你手动启动的 Chrome
    chrome_options.add_experimental_option(
        "debuggerAddress",
        "127.0.0.1:9222"
    )

    driver = webdriver.Chrome(
        options=chrome_options,
    )

    print("已连接到你手动启动的 Chrome")
    return driver


# =====================================================
# 伪装真人输入
# =====================================================
def human_type(driver, element, text):
    actions = ActionChains(driver)
    for c in str(text):
        actions.send_keys(c)
        actions.pause(random.uniform(0.15, 0.35))
    actions.perform()


# =====================================================
# 获取总记录数
# =====================================================
def get_total_records(wait):
    print("[步骤 0] 正在获取总记录数...")
    try:
        # 使用 visibility_of_element_located 确保元素已在 DOM 中并且可见
        element = wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_TOTAL_RECORDS_COUNT)))
        
        # 获取 span 中的文本，即数字部分（例如 "1,277"）
        text = element.text.replace(',', '').strip()
        
        # 尝试从文本中提取数字
        total = int("".join(filter(str.isdigit, text)))
        print(f"[成功] 检索到总记录数：{total} 条。")
        return total
    except Exception as e:
        print(f"[严重错误] 无法获取总记录数，请检查页面元素或等待时间。错误: {e}")
        return 0


# =====================================================
# 下载检测 (只在 CHROME_DOWNLOAD_DIR 内重命名)
# =====================================================
def wait_for_download_and_rename(keyword, chunk_index, start_record, end_record, min_wait=DOWNLOAD_WAIT_TIME, total_timeout=180):
    
    monitor_dir = CHROME_DOWNLOAD_DIR
    time.sleep(min_wait)
    start_time = time.time()

    while time.time() - start_time < total_timeout:
        # 搜索 Chrome 默认下载目录中的文件
        files = glob.glob(os.path.join(monitor_dir, "savedrecs*"))
        
        for filepath in files:
            if filepath.endswith(".crdownload"):
                continue

            # 检查文件大小是否稳定（下载完成）
            size1 = os.path.getsize(filepath)
            time.sleep(3)
            size2 = os.path.getsize(filepath)
            
            if size1 == size2:
                
                ext = os.path.splitext(filepath)[1]
                safe_kw = keyword.replace(" ", "_")
                # 文件名包含记录范围和块序号
                new_name = f"WOS_Export_{safe_kw}_Chunk_{chunk_index}_{start_record}-{end_record}{ext}"
                
                # 目标路径与旧路径在同一目录，实现重命名 (避免 WinError 17)
                new_path = os.path.join(monitor_dir, new_name) 
                
                try:
                    os.rename(filepath, new_path)
                    print(f"[成功] 文件已在下载目录 ({monitor_dir}) 中重命名为: {new_name}")
                    return True
                except Exception as rename_error:
                    print(f"[错误] 文件重命名失败: {rename_error}")
                    return False

        time.sleep(5)

    print("[错误] 文件下载超时")
    return False


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

        set_input(start_el, arguments[0]);
        set_input(end_el, arguments[1]);
    """

    driver.execute_script(js_set_value, str(start_record), str(end_record))
    time.sleep(1.5)


# =====================================================
# 导出指定记录范围
# =====================================================
def export_record_range(driver, wait, keyword, chunk_index, start_record, end_record):

    print(f"\n====== 开始导出第 {chunk_index} 块 ({start_record} - {end_record}) ======")
    time.sleep(random.uniform(PAUSE_TIME, PAUSE_TIME + 2)) 

    try:
        # 1. 点击 '导出' 主按钮
        print("[步骤 1] 点击 '导出' 主按钮...")
        export_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_BUTTON)))
        # export_button.click() # 简单点击即可
        ActionChains(driver).move_to_element(export_button).click(export_button).perform()
        
        time.sleep(random.uniform(2, 4)) 

        # 2. 点击 '导出到 Excel' 选项
        print("[步骤 2] 点击 '导出到 Excel'...")
        excel_button = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_EXPORT_TO_EXCEL)))
        # excel_button.click() # 简单点击即可
        ActionChains(driver).move_to_element(excel_button).click(excel_button).perform()
        
        # 增加等待时间，等待模态框充分稳定
        time.sleep(random.uniform(5, 7)) 

        print("[步骤 3] 强制选中 'Records from:' ...")

        # 等待弹窗内出现 mat-radio-button
        records_mat = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//mat-radio-button[contains(., 'Records from')]")
            )
        )

        # 找到内部原生 input
        native_input = records_mat.find_element(By.CSS_SELECTOR, "input[type='radio']")

        # 强制滚动到可见
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", native_input)
        time.sleep(0.5)

        # ----------- 终极方案：直接触发 Angular 的 change -----------
        js = """
        var input = arguments[0];
        var previous = input.checked;

        // 强制设置选中
        input.checked = true;

        // 构造 Angular 期望的事件
        var event = new Event('change', { bubbles: true });
        input.dispatchEvent(event);

        // 有些 Angular 版本还需要 click 事件
        var clickEvent = new MouseEvent('click', { bubbles: true });
        input.dispatchEvent(clickEvent);
        """

        driver.execute_script(js, native_input)

        print("[成功] 已强制选中 'Records from:'，等待输入框激活...")
        time.sleep(1.5)


        
        print("[步骤 4、5] 强制写入范围值...")
        force_set_range(driver, start_record, end_record)


        # 6. 展开内容选择下拉框
        print("[步骤 6] 展开内容下拉框...")
        dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_DROPDOWN_BUTTON)))
        dropdown.click()

        time.sleep(2)

        # 7. 选择 'Full Record'
        print("[步骤 7] 选择 'Full Record'...")
        full_record = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_CONTENT_FULL_RECORD_OPTION)))
        full_record.click()

        time.sleep(2)

        # 8. 点击最终 'Export' 按钮
        print("[步骤 8] 点击最终 'Export'...")
        final_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_FINAL_EXPORT_BUTTON)))
        final_btn.click()

        # 9. 等待下载完成和重命名
        # wait_for_download_and_rename(keyword, chunk_index, start_record, end_record)
        return True

    except Exception as e:
        print(f"[错误] 导出第 {chunk_index} 块 ({start_record}-{end_record}) 失败: {e}")
        return False


# =====================================================
# 主流程 (基于记录范围)
# =====================================================
def main_export_task():

    driver = setup_driver(DOWNLOAD_DIR)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    keyword = SEARCH_KEYWORD

    try:
        driver.get(WOS_URL_ROOT)

        print("\n请手动登录 Web of Science，然后按 Enter 继续...")
        input()

        time.sleep(PAUSE_TIME)

        print(f"[操作] 开始搜索：{keyword}")

        # --- 1. 执行搜索 ---
        search_box = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SEARCH_BOX)))
        search_box.click()
        time.sleep(1)

        human_type(driver, search_box, keyword)
        time.sleep(random.uniform(1.5, 3.0))

        search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_SEARCH_BUTTON)))
        search_btn.click()

        # 增加等待时间，确保搜索结果页面完全加载，包括总记录数
        print(f"[等待] 结果加载中...")
        time.sleep(PAUSE_TIME + 10) # 暂停 15 秒

        # --- 2. 获取总记录数 ---
        total_records = get_total_records(wait)
        if total_records == 0:
            print("[结束] 未找到记录或无法获取总数。")
            return

        # --- 3. 循环按范围导出 ---
        start_record = 1
        chunk_index = 1
        exported_count = 0

        while start_record <= total_records:
            # 计算当前块的结束记录
            end_record = min(start_record + MAX_EXPORT_PER_CHUNK - 1, total_records)
            
            # 导出当前范围
            if export_record_range(driver, wait, keyword, chunk_index, start_record, end_record):
                exported_count += (end_record - start_record + 1)
            else:
                print(f"[警告] 块 {chunk_index} 导出失败，跳过并尝试下一块。")

            # 准备下一块的起始记录
            start_record = end_record + 1
            chunk_index += 1

            # 每次导出之间增加延迟，避免被服务器拒绝
            if start_record <= total_records:
                print(f"\n[等待] 暂停 {PAUSE_TIME + 5} 秒，准备导出下一块...")
                time.sleep(PAUSE_TIME + 5)


        print(f"\n任务完成！")
        print(f"总记录数: {total_records}")
        print(f"成功导出记录数: {exported_count}")
        print(f"所有文件已保存在：{CHROME_DOWNLOAD_DIR} (已重命名)")

    except Exception as e:
        print(f"\n[致命错误] 主任务失败: {e}")

    finally:
        # driver.quit() # 调试时可注释
        pass 


if __name__ == "__main__":
    if 'C:\\Users\\Administrator\\Downloads' in CHROME_DOWNLOAD_DIR:
        print("❗ 注意：请确认 CHROME_DOWNLOAD_DIR 配置参数是你的 Chrome 实际下载目录。文件将保留在该目录中。")
    main_export_task()