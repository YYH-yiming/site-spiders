import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import os
import time
import random
import logging
from urllib.parse import urljoin
import base64 # 必须导入，用于解码浏览器传回的文件流
import datetime

# ================= 配置区域 =================
INPUT_CSV = 'doi_output.csv'       # 输入文件，必须包含 DOI 列
RESULT_CSV = 'results.csv'         # 结果统计文件
PDF_DIR = 'papers'                 # PDF 保存目录
BASE_URL = 'https://sci-hub.st/'   # 初始地址，会自动跳转
DEBUG_PORT = "127.0.0.1:9333"      # 接管已打开的浏览器
LOG_FILE = 'spider_run.log'        # 详细运行日志

now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

os.makedirs("logs", exist_ok=True)
LOG_FILE = os.path.join("logs", f"spider_run_{now_str}.log")

DOWNLOAD_DIR = "download_info"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
SUCCESS_LOG = os.path.join(DOWNLOAD_DIR, f"download_success_{now_str}.txt")
FAIL_LOG = os.path.join(DOWNLOAD_DIR, f"download_fail_{now_str}.txt")

# --- 初始化日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)

def record_link_log(filepath, doi, url):
    """记录简易日志，方便后续补录"""
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(f"{doi}\t{url}\n")
    except Exception as e:
        logging.error(f"写入链接日志失败: {e}")

def clean_filename(doi):
    """将 DOI 转换为合法的文件名"""
    return doi.replace('/', '_').replace(':', '-') + '.pdf'

def init_driver():
    """连接到已打开的 Chrome"""
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", DEBUG_PORT)
    driver = webdriver.Chrome(options=options)
    # 设置脚本执行超时时间 (秒)，防止下载大文件时 Selenium 以为卡死
    driver.set_script_timeout(180) 
    return driver

def log_result(doi, status, file_path=None, message=""):
    """记录详细结果到 CSV"""
    new_row = pd.DataFrame([{
        'doi': doi,
        'status': status,
        'file_path': file_path,
        'message': message,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
    }])
    header = not os.path.exists(RESULT_CSV)
    new_row.to_csv(RESULT_CSV, mode='a', header=header, index=False)

def random_sleep(min_s, max_s, reason=""):
    """随机等待"""
    duration = random.uniform(min_s, max_s)
    time.sleep(duration)

def human_input_simulation(element, text):
    """模拟逐字输入"""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.15))

def robust_input(driver, element, text, max_retries=3):
    """
    【核心增强】强力输入模式：
    1. JS 强制聚焦
    2. 清空
    3. 输入
    4. 校验输入框内容是否正确
    """
    for i in range(max_retries):
        try:
            # 1. 显式滚动到视野中央
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", element)
            time.sleep(1)

            # 2. JS 强制聚焦 (最关键的一步，解决点不准的问题)
            driver.execute_script("arguments[0].focus();", element)
            time.sleep(1)

            # 3. 清空旧内容
            element.clear()
            
            # 4. 拟人输入
            human_input_simulation(element, text)
            
            # 5. 校验：获取输入框当前的值
            current_val = element.get_attribute('value')
            
            # 宽松匹配：只要输入框里包含了我们的 DOI 就算成功
            if current_val and text in current_val:
                return True
            else:
                logging.warning(f"输入校验失败 (第 {i+1} 次)，当前框内值: '{current_val}'")
                
        except Exception as e:
            logging.warning(f"输入操作异常: {e}")
            
        time.sleep(1.0) # 失败后冷却
        
    logging.error(f"严重错误：尝试 {max_retries} 次后仍无法正确输入 DOI！")
    record_link_log(FAIL_LOG, text, "None")
    return False

def download_via_browser_js(driver, url, save_path):
    """
    【核心增强】使用浏览器内部 JS 下载，绕过 Python Requests 403 拦截
    """
    # 这段 JS 会在浏览器内执行：fetch -> blob -> base64
    js_script = """
    var url = arguments[0];
    var callback = arguments[1];
    
    fetch(url)
        .then(response => {
            if (response.status !== 200) {
                callback("HTTP_ERROR_" + response.status);
                return;
            }
            return response.blob();
        })
        .then(blob => {
            var reader = new FileReader();
            reader.readAsDataURL(blob); 
            reader.onloadend = function() {
                callback(reader.result); // 返回 Base64 字符串
            }
        })
        .catch(error => {
            callback("JS_ERROR_" + error.toString());
        });
    """
    
    try:
        logging.info("正在调用浏览器下载引擎...")
        # execute_async_script 会挂起 Python，直到 JS 调用 callback
        result = driver.execute_async_script(js_script, url)
        
        if result and isinstance(result, str):
            if result.startswith("HTTP_ERROR"):
                logging.error(f"浏览器下载失败，服务端状态码: {result}")
                return False
            elif result.startswith("JS_ERROR"):
                logging.error(f"JS 执行错误: {result}")
                return False
            elif result.startswith("data:"):
                # 解析 Base64 (格式: data:application/pdf;base64,JVBERi0xLj...)
                try:
                    header, encoded = result.split(",", 1)
                    file_data = base64.b64decode(encoded)
                    
                    with open(save_path, "wb") as f:
                        f.write(file_data)
                    
                    # 验证文件大小
                    if os.path.getsize(save_path) > 1000:
                        return True
                    else:
                        logging.warning("下载的文件太小 (<1KB)，可能是无效文件")
                        return False
                except Exception as decode_err:
                    logging.error(f"Base64 解码失败: {decode_err}")
                    return False
            else:
                logging.error("未知的数据返回格式")
                return False
        return False
        
    except Exception as e:
        logging.error(f"JS 下载过程发生 Python 异常: {e}")
        return False

def main():
    if not os.path.exists(PDF_DIR):
        os.makedirs(PDF_DIR)

    logging.info(">>> Sci-Hub爬虫程序启动...")
    
    try:
        driver = init_driver()
        logging.info("Chrome 连接成功")
    except Exception as e:
        logging.error(f"Chrome 连接失败，请检查是否启动了调试模式: {e}")
        return

    if not os.path.exists(INPUT_CSV):
        logging.error(f"找不到输入文件: {INPUT_CSV}")
        return
    
    # 读取 DOI
    try:
        df = pd.read_csv(INPUT_CSV)
        col_name = df.columns[0]
        all_dois = df[col_name].astype(str).tolist()
    except Exception as e:
        logging.error(f"读取 CSV 失败: {e}")
        return
    
    # 读取进度（断点续传）
    processed = set()
    if os.path.exists(RESULT_CSV):
        try:
            processed_df = pd.read_csv(RESULT_CSV)
            if not processed_df.empty:
                processed = set(processed_df['doi'].astype(str).values)
        except:
            pass
    
    todos = [d for d in all_dois if d not in processed]
    logging.info(f"任务统计：总数 {len(all_dois)} | 已完成 {len(processed)} | 待处理 {len(todos)}")

    wait = WebDriverWait(driver, 20)

    for index, doi in enumerate(todos):
        logging.info(f"[{index+1}/{len(todos)}] 正在处理: {doi}")
        
        try:
            # 1. 打开网页
            driver.get(BASE_URL)
            random_sleep(3, 5) 
            
            # 2. 寻找输入框 (带重试)
            input_box = None
            try:
                input_box = wait.until(EC.element_to_be_clickable((By.NAME, "request")))
            except TimeoutException:
                logging.warning("输入框加载超时，刷新重试...")
                driver.refresh()
                random_sleep(2.0, 3.0)
                input_box = wait.until(EC.element_to_be_clickable((By.NAME, "request")))

            # === 使用强力输入函数 ===
            if not robust_input(driver, input_box, doi):
                log_result(doi, "Input Failed")
                logging.error(f"输入失败，跳过: {doi}")
                continue
            
            random_sleep(0.3, 0.8)
            
            # 3. 点击 Open 按钮 (兼容多种UI)
            submit_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'open')] | //div[@id='buttons']//button")
            if submit_btns:
                driver.execute_script("arguments[0].click();", submit_btns[0])
            else:
                # 最后的尝试：回车键
                input_box.send_keys(Keys.ENTER)
            
            # 4. 等待结果
            try:
                # 等待: 错误提示 OR 下载按钮 OR 嵌入式PDF
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(@class, 'message')] | //div[contains(@class, 'download')] | //embed[@id='pdf']")
                ))
                random_sleep(1.0, 2.0)
            except TimeoutException:
                if "captcha" in driver.page_source.lower():
                    logging.warning(">>> !!! 检测到验证码 !!! <<<")
                    logging.warning("请手动在浏览器完成验证，然后按回车继续...")
                    input() 
                else:
                    logging.warning("结果页加载超时")
                    log_result(doi, "Timeout")
                continue

            # 5. 解析页面并下载
            current_page_url = driver.current_url
            pdf_url = None
            
            # 情况 A: 典型的下载按钮页面
            download_elements = driver.find_elements(By.CSS_SELECTOR, "div.download a")
            
            # 情况 B: 直接嵌入的 PDF
            embed_elements = driver.find_elements(By.ID, "pdf")

            if len(download_elements) > 0:
                raw_url = download_elements[0].get_attribute("href")
                pdf_url = urljoin(current_page_url, raw_url)
                logging.info(f"发现下载链接: {pdf_url}")
            
            elif len(embed_elements) > 0:
                raw_url = embed_elements[0].get_attribute("src")
                pdf_url = urljoin(current_page_url, raw_url)
                logging.info(f"发现嵌入式PDF: {pdf_url}")
            
            elif "Alas" in driver.page_source or "not found" in driver.page_source.lower():
                 logging.info(f"Sci-Hub 未收录: {doi}")
                 log_result(doi, "Not Found")
                 continue
            else:
                logging.error("页面结构无法识别")
                log_result(doi, "Structure Error")
                continue

            # 6. 执行 JS 下载
            if pdf_url:
                save_path = os.path.join(PDF_DIR, clean_filename(doi))
                
                if download_via_browser_js(driver, pdf_url, save_path):
                    logging.info(f"下载成功: {save_path}")
                    log_result(doi, "Success", file_path=save_path)
                    record_link_log(SUCCESS_LOG, doi, pdf_url)
                else:
                    logging.error(f"下载失败: {doi}")
                    log_result(doi, "Download Failed")
                    record_link_log(FAIL_LOG, doi, pdf_url)

        except Exception as e:
            logging.error(f"处理 {doi} 时发生异常: {e}")
            log_result(doi, "Error", message=str(e))
        
        # 任务间随机休息，避免触发更高等级的风控
        random_sleep(2.5, 5.0)

    logging.info(">>> 所有任务处理完毕。")

if __name__ == "__main__":
    main()