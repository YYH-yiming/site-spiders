import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import requests
import os
import time
import random
import logging  # <--- 新增

# ================= 配置区域 =================
INPUT_CSV = 'doi_output.csv'       
RESULT_CSV = 'results.csv'   
PDF_DIR = 'papers'           
BASE_URL = 'https://sci-hub.st/'
DEBUG_PORT = "127.0.0.1:9333" 
LOG_FILE = 'spider_run.log'  # <--- 新增：日志文件名
# ===========================================

# --- 初始化日志配置 ---
# 这样设置后，日志既会显示在屏幕上，也会保存在文件中，且带有时间戳
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a'), # 写入文件
        logging.StreamHandler()  # 输出到控制台
    ]
)

def clean_filename(doi):
    return doi.replace('/', '_').replace(':', '-') + '.pdf'

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", DEBUG_PORT)
    driver = webdriver.Chrome(options=options)
    return driver

def log_result(doi, status, file_path=None, message=""):
    """
    记录结构化结果到CSV (用于后续数据分析)
    """
    new_row = pd.DataFrame([{
        'doi': doi,
        'status': status,
        'file_path': file_path,
        'message': message,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S") # CSV里也加一列时间
    }])
    header = not os.path.exists(RESULT_CSV)
    new_row.to_csv(RESULT_CSV, mode='a', header=header, index=False)

def download_file_via_requests(url, save_path, cookies_dict, user_agent):
    headers = {'User-Agent': user_agent}
    try:
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = 'https://sci-hub.st' + url
            
        r = requests.get(url, headers=headers, cookies=cookies_dict, timeout=60)
        if r.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(r.content)
            return True
        return False
    except Exception as e:
        logging.error(f"下载流发生网络错误: {e}")
        return False

# --- 核心优化：随机等待助手 ---
def random_sleep(min_s, max_s, reason=""):
    """在 min_s 到 max_s 之间随机睡眠"""
    duration = random.uniform(min_s, max_s)
    # 如果你想看每一次等待的日志，可以取消下面这行的注释，但通常没必要
    # logging.debug(f"等待 {duration:.2f}s: {reason}") 
    time.sleep(duration)

def human_input(element, text):
    """模拟人类逐字输入，带打字韵律"""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.25))

def main():
    if not os.path.exists(PDF_DIR):
        os.makedirs(PDF_DIR)

    logging.info(">>> 程序启动，正在连接 Chrome 调试端口...")
    
    try:
        driver = init_driver()
        # 简单验证一下是否连接成功
        current_title = driver.title
        logging.info(f"Chrome 连接成功，当前页面标题: {current_title}")
    except Exception as e:
        logging.error(f"Chrome 连接失败，请检查是否已在命令行启动浏览器。错误详情: {e}")
        return

    if not os.path.exists(INPUT_CSV):
        logging.error(f"找不到输入文件: {INPUT_CSV}")
        return
    
    # 读取输入
    try:
        df = pd.read_csv(INPUT_CSV)
        col_name = df.columns[0]
        all_dois = df[col_name].astype(str).tolist()
    except Exception as e:
        logging.error(f"读取 CSV 失败: {e}")
        return
    
    # 读取已完成进度
    processed = set()
    if os.path.exists(RESULT_CSV):
        try:
            processed_df = pd.read_csv(RESULT_CSV)
            processed = set(processed_df['doi'].astype(str).values)
        except:
            logging.warning("读取结果文件失败，可能文件为空，将重新开始记录。")
    
    todos = [d for d in all_dois if d not in processed]
    logging.info(f"任务统计：总数 {len(all_dois)} | 已完成 {len(processed)} | 待处理 {len(todos)}")

    wait = WebDriverWait(driver, 20)

    for index, doi in enumerate(todos):
        # 进度提示
        logging.info(f"[{index+1}/{len(todos)}] 正在处理: {doi}")
        
        try:
            # 1. 打开网页
            driver.get(BASE_URL)
            random_sleep(1.5, 3.0, "浏览页面") 
            
            # 2. 寻找输入框
            input_box = wait.until(EC.element_to_be_clickable((By.NAME, "request")))
            
            # 瞄准
            random_sleep(0.5, 1.5, "瞄准输入框")
            ActionChains(driver).move_to_element(input_box).click().perform()
            random_sleep(0.2, 0.8, "准备打字")
            
            # 3. 清空并拟人输入
            input_box.clear()
            human_input(input_box, doi) 
            
            # 检查
            random_sleep(0.8, 2.0, "检查输入")
            
            # 4. 点击 Open 按钮
            submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'open')]")
            random_sleep(0.3, 1.0, "移动到按钮")
            driver.execute_script("arguments[0].click();", submit_btn)
            
            # 5. 等待结果加载
            try:
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(@class, 'message')] | //*[contains(@class, 'download')]")
                ))
                random_sleep(1.0, 2.0, "查看结果")
                
            except TimeoutException:
                if "captcha" in driver.page_source.lower():
                    logging.warning(f"检测到验证码！DOI: {doi}")
                    logging.warning(">>> 请切回浏览器手动完成验证，完成后按回车继续...")
                    input() # 阻塞等待
                    logging.info("用户表示验证已完成，继续运行...")
                else:
                    logging.warning(f"页面加载超时或结构异常: {doi}")
                    log_result(doi, "Timeout")
                continue

            # 6. 解析逻辑
            # 情况 A: 成功找到下载链接
            if len(driver.find_elements(By.CSS_SELECTOR, ".download a")) > 0:
                download_link = driver.find_elements(By.CSS_SELECTOR, ".download a")[0]
                pdf_url = download_link.get_attribute("href")
                
                logging.info(f"发现下载链接: {pdf_url[:50]}...")
                
                save_path = os.path.join(PDF_DIR, clean_filename(doi))
                cookies = {c['name']: c['value'] for c in driver.get_cookies()}
                ua = driver.execute_script("return navigator.userAgent;")
                
                if download_file_via_requests(pdf_url, save_path, cookies, ua):
                    logging.info(f"下载成功: {save_path}")
                    log_result(doi, "Success", file_path=save_path)
                else:
                    logging.error(f"下载失败: {doi}")
                    log_result(doi, "Download Failed")
            
            # 情况 B: 网站明确表示未收录
            elif len(driver.find_elements(By.CSS_SELECTOR, ".message")) > 0:
                msg_text = driver.find_element(By.CSS_SELECTOR, ".message").text
                if "Alas" in msg_text:
                    logging.info(f"Sci-Hub 未收录此文章: {doi}")
                    log_result(doi, "Not Found")
                else:
                    logging.warning(f"未知提示信息: {msg_text[:50]}")
                    log_result(doi, "Unknown Message", message=msg_text)
            
            # 情况 C: 结构无法识别
            else:
                logging.error(f"页面结构无法识别: {doi}")
                log_result(doi, "Structure Error")

        except Exception as e:
            logging.error(f"处理 {doi} 时发生未捕获异常: {e}")
            log_result(doi, "Error", message=str(e))
        
        # 休息
        logging.info("...随机休息中...")
        random_sleep(1.9, 4.3, "任务间歇")

    logging.info(">>> 所有任务处理完毕。")

if __name__ == "__main__":
    main()