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

# ================= 配置区域 =================
INPUT_CSV = 'dois.csv'       
RESULT_CSV = 'results.csv'   
PDF_DIR = 'papers'           
BASE_URL = 'https://sci-hub.st/'
DEBUG_PORT = "127.0.0.1:9333" 
# ===========================================

def clean_filename(doi):
    return doi.replace('/', '_').replace(':', '-') + '.pdf'

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", DEBUG_PORT)
    driver = webdriver.Chrome(options=options)
    return driver

def log_result(doi, status, file_path=None, message=""):
    new_row = pd.DataFrame([{
        'doi': doi,
        'status': status,
        'file_path': file_path,
        'message': message
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
        print(f"    [!] 下载流错误: {e}")
        return False

# --- 核心优化：随机等待助手 ---
def random_sleep(min_s, max_s, reason=""):
    """在 min_s 到 max_s 之间随机睡眠，并可打印原因方便调试"""
    duration = random.uniform(min_s, max_s)
    # print(f"    (等待 {duration:.2f}s: {reason})") # 调试时可取消注释
    time.sleep(duration)

def human_input(element, text):
    """模拟人类逐字输入，带打字韵律"""
    for char in text:
        element.send_keys(char)
        # 打字速度波动：0.05秒到0.25秒
        time.sleep(random.uniform(0.05, 0.25))

def main():
    if not os.path.exists(PDF_DIR):
        os.makedirs(PDF_DIR)

    print("正在连接 Chrome 调试端口...")
    try:
        driver = init_driver()
    except Exception as e:
        print(f"连接失败: {e}")
        return

    if not os.path.exists(INPUT_CSV):
        print(f"错误: 找不到 {INPUT_CSV}")
        return
    
    df = pd.read_csv(INPUT_CSV)
    col_name = df.columns[0]
    all_dois = df[col_name].astype(str).tolist()
    
    processed = set()
    if os.path.exists(RESULT_CSV):
        processed = set(pd.read_csv(RESULT_CSV)['doi'].astype(str).values)
    
    todos = [d for d in all_dois if d not in processed]
    print(f"任务统计：待处理 {len(todos)}")

    wait = WebDriverWait(driver, 20)

    for doi in todos:
        print(f"--> 处理: {doi}")
        try:
            # 1. 打开网页
            driver.get(BASE_URL)
            
            # 【随机】网页加载后的反应时间
            random_sleep(1.5, 3.0, "浏览页面") 
            
            # 2. 寻找输入框
            input_box = wait.until(EC.element_to_be_clickable((By.NAME, "request")))
            
            # 【随机】移动鼠标到输入框的犹豫时间
            random_sleep(0.5, 1.5, "瞄准输入框")
            
            # 点击聚焦
            ActionChains(driver).move_to_element(input_box).click().perform()
            
            # 【随机】点击后准备打字的反应时间
            random_sleep(0.2, 0.8, "准备打字")
            
            # 3. 清空并拟人输入
            input_box.clear()
            human_input(input_box, doi) 
            
            # 【随机】输完后“检查”一遍有没有输错的时间
            random_sleep(0.8, 2.0, "检查输入")
            
            # 4. 点击 Open 按钮
            submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'open')]")
            
            # 【随机】移动鼠标到按钮的时间
            random_sleep(0.3, 1.0, "移动到按钮")
            
            driver.execute_script("arguments[0].click();", submit_btn)
            
            # 5. 等待结果加载
            try:
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(@class, 'message')] | //*[contains(@class, 'download')]")
                ))
                # 【随机】结果出来后，人类阅读结果的反应时间
                random_sleep(1.0, 2.0, "查看结果")
                
            except TimeoutException:
                if "captcha" in driver.page_source.lower():
                    print("    [!] 遇到验证码，请手动处理！")
                    # 这里的 input 本身就是无限等待，直到你按回车
                    input("    >>> 处理完后按回车...")
                else:
                    print("    [?] 页面加载超时")
                    log_result(doi, "Timeout")
                continue

            # 6. 解析逻辑
            # 这里不需要 random_sleep，因为下面的 requests 是后台操作，网页不感知
            if len(driver.find_elements(By.CSS_SELECTOR, ".download a")) > 0:
                download_link = driver.find_elements(By.CSS_SELECTOR, ".download a")[0]
                pdf_url = download_link.get_attribute("href")
                print(f"    [v] 发现链接")
                
                save_path = os.path.join(PDF_DIR, clean_filename(doi))
                cookies = {c['name']: c['value'] for c in driver.get_cookies()}
                ua = driver.execute_script("return navigator.userAgent;")
                
                if download_file_via_requests(pdf_url, save_path, cookies, ua):
                    print(f"    [v] 下载成功")
                    log_result(doi, "Success", file_path=save_path)
                else:
                    log_result(doi, "Download Failed")
            
            elif len(driver.find_elements(By.CSS_SELECTOR, ".message")) > 0:
                msg_text = driver.find_element(By.CSS_SELECTOR, ".message").text
                if "Alas" in msg_text:
                    print("    [x] 未收录")
                    log_result(doi, "Not Found")
                else:
                    log_result(doi, "Unknown Message", message=msg_text)
            else:
                log_result(doi, "Structure Error")

        except Exception as e:
            print(f"    [!] 异常: {e}")
            log_result(doi, "Error", message=str(e))
        
        # 【随机】做完一个任务后的“休息”时间（防封核心）
        # 设置在 4 到 8 秒之间，模拟人类看会儿手机或者喝口水
        print("    ...休息中...")
        random_sleep(1.9, 4.3, "任务间歇")

if __name__ == "__main__":
    main()