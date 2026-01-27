- 使用Chrome浏览器，在设置中确认自己Chrome浏览器的版本。然后到https://googlechromelabs.github.io/chrome-for-testing/ 下载对应的chromedriver，解压出chromedriver.exe 放到代码同级目录下。
- 在当前代码目录下创建文件夹：selenium_user_dir
- 准备以Debugger模式启动Chrome浏览器。按 win+x，选择终端管理员，输入以下命令。
    ``` powershell
    & <your chrome.exe path> --remote-debugging-port=<port> 
    exe" --user-data-dir=<....\selenium_user_dir\wos_profile>
    ``` 
    其中chrome.exe根据自己的安装路径来，端口与代码相同就选9333，使用刚刚创建的在当前代码目录下创建文件夹：selenium_user_dir路径，后面加一个wos_profile即可如下是一个例子：
    ```powershell
    & "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9333 --user-data-dir="E:\产出\爬虫代码备份\sci-hub\selenium_user_dir\wos_profile"
    ```
- 根据需求修改代码download_pdf_by_doi.py里面关于路径的相关配置。启动代码 :
    ``` powershell
    python download_pdf_by_doi.py
    ```