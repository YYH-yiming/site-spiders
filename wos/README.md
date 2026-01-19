- 列表爬虫代码说明：
    - combine_wos_export.py 是合并单个xlsx文件, 导入到爬虫代码中使用, 作用是对爬取导出的论文信息进行合并。如果有多次中断的情况, 文件夹下面可能会有多个合并后的xlsx文件, 调用此代码再合并一次即可。
    - wos_spider_byself_range.py v1, 最低级可用版本, 查询单个检索词
    - wos_spider_byself_range_csv.py v2, 对csv中的关键词进行轮询
    - wos_export_by_last_state.py v3, 对csv中的关键词进行轮询, 支持断点续传, 记忆上一次失败时的状态
    - wos_export_by_advanced_search.py v4, 支持**WOS检索式构建**，根据SO、PY等查询。
    - 其余文件是根据业务需求产出的，比如统计某个刊多少文件、从下载出的文献里匹配城市、年份等。


- 使用流程：
    - 根据Chrome浏览器的版本, 下载对应版本的chromedriver, 放在与脚本同一目录下, 或者配置环境变量
    - 躲开wos对selenium自动化的检测, 以debug模式运行
    - 在命令行中(win + x)输入以下命令，根据自己的情况微调:
    
    ```powershell
    & "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="E:\实习\学位中心实习\wos_spider\selenium_user_dir\wos_profile"

- 弹出Google浏览器后, 打开WOS并登录, 运行以下脚本:
    python wos_export_by_last_state.py > WOS_Exported_Files\WOS_Export.log 2>&1

