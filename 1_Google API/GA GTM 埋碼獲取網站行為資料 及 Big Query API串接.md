###### tags: `GA`/`GTM`
# GA GTM 埋碼獲取網站行為資料 及 Big Query API串接

## 客戶端GA GTM設定文件教學
[GA及GTM埋設技術說明文件(合作方說明文件)](https://hackmd.io/KVx1_7IsSQCONcptBID5gA?view#%E4%BD%BF%E7%94%A8%E6%83%85%E5%A2%83)


## 我方GA-GTM埋碼
### GTM 設定事件
1. 主要分為兩部分: 觸發條件 與 變數，兩者合成為一個代碼

2. 需進行GA4之設定，點選新增代碼，選擇 GA4 設定，並輸入評估ID，觸發條件為init AllPage，即可追蹤GA4本身預設之事件
![](https://i.imgur.com/lO9YNxR.png)

3. 設定變數以取得欲取得之資料。Ex:想抓取點擊網站目錄之資料，設定下圖之變數以抓取。

設定：
```javascript
function(){
  
  if({{Click Classes}}=="q-tab__label"){
    menu_name = {{Click Text}}
  } else if({{Click Classes}}=="q-tab__indicator absolute-right"){
    menu_name = {{Click Element}}.previousSibling.firstChild.nextSibling.innerText
  } else if({{Click Classes}}=="q-icon notranslate material-icons q-tab__icon"){
    menu_name = {{Click Element}}.nextSibling.innerText
  } else {
    menu_name = ""
  }
  
  return menu_name
}
```
結果：
![](https://i.imgur.com/ECDXkbd.png)

4. 設定觸發條件
![](https://i.imgur.com/ZsHnN8j.png)

5. 設定代碼
![](https://i.imgur.com/QAoimp8.png)

6. 提交

7. 於GCP中創建專案並於GA串接BigQuery專案
[詳細操作請參考GA GTM官方文件](https://support.google.com/analytics/answer/10089681)
或下方BigQuery資訊

## BigQuery

1. BigQuery 提供當天完整資料與串流資料，可提升資料完整性並減少資料處理複雜度，減少API使用量
4. BigQuery 根據使用量與查詢量進行收費，基本上若不使用串流則所需之費用不高

### Big Query 收費



* 並行運算單元: 類似於虛擬CPU之概念，與執行之效率與併行數掛勾
[收費參考資料](https://cloud.google.com/bigquery/pricing?hl=zh-tw#overview_of_pricing)
#### 查詢部分
* 1. 以量計價
    *  前1TB免費，使用完之後 每TB 5.75 USD
    *  最多可使用共用區中2,000 個並行運算單元，由BigQuery自身進行調用，上限不訂
* 2. 固定費率
    * 以下以固定100個並行運算單元為單位進行計算，收費與該租用時段內處理之資料量無關
    ```csvpreview
    繳費方式,每月,地區
    月繳,2300,Taiwan
    年繳,1955,Taiwan
    彈性(60秒),3358(1小時4.6),Taiwan
    
    ```
#### 儲存空間
* 每月前10 GB免費
```csvpreview
動態儲存,0.02 per GB,90天內曾進行更動
長期儲存,0.01 per GB,90天以上未進行更動(可進行查詢及資料匯出)
```
* 超過1PB則需額外定價

#### 資料擷取
* 批量匯出/入則不須費用

* 1. 串流匯入
```csvpreview
串流資料插入,0.05 per GB, e.g. GA4 串流(1GB 約 60 萬筆)
BigQuery Storage Write API,0.025 per GB,每個月前 2 TB 免費
```
* 2. 串流匯出
```csvpreview
BigQuery Storage Read API,1.1 per TB, 每個月前 300 TB 免費

```


## BigQuery 設定
1. 需於GA中獲得編輯者權限，並使用自身之GCP project獲取資料
2. 先於GCP中建立project，並enable BigQuery 之 API
3. 至GA中，管理>資源>產品連結，點選BigQuery 連結
4. 點擊連結，選取建立之project，並選取資料位置與串流相關設定
* 若欲追蹤APP，建議打開加入行動應用程式串流的廣告 ID
5. 確認沒問題提交即可，資料通常於設定完成之後24小時內完成串接
6. 若資料流選一天則資料通常於中午前匯入前一天之資料
7. 若選串流則資料通常僅延遲數分鐘，但一開始匯入較慢，超過24小時
8. BigQuery資料自動為無限期，僅需注意若超過上限可能停止匯出資料(e.g. 儲存空間，GA 100萬筆資料限制)

[參考文件](https://support.google.com/analytics/answer/9358801?hl=zh-Hant)
## BigQuery 獲取Table
1. 至GCP project中，選取API和服務>憑證>建立憑證
2. 依據指示建立服務帳戶
3. 點選I AM與管理>身分與存取權管理 > 授予存取權 新增主體、指派角色選擁有者
4. 點選服務帳戶>金鑰>新增金鑰>json，系統將自動下載json檔金鑰
5. 將金鑰路徑設定為python env中 GOOGLE_APPLICATION_CREDENTIALS

5. 安裝套件
```python = 
pip install --upgrade google-cloud-bigquery
```
6. 使用以下方式呼叫，即可將bigquery資料轉為dataframe並匯入DB

```python=
from google.cloud import bigquery as bq

# import os
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="burnished-yeti-348308-e3b8400aea65.json"

client = bq.Client()

query = """
    SELECT event_name,event_timestamp,value.string_value,user_pseudo_id,traffic_source.name FROM `burnished-yeti-348308.analytics_312506279.events_20220426`,unnest(user_properties)  where event_name in ("點擊"," 購物車 "," 轉換") and traffic_source.source="email"
"""
query_job = client.query(query)  # Make an API request.

rows = query_job.result().to_dataframe()

```
*** GA4 Big Query 資料架構
https://support.google.com/analytics/answer/7029846

*** BigQuery API
https://cloud.google.com/bigquery/docs/reference/libraries







