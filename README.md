# 數據平台專案

近期所的專案內容主要在處理顧客數據平台(Customer Data Platform, CDP)及智慧零售辨識促銷系統(Smart Sales Promotion, SSP)的數據，含括了資料分析(Data Analysis, DA)、資料科學(Data Science, DS)及數據工程(Data engineering, DE)領域的工作範疇。工作內容主要分為以下5個步驟：

1. 串接一、二、三方API，獲取所需資料
2. Data Pipeline以及大量的ETL
3. 模型、演算法開發
4. 前端API
5. 自動化工作流程


## 專案簡介

### 顧客數據平台(CDP)

CDP為互動式的顧客數據平台，針對營運裝況、商品銷售狀況、網站行為等主題提供互動視覺化模組，讓企業快速掌握顧客足跡；同時也透過人工智慧/機器學習進行價值分群、推薦系統等模型，讓企業能精準行銷，進而減少成本、增加營收。

* 網站行為模組-路徑分析範例圖
![](https://i.imgur.com/LIqqSmQ.png)

* 顧客分群模組-價值分群
![](https://i.imgur.com/rFPYMXV.png)


### 智慧零售辨識促銷系統(SSP)

SSP結合人臉辨識技術及廣告推播，透過機台前置鏡頭以機器深度學習、影像辨識，即時分析前方顧客的人臉生物特徵，進而預測顧客購物偏好，為顧客達到精準廣告推播；此外，也提供數據後台供企業精準掌握顧客輪廓及喜好等資訊。

* 訪客對場域之點擊次數/停留時間範例圖

![](https://i.imgur.com/xOKmYtH.png)

* 功能使用散佈圖範例圖

![](https://i.imgur.com/MNWjkUL.png)


## 專案負責內容

### 1. 串接一、二、三方API，獲取所需資料

為了呈現CDP/SSP各種儀表板資訊，我們需要獲取或串接多方資料，如企業所收集的顧客會員/購買等資料、Google GA GTM網站行為資料、Google Ads投放廣告資料、Meta投放廣告資料等。獲取資料方式除串接API外，也會使用爬蟲、AWS S3等方式。

以Google GA GTM API為例，可參考：

* [Google GA GTM API串接文件](https://github.com/ilove2am31/DADSDE/blob/master/1_Google%20API/GA%20GTM%20%E5%9F%8B%E7%A2%BC%E7%8D%B2%E5%8F%96%E7%B6%B2%E7%AB%99%E8%A1%8C%E7%82%BA%E8%B3%87%E6%96%99%20%E5%8F%8A%20Big%20Query%20API%E4%B8%B2%E6%8E%A5.md)

* [Google GA GTM API串接語法範例](https://github.com/ilove2am31/DADSDE/blob/master/1_Google%20API/google_bq_api.py)

### 2. Data Pipeline以及大量的ETL

獲取完大量的原始數據，我們會處理大量的ETL，將資料轉換成CDP/SSP所需要的格式及欄位、表格，進而儲存至SQL/NoSQL中。

CDP/SSP ETL相關可參考：

* [CDP/SSP ETL文件](https://github.com/ilove2am31/DADSDE/blob/master/2_ETL/ETL(%E8%B3%87%E6%96%99%E6%93%B7%E5%8F%96(extract)%E3%80%81%E8%BD%89%E6%8F%9B(transform)%E3%80%81%E8%BC%89%E5%85%A5(load)).md)

* [CDP ETL語法](https://github.com/ilove2am31/DADSDE/tree/master/2_ETL/CDP%20Project)

* [SSP ETL語法](https://github.com/ilove2am31/DADSDE/tree/master/2_ETL/SSP%20Project)

### 3. 模型、演算法開發

CDP專案中部份會需要幫會員分群、貼標、銷售預測、商品推薦等資訊來協助企業行銷，我們使用AI機器學習、大數據等技術落實；像是使用k-means分群演算法搭配RFM演算法建立價值分群模型，利用pyspark中的ALS及自行開發的混合型推薦模型建置推薦系統等。

詳細內容請參考：

* [價值分群](https://github.com/ilove2am31/DADSDE/tree/master/3_Model/RFM)

* [推薦系統](https://github.com/ilove2am31/DADSDE/tree/master/3_Model/Recommender%20System)


### 4. 前端API

資料整理完成後，就可以撰寫前端API，提供前端所需要的API文件、SDK等，將資料串接至數據平台上。提供前端API有很多種方法，如FastAPI、Amazon API Gateway等。

相關範例請參考：

* [SSP API文件範例](https://github.com/ilove2am31/DADSDE/blob/master/4_Frontend%20API/SSP%20API%E8%AA%AA%E6%98%8E%E6%96%87%E4%BB%B6.md)

* [API語法範例](https://github.com/ilove2am31/DADSDE/blob/master/4_Frontend%20API/familymart_data.py)


### 5. 自動化工作流程

數據平台上的資料需要不定期更新，我們使用Apache Airflow來設定ETL的排程，讓資料可以依照所需的時段(每天、每小時、每月等)自動進行更新，並加以監控。

以下範例為利用Airflow去觸發AWS Lambda觸發ETL的function：

* [Airflow 工作排程](https://github.com/ilove2am31/DADSDE/tree/master/5_Airflow)



