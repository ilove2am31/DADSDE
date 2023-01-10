###### tags: `其他文件`
# ETL(資料擷取(extract)、轉換(transform)、載入(load))
客戶數據平台(CDP)及智能辨識促銷(SSP)皆需要處理大量的ETL。

## CDP
* CDP由於較於模組化，資料結構變化較小，故資料存於關聯式(SQL)資料庫中。

* 如下圖分別為標籤報表資訊中的「行銷渠道互動評級」及「Top10同時購買商品」。
![](https://i.imgur.com/oLOswKf.png)
![](https://i.imgur.com/Q9zKbTM.png)

1. 則須從原始的購買資料、行銷資料、會員資料等

(購買資料)

![](https://i.imgur.com/tRzGzBW.png)

(行銷資料)

![](https://i.imgur.com/3OF9jAS.png)

(會員資料)

![](https://i.imgur.com/vHjanm2.png)

2. 做大量的ETL
參考：[ETL CDP Project](https://github.com/ilove2am31/DADSDE/blob/master/ETL/CDP%20Project/labels_report.py)

3. 產出「行銷渠道互動評級」及「Top10同時購買商品」之database table

(行銷渠道互動評級)

![](https://i.imgur.com/lEmGxeh.png)

(Top10同時購買商品)

![](https://i.imgur.com/oJeEH4M.png)

* 最後再撰寫API串接資料給前端顯示於CDP中

-----------


## SSP
* SSP由於較於屬於專案性質，資料結構變化較大，故資料存於關聯式(NoSQL)資料庫中。

* 如下圖為SSP Dashboard範例
![](https://i.imgur.com/8lMdoL0.png)

1. 則須從原始的google ga 網站行為資料等
![](https://i.imgur.com/8O0NDyP.png)


2. 做大量的ETL
參考：[ETL SSP Project](https://github.com/ilove2am31/DADSDE/tree/master/ETL/SSP%20Project)

3. 產出dashboard所需資料至NoSQl database collections中
![](https://i.imgur.com/UiSeHID.png)

* 最後撰寫API串接資料給前端顯示於SSP中







