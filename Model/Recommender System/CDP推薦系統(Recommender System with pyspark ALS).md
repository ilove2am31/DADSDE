###### tags: `演算法`
# CDP推薦系統(Recommender System)
## 更新紀錄
```csvpreview {header="true"}
日期,內容,更新者
2022/12/6,first edit, Calvin
```
---
## 推薦系統類別
![](https://i.imgur.com/OG7ccyR.png)
(圖片來源： https://medium.com/@winnie54liu0504/%E6%8E%A8%E8%96%A6%E7%B3%BB%E7%B5%B1%E6%98%AF%E4%BB%80%E9%BA%BC-what-is-recommender-system-445abf82795)

### **內容過濾**
將商品做分類，比較商品的屬性，找出商品跟商品之間的關聯，進而找出最相似的商品推薦。
    - 優點: 速度快、占用資源少、可應用於新商品
    - 缺點: 準確率低，未考慮商品暢銷程度、使用者偏好

### **協同過濾**
結合眾人的意見，通過分析使用者或者事物之間的相似性(「協同」)，來預測使用者可能感興趣的內容並將此內容推薦給使用者。
![](https://i.imgur.com/91GXaot.png)
(from維基百科: https://zh.wikipedia.org/wiki/%E5%8D%94%E5%90%8C%E9%81%8E%E6%BF%BE)

### **混合推薦**
結合上述兩種方法，建立推薦系統。

---


## CDP RS
CDP的RS以ALS model base的CF搭配content base組合成混合推薦。
### **1. 使用Model Base CF找出每位User的top 5推薦清單**
Model Base CF: 使用各種機器學習、深度學習、強化學習等演算法建模，並以此模型預測、建立推薦清單。

* **模型概念**
![](https://i.imgur.com/xCTCrr7.png)
**矩陣分解(Matrix Factorization)**
假設消費者A、B、C、D，是否購買正餐、點心(在此稱為特徵features或潛在因子latent factor，由模型演算法得出)，可建立**使用者嵌入矩陣(user embedding matrix)**，如左下圖。
假設商品I1~I5，與商品及特徵的相關程度，可建立**商品嵌入矩陣(item embedding matrix)**，如右上圖。
由使用者嵌入矩陣 * 商品嵌入矩陣可得右下圖User-Item矩陣。其中A * I1表A對於I1商品的喜好分數。

假設消費者E僅購買過I1、I2、I5商品，我們須推薦I3、I4其中一項商品給E。我們可以看出E與B喜好商品程度最為類似，因此預測出E喜歡I3程度為4分，喜歡I4程度為1分，即推薦I3商品給E。

套用在CDP資料上: 使用者嵌入矩陣則為每位消費者對features的評價(由購買次數、點擊次數轉換而得)；商品嵌入矩陣為每項商品與徵的相關程度。得出右下圖**User-Item矩陣後產出每位消費者之top5消費清單**。

* **使用模型: 交替最小平方法(Alternating least squares, ALS)**
最小化實際喜好商品分數與預測喜好分數差
(參考文獻: https://developers.google.com/machine-learning/recommendation/collaborative/matrix,
https://blog.insightdatascience.com/explicit-matrix-factorization-als-sgd-and-all-that-jazz-b00e4d9b21ea)

* **使用資料: 購買資料、點擊資料(可視資料狀況決定是否加入)**
喜好分數 = w1 * 購買商品分數 + w2 * 點擊商品分數。
(w1,w2介於0與1之間；w1+w2=1)
(目前設定w1=0.7, w2=0.3)


### **2. 新消費者(或未購買/點擊之消費者)推薦**
新消費者、商品對於推薦系統來說皆為冷啟動問題，亦即由於沒有相關購買/點擊資料造成無法找出相似消費者或商品來建立推薦清單。
* 解決方法: 將所有消費者依年齡、性別、婚姻狀況、會員等級等特徵分群，以該群平均最高喜好分數作為該群新消費者推薦清單。


### **3. 新商品推薦**
同新消費者，新商品也會有冷啟動問題。
* 解決方法: 使用內容過濾(content-based filtering)。內容過濾僅考慮商品之間的關聯性，將商品分好類後，新商品可與過濾出來之同類商品一同推薦。Ex:新商品黑巧克力與舊商品牛奶巧克力屬同類商品，且黑巧克力與牛奶巧克力經模型計算後相關性高，若某消費者推薦清單有牛奶巧克力，則一同推薦黑巧克力給乾消費者。(可將黑巧克力增列為新商品推薦等)
(補充： 目前客戶商品皆為有購買或點擊才會放入資料庫中，)


### **4. 模型評估指標**
* quality of the predictions: 直接評估模型實際值(偏好分數)與預測值，如常見指標MSE、MAE、RMSE等。
* quality of the set of recommendations： 比較TOP N推薦清單與實際購買之間的差距，如平均準確率(Mean Average Precision, MAP)，計算有確實購買推薦商品佔有購買人數之比例。
目前CDP是以顧客是否購買所推薦top5商品昨計算，準確率約為60%，待有商品分類時可以商品分類作為準確率計算。
(參考連結： https://codingnote.cc/zh-tw/p/74475/)
(參考連結： http://sdsawtelle.github.io/blog/output/mean-average-precision-MAP-for-recommender-systems.html)


### **5. 未來發展**
有更多資料後可以部屬深度學習相關的推薦模型，如reinforcement learning(RL), Graph Neural Network(GNN)等，此些模型因模型較智慧且複雜，需要大量的資料訓練，因此未來資料量夠大時可朝這方面發展。

---


## pyspark安裝及環境設定
推薦系統通常資料量較大且pyspark Mlib有ALS套件，因此可使用pyspark als建模。但spark安裝及環境設定很麻煩，可主要參考以下網址：
* Windows
https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-on-windows/
* Ubuntu
https://levelup.gitconnected.com/how-to-delete-and-install-pyspark-on-ubuntu-4e1bbefa11a3
* 連接mysql
https://ithelp.ithome.com.tw/articles/10273548




