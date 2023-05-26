###### tags: `DS文件`, `演算法`
# CDP建案推薦系統(Recommender System)
## 更新紀錄
```csvpreview {header="true"}
日期,內容,更新者
2023/5/9,first edit, Calvin
2023/5/26,add model explain, Calvin
```
---
## 推薦系統類別
![](https://hackmd.io/_uploads/SJJbZCTS2.png)

[圖片來源](https://towardsdatascience.com/recommendation-system-in-python-lightfm-61c85010ce17)

### **內容過濾**
將商品做分類，比較商品的屬性，找出商品跟商品之間的關聯，進而找出最相似的商品推薦。
    - 優點: 速度快、占用資源少、可應用於新商品
    - 缺點: 準確率低，未考慮商品暢銷程度、使用者偏好

### **協同過濾**
結合眾人的意見，通過分析使用者或者事物之間的相似性(「協同」)，來預測使用者可能感興趣的內容並將此內容推薦給使用者。
![](https://i.imgur.com/91GXaot.png)

[資料來源:維基百科](https://zh.wikipedia.org/wiki/%E5%8D%94%E5%90%8C%E9%81%8E%E6%BF%BE)

### **混合推薦**
結合上述兩種方法，建立推薦系統。

---


## CDP推薦系統 - LightFM Model
以CF Model-Based(利用機器學習、深度學習技術優化推薦)的LightFM(2015)建置。
### 1. 模型說明
如果使用一般Memory-Based的CF會有冷啟動(新用戶、新建案無預約資料而無法推薦)及稀疏性(通常用戶及建案多，而真正有預約的資料少，造成資料過於龐大而無用)問題，LightFM模型增進了模型的預測力及解決以上兩個問題。
* 增加features交互作用來增加推薦資訊，進而提升預測力。Ex:假設選了兩種features(用戶性別及地區)，則新增了(男 * 北部)、(女 * 北部)、(男 * 中部)...等交互資訊。
* 解決冷啟動問題: 使用用戶Metadata(性別、年齡等)、建案Metadata(類別、地區等)資料作為比對新用戶及新用品的特徵。Ex:假設A為(男,20歲)被推薦建案a，B為(男,30歲)被推薦建案b，新用戶C為(男,20歲)，則會將建案a作為C用戶的推薦。
* 解決稀疏性問題: 做矩陣上的轉換，減少矩陣維度，進而減少資料空間及計算時間。

### 2. 使用資料
* 預約資料、LINE建案資料、點擊建案資料
1. 日期
2. 人員
3. 建案
* 會員相關
1. 性別
2. 年齡
3. 職業
4. 婚姻
5. 家庭成員
6. 居住城市
7. LINE推案熱點
8. LINE OA活動
* 建案相關
1. 城市
2. 類別
3. 格局
4. 坪數
5. 樓層
6. 建築設計


### 3. 模型
#### Step 1. 資料前處理
##### A. 預約(互動)資料
依客戶要求或實務經驗判斷預約、點擊之重要程度而給予不同權重。
##### B. 會員、建案特徵
極端值排除、遺失值處理。
#### Step 2.稀疏矩陣建置
##### A. 會員及建案交互ID
先加入有互動資料後加入無互動資料。
##### B. 建置稀疏矩陣
1. 互動資料稀疏矩陣
若有需要做測試驗證，可將資料拆分為訓練集及測試集。(目前設定訓練集70%、測試集30%)
2. 會員特徵稀疏矩陣
3. 建案特徵稀疏矩陣
##### Step 3. 模型建置
建置模型並訓練模型，可用類似grid_search(網格搜尋)尋找較佳參數。

[官方文件參數定義](https://making.lyst.com/lightfm/docs/lightfm.html)

模型範例:

```python=
model = LightFM(loss = 'warp',
                random_state = 42,
                learning_rate = 0.5,
                no_components = 50,
                user_alpha = 0.000005,
                item_alpha = 0.000005)
model = model.fit(interactions = interactions_train,
                  user_features = user_features,
                  item_features = item_features,
                  epochs = 100,
                  verbose = True)
```
超參數(learning_schedule)比較範例圖:
![](https://hackmd.io/_uploads/SJCfbATrn.png)


超參數(no_components)比較範例圖:
![](https://hackmd.io/_uploads/HyNQWR6Sn.png)


### 4. 驗證與評估
1. auc_score
2. precision_at_k
3. recall_at_k (目前採用此方法，k設定為5)

判斷每位會員正確預測推薦佔真正有預約(互動)之比率。

Ex: A會員真正有預約(a,b,c,d,e)，模型推薦(a,b,c,f,g)，recall=TP/(TP+FN)=3/(3+2)=0.6。

[評估指標官方文件參考](https://making.lyst.com/lightfm/docs/lightfm.evaluation.html)


### 5. 預測
1. 每位會員top5推薦建案。
![](https://hackmd.io/_uploads/rygEWA6Sn.png)


2. 每建案推薦會員名單。

目前採用模型推薦分數>0即加入名單。
![](https://hackmd.io/_uploads/ry5NZRpH3.png)








