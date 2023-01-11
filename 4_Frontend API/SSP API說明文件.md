# SSP API說明文件


## API Management
```csvpreview {header="false"}
Domain, https://ssp.retailing-data.net
Authorization, HSeSyNRfvAoJDFhojCVzoAA6KBhvjD15
```
## Statuscode

```csvpreview {header="false"}
code,說明
200,Success
400,Authentication Denied
404,No method found matching route
500,Server Endpoint、Network 、System Error
```


## 瓶蓋數據資料API
### StringParameter
```python=
first_date=2022-06-20 
last_date=2022-07-02 
```
---

### info
```csvpreview
Method, GET
Path, api/data/info
```
* 機台數據資料

![](https://i.imgur.com/OGcOFOt.png)

```json=
{
    "A1": {"機台閱覽群數": [66],
           "機台停留時間": [36.27]},
    "I": {"機台閱覽群數": [53],
          "機台停留時間": [36.3]}
}
```


### timeline
```csvpreview
Method, GET
Path, api/data/timeline
```
* 每日機台 人群數/停留時間

![](https://i.imgur.com/GfWZKEC.png)

```json=
{
    "A1": {
        "date": [
            "2022-06-29",
            "2022-06-30",
            ...
        ],
        "機台閱覽群數": [
            6,
            8,
            ...
        ],
        "機台停留時間": [
            45.66,
            13.71,
            ...
        ]
        },
    "I": {
        "date": [
            "2022-06-30",
            "2022-07-01",
            ...
        ],
        "機台閱覽群數": [
            27,
            15,
            ...
        ],
        "機台停留時間": [
            36.65,
            38.65,
            ...
        ]
        }
}

```


### relation
```csvpreview
Method, GET
Path, api/data/relation
```
* 顧客屬性占比

![](https://i.imgur.com/xYOsGDk.png)

```json=
{
    "A1": {
        "relation": [
            "男",
            "女",
            "朋友"
        ],
        "屬性占比": [
            65.15,
            21.21,
            13.64
        ]
        },
    "I": {
        "relation": [
            "男",
            "女",
            "朋友"
        ],
        "屬性占比": [
            71.7,
            22.64,
            5.66
        ]
        }
}
```


### age
```csvpreview
Method, GET
Path, api/data/age
```
* 訪客年齡層占比

![](https://i.imgur.com/UGGd9rK.png)

```json=
{
    "A1": {
        "age_range": [
            "21~25",
            "26~30",
            "11~20",
            "31~40"
        ],
        "年齡層群數": [
            87,
            74,
            28,
            15]
            },
    "I": {
        "age_range": [
            "26~30",
            "21~25",
            "31~40",
            "11~20",
            "41~50"
        ],
        "年齡層群數": [
            57,
            37,
            17,
            14,
            5]
            }
}
```


### click_stay
```csvpreview
Method, GET
Path, api/data/click_stay
```
* 場域點擊次數/停留時間

![](https://i.imgur.com/vYpV43M.png)


```json=
{
    "A1": {
        "選單": {
            "item_name": ["首頁","課程","購物","地圖","歷史"],
            "點擊次數": [586,327,325,229,195],
            "停留時間": [239.93,72.17,103.2,59.0,35.57]
        },
        "地圖": {
            "item_name": ["I棟","F棟","B棟","A2棟","樹下廣場","M棟","A1棟","G棟","藝文走廊"],
            "點擊次數": [72,33,32,14,13,12,12,12,7],
            "停留時間": [27.35,14.63,14.88,3.67,1.73,3.45,2.35,3.87,1.08]
        }
    },
    "I": {
        "選單": {
            "item_name": ["首頁",...],
            "點擊次數": [67,...],
            "停留時間": [31.58,...]
        },
        "地圖": {
            "item_name": ["I棟",...],
            "點擊次數": [4,...],
            "停留時間": [1.08,...]
        }
    }
}
```


### click_stay_re
```csvpreview
Method, GET
Path, api/data/click_stay_re
```
* 訪客對場域之點擊次數/停留時間

![](https://i.imgur.com/zgbICQl.png)

```json=
{
    "A1": {
        "地圖": {
            "女": {
                "item_name": ["I棟","A1棟",...],
                "點擊次數": [3,1,...],
                "停留時間": [1.45,0.95,...]
                    },
            "情侶": {
                "item_name": ["I棟","A1棟",...],
                "點擊次數": [1,0,...],
                "停留時間": [0.15,0.28,...]
                    },
            "朋友": {
                "item_name": [...],
                "點擊次數": [...],
                "停留時間": [...]
                    },
            "男": {...},
            "家庭": {...}
                },
        "課程": {
            "女": {
                "item_name": ["當期課程","動手做岩石系皂盤"],
                "點擊次數": [2,1],
                "停留時間": [1.35,0.18]
                    },
            "情侶": {...},
            ...
                },
        "選單": {...},
        "購物": {...}
            },
    "I": {...}
}
```


### top10click
```csvpreview
Method, GET
Path, api/data/top10click
```
* 場域Top10瀏覽項目

![](https://i.imgur.com/vrdJZPf.png)

```json=
{
        "A1": {
            "課程": {
                "item_name": [
                    "團體教育局金工課",
                    "當期課程",
                    "水泥鏝抹無框畫",
                    "鐵花窗彎折工藝課",
                    "初夏釀酒日",
                    "樂齡活絡律動",
                    "黃銅開瓶戒",
                    "動手做岩石系皂盤",
                    "親子歡喜律動",
                    "植感生活萬用面紙收納套"
                ],
                "點擊次數": [
                    14,
                    10,
                    7,
                    5,
                    3,
                    3,
                    3,
                    1,
                    1,
                    1
                ]
            },
            "購物": {
                "item_name": [
                    "小圓管隨身香水洗手皂",
                    "暗黑衝鋒車",
                    "小圓瓶保濕免洗手噴霧"
                ],
                "點擊次數": [
                    2,
                    1,
                    1
                ]
            }
        },
        "I": {
            "課程": {
                "item_name": [
                    "當期課程",
                    "常態課程",
                    "親子歡喜律動",
                    "團體教育局金工課"
                ],
                "點擊次數": [
                    12,
                    1,
                    1,
                    1
                ]
            },
            "購物": {
                "item_name": [
                    "竹玻花器",
                    "小圓管隨身香水洗手皂",
                    "印第安椅",
                    "七寶花窗衛生紙盒",
                    "暗黑衝鋒車",
                    "軟水泥名片夾 黑紋石",
                    "SUCCULENT-蓮",
                    "原色-六角編小竹革提籃",
                    "小圓瓶保濕免洗手噴霧",
                    "手工折花抱枕"
                ],
                "點擊次數": [
                    4,
                    3,
                    3,
                    3,
                    2,
                    2,
                    1,
                    1,
                    1,
                    1
                ]
            }
        }
    }
```
---




## 全家數據資料API
### StringParameter
```python=
first_date=2022-10-25 
last_date=2022-11-02 
```
---

### info
```csvpreview
Method, GET
Path, api/familymart/data/info
```
* 機台前人、群數、停留時間、點擊首頁

![](https://i.imgur.com/XuKfCUi.png)

```json=
{
"機台閱覽人數": {
    "值": 89,
    "成長率": -43.0
},
"機台閱覽群群": {
    "值": 63,
    "成長率": -39.0
},
"平均機台停留時間": {
    "值": 10.2,
    "成長率": -2.0
},
"機台點擊首頁人數": {
    "值": 51,
    "成長率": -40.0
}
```

### timeline
```csvpreview
Method, GET
Path, api/familymart/data/timeline
```
* 每日機台 人數/停留時間

![](https://i.imgur.com/j7RtUAv.png)

```json=
{
    "2022-10-25": {
        "全": {
            "人數": [255],
            "停留時間": [14.3]
        },
        "女": {
            "人數": [75],
            "停留時間": [10.09]
        },
        "男": {
            "人數": [180],
            "停留時間": [16.06]
        }
    },
    "2022-10-26": {
        "全": {
            "人數": [35],
            "停留時間": [15.07]
        },
        "女": {
            "人數": [4],
            "停留時間": [10.64]
        },
        "男": {
            "人數": [31],
            "停留時間": [15.64]
        }
    },
    ...
}
```

### relation
```csvpreview
Method, GET
Path, api/familymart/data/relation
```
* 訪客屬性占比

![](https://i.imgur.com/pmZPwKV.png)

```json=
{
    "relation_group": [
        "male",
        "female",
        "friend",
        "couple",
        "family"
    ],
    "屬性占比": [
        70.4,
        28.04,
        1.25,
        0.31,
        0.0
    ]
}
```

### age_gender
```csvpreview
Method, GET
Path, api/familymart/data/age_gender
```
* 各年齡層訪客數

![](https://i.imgur.com/eVwvlwV.png)

```json=
{
    "gender": [
        "男",
        "男",
        "男",
        "男",
        "女",
        "女",
        "女",
        "女"
    ],
    "age_range": [
        "25歲以下",
        "26-35歲",
        "36-45歲",
        "46歲以上",
        "25歲以下",
        "26-35歲",
        "36-45歲",
        "46歲以上"
    ],
    "counts": [
        183,
        161,
        19,
        6,
        55,
        52,
        22,
        2
    ]
}
```

### scatter
```csvpreview
Method, GET
Path, api/familymart/data/scatter
```
* 功能使用散佈圖

![](https://i.imgur.com/qyTf2hZ.png)

```json=
{
    "品牌": {
        "name": [
            "KIWES",
            "全家科館店",
            "八方雲集",
            "可不可熟成紅茶",
            "安妞韓國烤肉食堂",
            "山本茉莉服飾",
            "樂玩咖啡",
            "漢堡王"
        ],
        "click": [
            9,
            19,
            2,
            2,
            4,
            4,
            1,
            4
        ],
        "time": [
            0.28,
            2.31,
            0.28,
            0.0,
            0.72,
            0.41,
            0.0,
            0.28
        ]
    },
    "活動": {
        "name": [
            "Fami 週期購",
            "Let’s Café經典系列",
            "免費借杯 循環愛地球",
            "全家禮物卡",
            "就是要喝酷冰沙",
            "悠遊卡All in 嗶卡賺5%",
            "潔衣家",
            "職人美味金咖哩",
            "雙10狂歡歡慶來囉",
            "點數交換上線囉"
        ],
        "click": [
            1,
            7,
            1,
            1,
            3,
            2,
            1,
            5,
            10,
            4
        ],
        "time": [
            0.5,
            1.71,
            0.13,
            0.12,
            0.7,
            0.55,
            0.12,
            0.59,
            2.26,
            0.53
        ]
    },
    "產品": {
        "name": [
            "原味可樂餅",
            "奶油蕈菇松露燉飯",
            "新仙女醇奶茶",
            "棉被仙女紅茶",
            "棉被蜜香紅茶",
            "檸檬奶香霜鬆餅",
            "經典黑炫牛奶酷繽沙",
            "綠豆蒜牛奶酷繽沙",
            "美式辣味雞球分享包",
            "茶",
            "茶2",
            "麻油肉絲炒飯"
        ],
        "click": [
            1,
            1,
            2,
            18,
            1,
            2,
            1,
            5,
            1,
            23,
            3,
            5
        ],
        "time": [
            0.5,
            0.12,
            0.77,
            8.39,
            0.5,
            0.78,
            0.37,
            2.32,
            0.5,
            10.44,
            0.67,
            2.35
        ]
    }
}
```

### visiters
```csvpreview
Method, GET
Path, api/familymart/data/visiters
```
* 訪客屬性點擊排行

![](https://i.imgur.com/thvGQti.png)

```json=
{
    "buttom": {
        "品牌": {
            "樂玩咖啡": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0
                ]
            }
        },
        "活動": {
            "全家禮物卡": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0
                ]
            },
            "雙10狂歡歡慶來囉": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    0.0,
                    3.0,
                    0.0,
                    0.0,
                    0.0
                ]
            }
        }
    },
    "top": {
        "品牌": {
            "樂玩咖啡": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0
                ]
            }
        },
        "活動": {
            "全家禮物卡": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0
                ]
            },
            "雙10狂歡歡慶來囉": {
                "relation": [
                    "male",
                    "female",
                    "friend",
                    "couple",
                    "family"
                ],
                "value": [
                    0.0,
                    3.0,
                    0.0,
                    0.0,
                    0.0
                ]
            }
        }
    }
}
```

### recommend
```csvpreview
Method, GET
Path, api/familymart/data/recommend
```
* Top 10 相關好物產品點擊

![](https://i.imgur.com/3acJg3r.png)

```json=
{
    "item_name": [
        "茶2"
    ],
    "count": [
        3
    ]
}
```

### filter
```csvpreview
Method, GET
Path, api/familymart/data/filter
```
* Top 5 篩選項目點擊次數

![](https://i.imgur.com/guM9A28.png)

```json=
{
    "活動": {
        "filter_name": [
            "會員優惠",
            "主題活動",
            "支付優惠"
        ],
        "count": [
            11,
            7,
            1
        ]
    },
    "產品": {
        "filter_name": [
            "匠吐司",
            "媽媽煮藝",
            "金系列",
            "飲品",
            "鬆餅"
        ],
        "count": [
            1,
            1,
            1,
            1,
            1
        ]
    }
}
```
