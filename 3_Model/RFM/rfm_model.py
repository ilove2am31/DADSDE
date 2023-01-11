import numpy as np
import pandas as pd
import os
from datetime import date
from dateutil.relativedelta import relativedelta
from sklearn.cluster import KMeans
import boto3
from sqlalchemy import create_engine
from sqlalchemy.types import String
# import matplotlib.pyplot as plt 
# import seaborn as sns


### Set AWS_DEFAULT_REGION ###
os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'


### Connect Mysql local database ###
def connect_DB(database='fircdp-dev'):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('db_config')
    response = table.get_item(Key={'cdb_id': database})
    setting = response['Item']
    user = setting['user']
    passwd = setting['passwd']
    host = setting['host']
    db_name = setting['db_name']
    
    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:3306/{db_name}?charset=utf8mb4', echo=False)
    con = engine.connect()

    return engine, con

### Achive transformed RFM(Recency, Frequency, Monetary) data from sql database ###
def data(cdb_id):
    # rfm data #
    engine,con  = connect_DB(cdb_id)
    path = f''' SELECT * FROM Info.info_member_stat; '''
    cursor = con.execute(path)
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    
    # members joint date data #
    path = f''' SELECT member_id, join_date as date FROM Info.info_member_info Where member Like 'M%%'; '''
    cursor = con.execute(path)
    df1 = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    
    con.close()
    engine.dispose() 
    
    return df, df1

### rfm score transformation fuction ###
# Recency #
def r_mic(col):
    if col<=60:
        return 3
    elif 60<col & col<=120:
        return 2
    elif col>120:
        return 1
    else:
        return "wrong"

# Frequency #
def f_mic(col):
    if col<=1:
        return 1
    elif col==2:
        return 2
    elif col>2:
        return 3
    else:
        return "wrong"

# Monetary #
def m_mic(col):
    if col<=1000:
        return 1
    elif 1000<col & col<=2000:
        return 2
    elif col>2000:
        return 3
    else:
        return "wrong"


### Value label functions ###
def new_cus(df):
    dates = df[0]
    m = df[1]

    if (dates>(date.today()+relativedelta(months=-1))) & (m>0):
        return "新客客群"
    else:
        return np.nan

def value_label(col):
    if col == center["SUM"].max():
        return "高價值客群"
    elif col == center["SUM"].min():
        return "流失客群"
    else:
        return "其他客群"

def loyal(df):
    label = df[0]
    fr_m = df[1]
    if label == "其他客群":
        if fr_m == center["FM-R"].max():
            return "忠誠客群"
        else:
            return "沉睡客群"
    else:
        return label


#%%
### RFM model with k-means clustering ###
def rmf_kmeans_model(cdb_id):
    # Achieve customer members' RFM and joint date data #
    df, df1 = data(cdb_id)
    df["member_id"] = df["member_id"].astype(int)
    df = pd.merge(df, df1, on="member_id", how="left")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # New customer label #
    df["label"] = df[["date", "M"]].apply(new_cus, axis=1)  
    df_new = df[df["label"] == "新客客群"].drop(["AOV"], axis=1)
    
    # RFM scores for each member #
    df = df[df["label"] != "新客客群"]
    df["R_s"] = df["R"].apply(r_mic)
    df["F_s"] = df["F"].apply(f_mic)
    df["M_s"] = df["M"].apply(m_mic)
    df_c = df[["R_s", "F_s", "M_s"]]
    
    # Find appropriate number of clusters #
    # wss = []
    # for i in range(1,11):
    #     kmeans = KMeans(n_clusters=i, init='k-means++', random_state=0)
    #     kmeans.fit(df_c)
    #     wss.append(kmeans.inertia_)
        
    # plt.plot(range(1,11), wss, marker='o')
    # plt.title('Elbow graph')
    # plt.xlabel('Cluster number')
    # plt.ylabel('WSS')
    
    # K-means Clustring Model # 
    kmeans = KMeans(n_clusters=6, init='k-means++', random_state=0)
    df_c['cluster_label'] = kmeans.fit_predict(df_c)
    # Label algorithm #
    global center
    center = pd.DataFrame(kmeans.cluster_centers_, columns=["R", "F", "M"])
    center = pd.concat([center, center.apply(sum, axis=1)], axis=1).reset_index().rename(columns={"index": "cluster_label", 0: "SUM"})
    center["label"] = center["SUM"].apply(value_label)
    center["FM-R"] = center["F"] + center["M"] - center["R"]
    center["label"] = center[["label", "FM-R"]].apply(loyal, axis=1)
    
    df_c = pd.merge(df_c, center[["cluster_label", "label"]], how="left", on="cluster_label")
    df.set_index(df_c.index, inplace=True)
    df["cluster_label"] = df_c['cluster_label']
    df_label = pd.concat([df[["member_id", "date", "R", "F", "M"]], df_c["label"]], axis=1, ignore_index=True)
    df_label.columns = ["member_id", "date", "R", "F", "M", "label"]
    df_label =  pd.concat([df_label, df_new], ignore_index=True)
    
    df_all = pd.merge(df1["member_id"], df_label, on=["member_id"], how="left")
    df_all["label"].fillna("無購買客戶", inplace=True)
    
    return df_all


### Label data to SQL database ###
def run_model(cdb_id):
    try:
        output = rmf_kmeans_model(cdb_id)
        dtype = [String(20),String(20),String(20),String(20),String(20),String(20)]
        dtypes = dict(zip(output.columns, dtype))
        
        engine, con  = connect_DB(cdb_id)
        output.to_sql(name="info_member_label", chunksize=10000, schema='Info', con=con, index=False, if_exists="replace", dtype=dtypes)
        con.close()
        engine.dispose()  
        
        return "success"
    
    except:
        return "fail"


### AWS Lambda to run code ###
def lambda_handler(event, context):
    
    cdb_id = event.get('cdb_id')
    if pd.isnull(cdb_id):        
        cdb_id = 'fircdp-dev'
    
    ans = run_model(cdb_id)
    
    return ans
    


