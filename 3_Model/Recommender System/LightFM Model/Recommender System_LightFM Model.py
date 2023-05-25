import numpy as np
import pandas as pd
import boto3
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from lightfm.data import Dataset
from lightfm import LightFM
from lightfm.evaluation import precision_at_k, recall_at_k

cdb_id = 'customer-xxx'


def connect_DB(database = cdb_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('db_config')
    response = table.get_item( Key = {'cdb_id': database} )
    setting = response['Item']
    user = setting['user']
    passwd = setting['passwd']
    host = setting['host']
    db_name = setting['db_name']
    
    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:3306/{db_name}?charset=utf8mb4', echo=False, poolclass=NullPool)
    con = engine.connect()

    return engine, con


### Recommend k item to the user & Recommend user to items ###
def rec_topk_item_function(user_ids, item_ids, user_features, item_features, model, user_id_mapping, item_id_mapping, case_mapping, df_user_features, k=5):
    # Recommendation scores for each item to the user
    scores = model.predict(user_ids = user_ids, #0 -> member_id=1
                          item_ids = item_ids, #0 -> case_id=1
                          user_features = user_features,
                          item_features = item_features)
    scores = pd.DataFrame(scores, columns=["scores"]).reset_index().rename({"index": "item_ids"}, axis=1)
    item_score = scores.merge(item_id_mapping, on="item_ids", how="left").sort_values(by=["scores"], ascending=False)
    item_score = item_score.merge(case_mapping, on=["case_id"], how="left")
    
    # REC Top k Item to the user
    topk_item = pd.DataFrame(item_score.loc[:(k-1), "case_name"]).transpose()
    topk_item.columns = [f"case_name{num}" for num in range(1, k+1)]
    topk_item["member_id"] = user_id_mapping.loc[user_id_mapping["user_ids"] == user_ids, "member_id"].values[0]
    
    # REC users for item
    users = item_score[item_score["scores"] > 0].drop(["case_id", "item_ids"], axis=1)
    users["member_id"] = user_id_mapping.loc[user_id_mapping["user_ids"] == user_ids, "member_id"].values[0]
    
    return topk_item, users






#%%
def lightFm_rec(cdb_id):
    ### Step1. Read Data ###
    ## Interactions data ##
    engine, con = connect_DB(cdb_id)
    # Reservation Data
    path_res = ''' Select member_id, case_name, Count(*) as "times"
                    FROM Info.info_member_booking
                    Where case_name In (Select case_name From Info.info_case_info)
                    Group By member_id, case_name '''
    cursor = con.execute(path_res)
    df_res = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_res["case_name"].nunique() 
    # GA Click Case Data
    path_click_case =  ''' SELECT member_id, title as "case_name", Sum(times) as "times"
                            FROM cdp.member_online_action 
                            Where action = "2"
                            	And title In (Select case_name From Info.info_case_info)
                            Group By member_id, title; '''
    cursor = con.execute(path_click_case)
    df_click_case = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_click_case["case_name"].nunique() 
    # GA Click Reservation Data
    path_click_res =  ''' SELECT member_id, title as "case_name", Sum(times) as "times"
                            FROM cdp.member_online_action 
                            Where action = "3"
                            	And title In (Select case_name From Info.info_case_info)
                            Group By member_id, title; '''
    cursor = con.execute(path_click_res)
    df_click_res = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_click_res["case_name"].nunique()
    # Line 
    path_line = ''' SELECT member_id, line_label as "case_name", Count(*) as "times"
                    FROM Info.info_line_member_label 
                    Where type = "LINE建案" And member_id Is Not Null
                    	And line_label In (Select case_name From Info.info_case_info)
                    Group By member_id, line_label; '''
    cursor = con.execute(path_line)
    df_line = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_line["case_name"].nunique()
    
    ## User Features Data ##
    # Member_info
    path_user = ''' SELECT member_id, gender, age, career, marriage, families, city
                    FROM Info.info_member_info; '''
    cursor = con.execute(path_user)
    df_user_features = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    # Line_label
    path_line_label = ''' SELECT member_id, line_label
                            FROM Info.info_line_member_label
                            Where member_id Is Not Null;'''
    cursor = con.execute(path_line_label)
    df_user_line_features = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_user_line_features["count"] = 1
    df_user_line_features = pd.pivot_table(df_user_line_features, index="member_id", columns="line_label", values="count",
                                           aggfunc=np.sum, fill_value=0).reset_index()
    
    ## Item Features Data ##
    engine, con = connect_DB(cdb_id)
    path_item = ''' SELECT case_name, city, class, num_room, num_ping, num_floor, case_design
                    FROM Info.info_case_info; '''
    cursor = con.execute(path_item)
    df_item_features = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    df_item_features["case_id"] = [i for i in range(1, len(df_item_features)+1)]
    con.close()
    engine.dispose()
    
    
    
    ### Step2. Data Preprocessing ###
    ## Interactions data ##
    # Giving rating weights
    df_res["ratings"] = df_res["times"] * 3
    df_click_res["ratings"] = df_click_res["times"] * 2
    df_click_case["ratings"] = df_click_case["times"]
    df_line["ratings"] = df_line["times"]
    # Conbine all interaction data
    df_interactions = pd.concat([df_res, df_click_res, df_click_case, df_line])
    df_interactions = df_interactions.groupby(by=["member_id", "case_name"], as_index=False)["ratings"].sum()
    df_interactions = df_interactions.merge(df_item_features[["case_name", "case_id"]], on=["case_name"], how="left")\
        .drop(["case_name"], axis=1).sort_values(by=["member_id", "case_id"])
    df_interactions["ratings"] = df_interactions["ratings"].astype(int)
    df_interactions = df_interactions[['member_id', 'case_id', 'ratings']]
    
    ## User Features Data ##
    # Add Line label data to member data
    df_user_features = df_user_features.merge(df_user_line_features, on=["member_id"], how="left")
    # Fill na
    df_user_features["age"] = -1
    df_user_features[['gender', 'career', 'marriage', 'families', 'city']] = \
        df_user_features[['gender', 'career', 'marriage', 'families', 'city']].fillna("unknown")
    df_user_features.fillna(0, inplace=True)
    
    ## Item Features Data ##
    case_mapping = df_item_features[["case_id", "case_name"]]
    df_item_features.drop(columns=["case_name"], inplace=True)
    df_item_features = df_item_features[['case_id', 'city', 'class', 'num_room', 'num_ping', 'num_floor', 'case_design']]
    
    
    
    ### Step3. Building the ID mappings ###
    dataset = Dataset()
    dataset.fit(users = df_interactions["member_id"],
                items = df_interactions["case_id"],
                user_features = (tuple(x) for x in df_user_features.drop(["member_id"], axis=1).values),
                item_features = (tuple(x) for x in df_item_features.drop(["case_id"], axis=1).values))
    # How many users and books it knows about
    num_users, num_items = dataset.interactions_shape()
    print('Num users: {}, num_items {}.'.format(num_users, num_items))
    
    # Add other user/items that is not in interactions, 
    dataset.fit_partial(users = df_user_features["member_id"],
                        items = df_item_features["case_id"],
                        user_features = (tuple(x) for x in df_user_features.drop(["member_id"], axis=1).values),
                        item_features = (tuple(x) for x in df_item_features.drop(["case_id"], axis=1).values))
    num_users, num_items = dataset.interactions_shape()
    print('Num users: {}, num_items {}.'.format(num_users, num_items))
    
    
    ### Step4. Building the interactions matrix ###
    # 1. Interactions matrix (COO Sparse Matrix):
    interactions, interactions_weights = dataset.build_interactions(list(df_interactions.itertuples(index=False)))
    #print(repr(interactions))
    #print(repr(interactions_weights))
    
    # 2. User features matrix (CSR Sparse Matrix):
    user_features_tuple = ([(x[0], [tuple(x[i] for i in range(1, len(df_user_features.columns)))]) for x in df_user_features.values])
    user_features = dataset.build_user_features(user_features_tuple)
    #print(repr(user_features))
    
    # 3. Item features matrix (CSR Sparse Matrix):
    item_features_tuple = ([(x[0], [tuple(x[i] for i in range(1, 7))]) for x in df_item_features.values])
    item_features = dataset.build_item_features(item_features_tuple)
    #print(repr(item_features))
    
    
    
    ### Step5. Modeling ###
    model = LightFM(loss = 'warp',
                    random_state = 42,
                    learning_rate = 0.5,
                    no_components = 50,
                    user_alpha = 0.000005,
                    item_alpha = 0.000005)
    model = model.fit(interactions = interactions_weights,
                      user_features = user_features,
                      item_features = item_features,
                      epochs = 100,
                      verbose = True)
    
    
    
    ### Step6. Predict ###
    # Top 5 recommend items for each user & recommend users for items
    dftop5_user_item = pd.DataFrame()
    df_item_user = pd.DataFrame()
    item_ids = np.array(df_item_features.index,)
    user_id_mapping = pd.DataFrame.from_dict(dataset._user_id_mapping, orient='index').reset_index()
    user_id_mapping.columns = ["member_id", "user_ids"]
    item_id_mapping = pd.DataFrame.from_dict(dataset._item_id_mapping, orient='index').reset_index()
    item_id_mapping.columns = ["case_id", "item_ids"]
    
    for user_ids in user_id_mapping["user_ids"]:
       top5_item, users = rec_topk_item_function(user_ids, item_ids, user_features, item_features, model, user_id_mapping, item_id_mapping, case_mapping, df_user_features, k=5)
       dftop5_user_item = pd.concat([dftop5_user_item, top5_item])
       df_item_user = pd.concat([df_item_user, users])
    
    dftop5_user_item.reset_index(drop=True, inplace=True)
    df_item_user.sort_values(by=["case_name", "scores"], ascending=False, inplace=True)
    
    
    return dftop5_user_item, df_item_user





#%%
def to_sql(cdb_id):
    ### to_sql ###
    dftop5_user_item, df_item_user = lightFm_rec(cdb_id)
    
    engine, con = connect_DB(cdb_id)
    path = ''' Truncate Table cdp.rec_member_case; '''
    con.execute(path)
    dftop5_user_item.to_sql(name="rec_member_case", con=con, schema="cdp", index=False, if_exists="append")
    path = ''' Truncate Table cdp.rec_case_member; '''
    con.execute(path)
    df_item_user.to_sql(name="rec_case_member", con=con, schema="cdp", index=False, if_exists="append", chunksize=50000)
    con.close()
    engine.dispose()
    
    return "success"







