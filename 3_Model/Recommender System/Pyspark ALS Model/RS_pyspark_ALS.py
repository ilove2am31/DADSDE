##### Recommender System with pyspark Als model #####
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, lit, explode, col, row_number, collect_list
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import time
import boto3
import json


### Pyspark connection with MySQL ###
def spark_connectsqldb(cdb_id):
    # spark initialization #
    sc = SparkSession.builder.appName("sql").getOrCreate()
    spark = SQLContext(sc)
    # mysql configuration #
    client = boto3.client('s3')
    result = client.get_object(Bucket='rema-lambda-function', Key='config/db')
    setting = result["Body"].read().decode()
    setting = json.loads(setting)
    setting = setting[cdb_id]
    user = setting['user']
    password = setting['passwd']
    host = setting['host']
    url = f'jdbc:mysql://{user}:{password}@{host}:3306'
    
    return sc, spark, url

### Customer click/buy rating score function ###
def rating(cols):
    if cols <= per[0]:
        return 1
    elif cols <= per[1]:
        return 2
    elif cols <= per[2]:
        return 3
    elif cols <= per[3]:
        return 4
    else:
        return 5

### customers' age transform to age_range score function ### 
def age_range(age):
    if 0 < age <= 20:
        return 1
    elif 20 < age <= 40:
        return 2
    elif 40 < age <= 60:
        return 3
    elif 60 < age <= 80:
        return 4
    elif 80 < age <= 100:
        return 5
    else:
        return 0

### Achieve customer id from AWS function ###
def get_cf_id(cdb_id):
    
    client = boto3.client('s3')
    result = client.get_object(Bucket='clickforce', Key='click-force.json')
    setting = result["Body"].read().decode()
    setting = json.loads(setting)
    cf_id = setting[cdb_id]
    cf_id = cf_id.replace("-", "_")
    
    return(cf_id)


#%%
cdb_id = 'customer-laurel'

### Recommend system for users who clicked or purchased products before ###
def rec_func(cdb_id):
    ### Recommend System for purchase data ###
    ## Read Data ##
    sc, spark, url = spark_connectsqldb(cdb_id)
    query = ''' Select member_id, product_id, sum(count) as total_count From Info.info_purchase_record 
                Where product_id Not Like 'D%%' And member_id Is Not Null Group By member_id, product_id '''
    df = spark.read.format("jdbc").option("url", url).option("query", query).load()
    
    for i in df.columns:
        df = df.withColumn(i, df[i].cast("int"))
   
    ## Rating ##
    global per
    per = df.approxQuantile(col=["total_count"], probabilities=[.2, .4, .6, .8], relativeError=0)[0]
    rating_func = udf(lambda i: rating(i), IntegerType())
    df = df.withColumn("rating", rating_func(col("total_count")))

    ## RS ALS Model ##
    # Create ALS model #
    als_model = ALS(userCol = "member_id", 
                    itemCol = "product_id",
                    ratingCol = "rating", 
                    nonnegative = True, 
                    implicitPrefs = False,
                    coldStartStrategy = "drop")
    
    # Grid Search for hyperparameters #
    param_grid = ParamGridBuilder() \
                    .addGrid(als.rank, [10, 100, 2000]) \
                    .addGrid(als.regParam, [0.001, 0.01, 0.1]) \
                    .build()
    print ("Num for grids:", len(param_grid))
    
    # Build cross validation #
    cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=4)
    
    ################################################
    # ### Model evaluate ###
    # ## Train test split ##
    # train, test = df.randomSplit([0.8, 0.2], seed=42)
    
    # # Fit model ##
    # ts = time.time()
    # model = cv.fit(train)
    # best_model = model.bestModel
    # print(round((time.time() - ts)/60, 2), "mins")
    
    ## Output ##
    # # Define evaluator #
    # evaluator = RegressionEvaluator(metricName = "rmse", 
    #                                 labelCol = "rating", 
    #                                 predictionCol = "prediction")
    # print ("Evaluator for Regression:", evaluator.getMetricName())
    
    # pred = best_model.transform(test)
    # RMSE = evaluator.evaluate(pred) 
    # MAE = evaluator.evaluate(pred, {evaluator.metricName: "mae"})
    # print("RMSE:", RMSE, "MAE:", MAE)
    #################################################

    ts = time.time()
    model = cv.fit(df)
    best_model = model.bestModel
    print(round((time.time() - ts)/60, 2), "mins")
    
    # Best 10 recommendations for each member with purchased data #
    recommendations = best_model.recommendForAllUsers(10)
    recommendations = recommendations.withColumn("recommendations", explode("recommendations"))
    recommendations = recommendations.withColumn("product_id", recommendations["recommendations"]["product_id"]) \
                                        .withColumn("rating", recommendations["recommendations"]["rating"]*0.7)
    
    
    ### Recommend System for click data ###
    ## Read Data ##
    cf_id = get_cf_id(cdb_id)
    sc, spark, url = spark_connectsqldb(cdb_id="clickforce")
    query1_2 = f''' with cte1 as 
    (
    SELECT product_name, cid FROM {cf_id}.behavior_laurel
    Where event_name = 'select_product' And product_price is Not Null
    )
    SELECT b.member_id, a.product_name as product, Count(*) as total_count
    FROM cte1 as a
    Left Join customer_laurel.mapping as b
    	On a.cid = b.cid
    Where b.member_id Is Not Null
    Group By b.member_id, a.product_name '''
    df1_2 = spark.read.format("jdbc").option("url", url).option("query", query1_2).load()

    sc, spark, url = spark_connectsqldb(cdb_id)
    query1_3 = ''' SELECT product_id, product_name as product FROM Info.info_brand '''
    df1_3 = spark.read.format("jdbc").option("url", url).option("query", query1_3).load()
    df1 = df1_2.join(df1_3, on=["product"], how="left").na.drop(subset=["product_id"]).drop("product")
    del df1_2, df1_3

    df1 = df1.select([df1[i].cast("int") for i in df1.columns])

    ## Rating ##
    per = df1.approxQuantile(col=["total_count"], probabilities=[.2, .4, .6, .8], relativeError=0)[0]
    df1 = df1.withColumn("rating", rating_func(col("total_count")))
    
    ## RS ALS Model ##
    ts = time.time()
    best_model1 = als_model.fit(df1)
    print(round((time.time() - ts)/60, 2), "mins")
    
    ## Output ##
    # pred = best_model1.transform(df1)
    # RMSE = evaluator.evaluate(pred) 
    # MAE = evaluator.evaluate(pred, {evaluator.metricName: "mae"})
    # print("RMSE:", RMSE, "MAE:", MAE)
    
    # Best 10 recommendations for each member with click data #
    recommendations1 = best_model1.recommendForAllUsers(10)
    recommendations1 = recommendations1.withColumn("recommendations", explode("recommendations"))
    recommendations1 = recommendations1.withColumn("product_id", recommendations1["recommendations"]["product_id"]) \
                                        .withColumn("rating", recommendations1["recommendations"]["rating"]*0.3)
                                

    ### Join both purchase & click data ###
    recommendations = recommendations.union(recommendations1).drop("recommendations")
    recommendations = recommendations.groupBy(["member_id", "product_id"]).sum("rating") \
                        .withColumnRenamed("sum(rating)", "rating")
    ## Top10 recommends ##
    partition_gb0 = Window.partitionBy("member_id").orderBy(col("rating").desc())
    recommendations = recommendations.withColumn("top", row_number().over(partition_gb0))
    recommendations = recommendations.groupBy("member_id") \
                        .agg(collect_list("product_id").alias('product_id'), collect_list("rating").alias('rating'))
    for i in range(0, 10):
        recommendations = recommendations.withColumn(f"product_{i+1}", recommendations["product_id"][i])
        recommendations = recommendations.withColumn(f"rating{i+1}", recommendations["rating"][i])
    recommendations = recommendations.drop("product_id", "rating")
    #recommendations.printSchema()
    #recommendations.show(15)
    
    
    return recommendations


### Recommend system for new or nonpurchased users ###
def rec_allser(cdb_id):
    ### New/Unpurchase member ###
    ## Read data ##
    sc, spark, url = spark_connectsqldb(cdb_id)
    query2 = ''' SELECT member_id, gender, age, marriage, birth_month, region_sort, level FROM Info.info_member_info '''
    mem = spark.read.format("jdbc").option("url", url).option("query", query2).load()
    
    ## Data preprocess ##
    # mem.select("member_id").distinct().count() #how many members
    mem = mem.na.fill({"age": 0})
    age_func = udf(lambda i: age_range(i), IntegerType())
    mem = mem.withColumn("age", age_func(col("age")))
    
    ## Group members ##
    mem = mem.na.fill("無資料")
    partition = Window().partitionBy('new_column').orderBy(lit('A'))
    mem_gb = (mem.select("gender").distinct()) \
                .join(mem.select("age").distinct()) \
                .join(mem.select("marriage").distinct()) \
                .join(mem.select("birth_month").distinct()) \
                .join(mem.select("region_sort").distinct()) \
                .join(mem.select("level").distinct()) \
                .withColumn("new_column", lit("ABC")) \
                .withColumn("group", row_number().over(partition)).drop("new_column")

    mem = mem.join(mem_gb, on=["gender", "age", "marriage", "birth_month", "region_sort", "level"])
    mem = mem.select("member_id", "group")

    ## Group recommends ##
    recommendations = rec_func(cdb_id)
    recommendations = recommendations.join(mem, on=["member_id"], how="left")

    schema = StructType([StructField('group', IntegerType(), True),
                         StructField('product', IntegerType(), True),
                         StructField('rating', DoubleType(), True)])
    re_gb = spark.createDataFrame([], schema=schema)
    for i in range(1, 11):
        temp = recommendations.groupBy("group", f"product_{i}").sum(f"rating{i}") \
                .withColumnRenamed(f"product_{i}", "product").withColumnRenamed(f"sum(rating{i})", "rating")
        re_gb = re_gb.union(temp)
    re_gb = re_gb.orderBy(["group", "rating"], ascending=False).dropDuplicates(['group', 'product'])

    partition_gb = Window.partitionBy("group").orderBy(col("rating").desc())
    re_gb = re_gb.withColumn("top", row_number().over(partition_gb)).filter(col("top") <= 5)
    
    re_gb = re_gb.groupby("group").agg(collect_list("product").alias('product'))
    for i in [x for x in range(0, 5)]:
        re_gb = re_gb.withColumn(f"product_{i+1}", re_gb["product"][i])
    re_gb = re_gb.drop("product")
    
    ## Top5 recommends ##
    query3 = ''' Select product_id From Info.info_purchase_record 
                 Where product_id Not Like 'D%%' 
                 Group By product_id  Order By sum(count) Desc Limit 5'''
    top5 = spark.read.format("jdbc").option("url", url).option("query", query3).load()
    
    
    ### All users recommendations ###
    ## rec1 -> purchased ##
    rec1 = mem.join(recommendations.select(["member_id", "product_1", 'product_2', 'product_3', 'product_4', 'product_5']), on="member_id", how="left")
    ## rec2 -> nonpurchased ##
    rec2 = rec1.filter(rec1["product_1"].isNull())
    rec1 = rec1.na.drop(subset=["product_1"])
    rec2 = rec2.select(["member_id", "group"]).join(re_gb, on="group", how='left')

    top5_dict = {f"product_{i+1}": top5.select("product_id").collect()[i][0]
                 for i in range(0, 5)}
    rec2 = rec2.fillna(top5_dict)

    rec = rec1.drop("group").union(rec2.drop("group"))

    return rec, url


### Recommendations to mysql database ###
def run_rec(cdb_id):
    
    ### To sql ###
    rec, url = rec_allser(cdb_id)
    
    db_name = 'Rec'
    url_db = url + f'/{db_name}?charset=utf8mb4'
    rec.write.format("jdbc").mode("overwrite").option("url", url_db).option("truncate", "true").option("dbtable", "Rec_predict1").save()

    sc.stop()   
    
    return "success"



#%%
##### Valdation RS model  ####
### functions ###
def inin(prod1, real_prod_list):
    if prod1 in real_prod_list:
        return 1
    else:
        return 0

def bi(col):
    if col == 0:
        return 0
    else:
        return 1

### Validation ###
def rec_validation():
    ## Validation ##
    df2 = df.select("member_id", "product_id").union(df1.select("member_id", "product_id"))
    df_all = df2.groupby("member_id").agg(collect_list("product_id").alias('real_prod'))
    rec_all = rec.join(df_all, on="member_id", how="left")
    #rec_all.show()
    rec_all = rec_all.dropna(subset="real_prod")
    
    ## real click/purchase data whether in recommendations or not ##  
    inin_func = udf(inin, IntegerType())
    for i in range(1, 6):
        rec_all = rec_all.withColumn(f"p{i}_in", inin_func(col(f"product_{i}"), col("real_prod")))
    rec_all = rec_all.withColumn("in", col("p1_in")+col("p2_in")+col("p3_in")+col("p4_in")+col("p5_in"))
    
    binarys_func = udf(bi, IntegerType())
    rec_all = rec_all.withColumn("in_or_not", binarys_func(col("in")))
    
    ## Accuracy for RS Model ##
    rec_all.select("member_id", "product_1", "product_2", "product_3", "product_4", "product_5", "real_prod", "in_or_not").show()
    accuracy = rec_all.filter(rec_all["in_or_not"] == 1).count()*100 / rec_all.count()
    
    print(f"{round(accuracy, 2)}% of member will buy for recommends list")
    
    
