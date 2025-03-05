import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType,StructField
from pyspark.sql.functions import col, concat_ws,sha1,when
import utils

bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties


class Dim_Agent:
        
    def agent_transform(self,org_name):
        spark = utils.create_session()

        folder_prefix_1 = org_name + '/agent/'
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix_1)
        file_keys = [obj['Key'] for obj in response.get('Contents', [])]
        # file_keys = file_keys[1:]
        # defining the schema of call dataframe
        agent_schema = StructType([
            StructField("agent_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("mobile_no", LongType(), True),
            StructField("email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", LongType(), True),
            StructField("hire_date", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("status", StringType(), True),
        ])
        agent_source_df = spark.createDataFrame([], schema=agent_schema)

        for file_key in file_keys:
            file_n = file_key.split("/")[-1]
            # key = folder_prefix_1 + file_n
            # obj = s3.get_object(Bucket=bucket_name, Key=key)
            # obj = utils.chk_size(obj)
            s3_file_path = "s3a://" + bucket_name +"/"+ folder_prefix_1+file_n
            print(file_key)

            agent_df = spark.read \
            .option("multiline", "true") \
            .option("minPartitions", 4) \
            .json(s3_file_path)
            print("after reading")
            print(agent_df.rdd.getNumPartitions())
            agent_df = agent_df.repartition(4)
            print("after repartitioning")
            print(agent_df.rdd.getNumPartitions())
            # if obj == "Empty":
            #     print(f"Agent data file is empty {file_n}")
            # else:
            #     json_data = obj['Body'].read().decode('utf-8')
            #     agent_df = spark.read.json(spark.sparkContext.parallelize([json_data]))  # reading json file from s3
            agent_df = agent_df.select(col("agent_id"), col("FirstName").alias("first_name"),
                                    col("LastName").alias("last_name"),
                                    col("Phone").alias("mobile_no"), col("Email").alias("email"),
                                    col("City").alias("city"),
                                    col("Gender").alias("gender"), col("agent_age").alias("age"),
                                    col("HireDate").alias("hire_date"),
                                    col("Department").alias("department"), col("JobTitle").alias("job_title"),
                                    col("agent_status").alias("status"))

            agent_source_df = agent_source_df.unionAll(agent_df)

        agent_source_df = agent_source_df.dropDuplicates(['agent_id'])
        if agent_source_df.isEmpty():
            print("No Agent data available")
        else:
            # agent_ids = str([row.agent_id for row in agent_source_df.select("agent_id").collect()])[1:-1]
            agent_ids= ",".join("'{}'".format(x[0])for x in agent_source_df.select("agent_id").rdd.collect())
            
            if len(agent_ids) > 0:
                agent_query = '(SELECT * FROM main_agent WHERE agent_id IN ({}) AND active_flag) as query'.format(
                    agent_ids)
                agent_target_df = spark.read.jdbc(url=url, table=agent_query, properties=mysql_properties)
                if agent_target_df.isEmpty():
                    print("Source data is totally new")
                else:
                    agent_source_col_list = agent_source_df.columns[1:]
                    agent_source_df = agent_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *agent_source_col_list)).cast("string"))
                    agent_target_df = agent_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *agent_source_col_list)).cast(
                                                                "string")).drop("agent_key_id")
                    agent_merge_df = agent_source_df.join(agent_target_df,
                                                        agent_source_df.agent_id == agent_target_df.agent_id,
                                                        "left").select(agent_source_df["*"],
                                                                    agent_target_df["agent_id"].alias("new_agent_id"),
                                                                    agent_target_df["cdc_hash"].alias("new_cdc_hash"))
                    agent_merge_df = agent_merge_df.withColumn("action", when(
                        (col("agent_id") == col("new_agent_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_agent_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # agent_merge_df.show()
                    agent_merge_df = agent_merge_df.filter(col("action") != "no_action")
                    # agent_merge_df.show()
                    agent_source_df = agent_merge_df.drop("cdc_hash", "new_agent_id", "new_cdc_hash", "action")

        return agent_source_df
    