# Importing the internal pyspark and supporting libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,ArrayType
import os
# Importing environmental variables for required for spark
os.environ["PYSPARK_PYTHON"] = "/home/ashish/anaconda3/bin/python3"
os.environ["JAVA_HOME"] = "/home/ashish/java"
os.environ["SPARK_HOME"] = "/home/ashish/spark"
os.environ["PYTHONPATH"] = os.environ["SPARK_HOME"] + "/python/"+ os.environ["PYTHONPATH"]

# Creating the spark session
def createspark():
        spark = SparkSession \
                .builder \
                .appName('TealiumPysparkSolution') \
                .master('local[*]') \
                .enableHiveSupport() \
                .getOrCreate()
        return spark

# Creating the main method for executing actual code.
def main():
        spark_seesion = createspark()
        spark_seesion.sparkContext.setLogLevel('WARN')

        # Creating customers dataframe from json file
        df_customers=spark_seesion.read\
                .format('org.apache.spark.sql.json')\
                .load('data/customers/customers.json')
        # Filtering out only customer who are active
        df_active_customer = df_customers.filter('is_active==True')

        # Creating dataframe for purchases from parquet file
        df_purchases = spark_seesion.read.parquet('data/purchases')

        # Crating the dataframe for visits from parquet files
        # Hard coding the paths foreach account from 1 to 7 as partitions are limited
        # It can be done in better way as well.
        df_visits = spark_seesion.read\
                .option('basePath','data/visits')\
                .parquet('data/visits/account=1','data/visits/account=2','data/visits/account=3',
                         'data/visits/account=4','data/visits/account=5','data/visits/account=6',
                         'data/visits/account=7'
                         )
#############################################################################################################
        # Total number of visits for each account, profile, and date
        join_condition = ['account','profile']
        join_type = 'left'
        df_join_customers_visits = df_visits\
                .join(df_active_customer,join_condition,join_type)\
                .drop('is_active')
        #df_join_customers_visits.show(n= 10)
        # Defining json schema for read json data present in value column
        json_schema = StructType (
                [
                        StructField('visitor_id',StringType(),True),
                        StructField('customer_id', StringType(), True),
                        StructField('browser', StringType(), True),
                        StructField('session_id', StringType(), True),
                        StructField('visit_number', IntegerType(), True),
                        StructField('event_ids', ArrayType(StringType()), True)
                ]
        )
        # Getting only required columns for aggregation
        df_required_columns=df_join_customers_visits.withColumn('visitor_num',F.from_json(F.col('value'),json_schema))\
                                .select(
                                        F.col('visitor_num.visit_number'),
                                        F.col('account'),
                                        F.col('profile'),
                                        F.col('dt')
                                )
        # Performing aggregation and summing total visit
        df_total_visits = df_required_columns.groupby(
                                        F.to_date(F.col('dt'), 'yyyy-MM-dd').alias('dt'),
                                        F.col('account'),
                                        F.col('profile')
                                                )\
                                        .agg(
                                                F.sum(F.col('visit_number')).alias('total_number_visits')
                                        )\
                                        .orderBy(F.col('dt'))
        # Writing the total visits back to disk in parquet format partitioned by account, profile, date
        #df_total_visits.show(n = 100)
        df_total_visits.repartition('account','profile','dt')\
                .write.partitionBy('account','profile','dt')\
                .option('compression','snappy')\
                .mode('overwrite')\
                .parquet('output/parquet_total_visit')
#############################################################################################################
        # Total purchase amount  for each account, profile, and date

        # Getting only required columns from dataframe visit
        df_required_purchase_visit = df_visits.withColumn('visitor',
                                                         F.from_json(F.col('value'), json_schema)) \
                                        .select(
                                                F.col('visitor.session_id'),
                                                F.col('visitor.customer_id'),
                                                F.col('account'),
                                                F.col('profile'),
                                                F.col('dt')
                                        )
        join_condition_pv = ['session_id']
        join_type_pv = 'inner'
        # Joining purchases and visit dataframe
        df_join_purchase_visit = df_required_purchase_visit.join(df_purchases,join_condition_pv,join_type_pv)\
                                .drop('product_codes')

        join_condition_apv = ['account','profile']
        join_type_apv ='left'
        # Joining the results of purchases and visit to get only active customers
        df_customer_purchases_visits=df_join_purchase_visit.join(df_active_customer,join_condition_apv,join_type_apv)\
                                .drop('session_id','customer_id','is_active')

        # Performing the aggregation and creating final dataframe.
        df_total_amount = df_customer_purchases_visits.groupby(
                                        F.to_date(F.col('dt'), 'yyyy-MM-dd').alias('dt'),
                                        F.col('account'),
                                        F.col('profile')
                                                )\
                                        .agg(
                                                F.round(F.sum(F.col('total_purchase_amount')),2)\
                                                    .alias('total_purchase_amount')
                                        )\
                                        .orderBy(F.col('dt'))
        # Writing results to parquet format
        df_total_amount.repartition('account', 'profile', 'dt') \
            .write.partitionBy('account', 'profile', 'dt') \
            .option('compression', 'snappy') \
            .mode('overwrite') \
            .parquet('output/parquet_total_amount')
##########################################################################################################
        # Total number of items purchased for each account, profile, and date

        # Getting only required columns from dataframe visit
        df_required_purchase_visit_code = df_visits.withColumn('visitor',
                                                          F.from_json(F.col('value'), json_schema)) \
                                        .select(
                                                F.col('visitor.session_id'),
                                                F.col('account'),
                                                F.col('profile'),
                                                F.col('dt')
                                         )

        join_condition_code = ['session_id']
        join_type_code = 'inner'
        # Joining purchases and visit dataframe
        df_join_purchase_visit_prod_code = df_required_purchase_visit_code.join(df_purchases, join_condition_code, join_type_code) \
            .drop('customer_id','total_purchase_amount','session_id')

        join_condition_code_apv = ['account', 'profile']
        join_type_code_apv = 'left'
        # Joining the results of purchases and visit to get only active customers
        df_customer_purchases_visits_code = df_join_purchase_visit_prod_code\
                                        .join(df_active_customer, join_condition_code_apv,join_type_code_apv) \
                                        .drop('customer_id', 'is_active')
        # Creating the dataframe for total item purchased
        df_items_purchased=df_customer_purchases_visits_code\
                            .select("*",F.explode(F.col('product_codes')).alias('product_exploded'))\
                            .groupby(
                                     F.to_date(F.col('dt'), 'yyyy-MM-dd').alias('dt'),
                                     F.col('account'),
                                    F.col('profile')
                                     ) \
                                 .agg(
                                        F.count(F.col('product_exploded')) \
                                        .alias('total_item_purchased')
                                ) \
                            .orderBy(F.col('dt'))

        # Writing results to parquet format
        df_items_purchased.repartition('account', 'profile', 'dt') \
            .write.partitionBy('account', 'profile', 'dt') \
            .option('compression', 'snappy') \
            .mode('overwrite') \
            .parquet('output/parquet_total_items_purchased')
####################################################################################################################
        # Total number of events leading to purchase for each account, profile, and date

        # Getting only required columns from dataframe visit
        df_visits.createOrReplaceTempView('visits')
        query = """
                select get_json_object(value,'$.session_id') as session_id,
                        translate(translate(get_json_object(value,'$.event_ids'),'[',''),']','') as event_ids,
                        account as account,profile as profile,dt as dt 
                        from visits"""
        df_visits_converted=spark_seesion.sql(query)

        join_condition_event = ['session_id']
        join_type_event = 'inner'
        # Joining purchases and visit dataframe
        df_join_purchase_visit_events = df_visits_converted.join(df_purchases, join_condition_event,
                                                                                join_type_event) \
            .drop('customer_id', 'total_purchase_amount', 'session_id','product_codes') \
            .select(F.col('account'),F.split(F.col('event_ids'),',').alias('event_ids'),F.col('profile'),F.col('dt'))

        join_condition_event_apv = ['account', 'profile']
        join_type_event_apv = 'left'
        # Joining the results of purchases and visit to get only active customers
        df_customer_purchases_visits_events = df_join_purchase_visit_events \
            .join(df_active_customer, join_condition_event_apv, join_type_event_apv) \
            .select(F.col('account'),F.col('profile'),F.col('dt'),F.explode(F.col('event_ids')).alias('events'))\
            .drop('customer_id', 'is_active')

        # Creating the dataframe for total events leading to purchase
        df_items_to_purchase = df_customer_purchases_visits_events \
            .groupby(
            F.to_date(F.col('dt'), 'yyyy-MM-dd').alias('dt'),
            F.col('account'),
            F.col('profile')
        ) \
            .agg(
            F.count(F.col('events')) \
                .alias('total_event_to_purchase')
        ) \
            .orderBy(F.col('dt'))

        # Writing results to parquet format
        df_items_purchased.repartition('account', 'profile', 'dt') \
            .write.partitionBy('account', 'profile', 'dt') \
            .option('compression', 'snappy') \
            .mode('overwrite') \
            .parquet('output/parquet_total_event_to_purchase')
#####################################################################################################################

if __name__=='__main__':
        main()
