from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType
from pyspark.sql import SparkSession
import os
import mysql.connector as mc

# connector to mysql

mysql_config_user = 'naya'
mysql_config_password = 'NayaPass1!'
mysql_config_host = 'localhost'
mysql_config_port = '3306'

mysql_conn = mc.connect(
    user=mysql_config_user,
    password=mysql_config_password,
    host=mysql_config_host,
    port=mysql_config_port,
    autocommit=True,  # <--
    # database=mysql_database_name)
)

mysql_create_tbl_events = '''create table if not exists yad2.yad04
    (current_ts varchar (78) primary key ,
    record_id numeric NULL,
    ad_number numeric NULL,
    price varchar (20) NULL, 
    currency varchar (10) NULL, 
    city_code numeric NULL, 
    city varchar (30) NULL, 
    street varchar (78) NULL, 
    AssetClassificationID_text varchar (200) NULL, 
    coordinates varchar (200) NULL, 
    ad_date varchar (20) NULL, 
    date_added varchar (20) NULL, 
    no_of_rooms numeric NULL, 
    floor_no numeric NULL, 
    size_in_sm numeric NULL,
    price_per_SM numeric NULL);'''

mysql_create_tbl_df = '''create table if not exists yad2.df
    (city_code numeric primary key ,
    avg_SquareMeter numeric,
    avg_price_per_SM numeric ,
    count_city numeric);'''

mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_create_tbl_events)
mysql_cursor.execute(mysql_create_tbl_df)
mysql_cursor.close()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = '34.71.172.85:9092'
topics = "yad2"

spark = SparkSession\
        .builder\
        .appName("yad2")\
        .getOrCreate()

schema = StructType() \
    .add('current_ts', StringType()) \
    .add("record_id", IntegerType()) \
    .add("ad_number", IntegerType()) \
    .add("rooms", FloatType()) \
    .add("floor", FloatType()) \
    .add("SquareMeter", FloatType()) \
    .add("price", FloatType()) \
    .add("currency", StringType()) \
    .add("city_code", IntegerType()) \
    .add("city", StringType()) \
    .add("street", StringType()) \
    .add("AssetClassificationID_text", StringType()) \
    .add("coordinates", StringType()) \
    .add("date", StringType()) \
    .add("date_added", StringType())

df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option("subscribe", topics) \
    .load()

df_kafka = df_kafka.select(col("value").cast("string"))\
    .select(from_json(col("value"), schema).alias("value"))\
    .select("value.*")

df_kafka.printSchema()

df_kafka = df_kafka.withColumn("price_per_SM", df_kafka.price/df_kafka.SquareMeter)
df_kafka = df_kafka.withColumn("current_ts", current_timestamp().cast('string'))

df_CityAvgPrice = df_kafka\
    .groupby("city_code")\
    .agg(avg("SquareMeter").alias("avg_SquareMeter"),avg("price_per_SM").alias("avg_price_per_SM"),count("record_id").alias("count_city"))

df_CityAvgPrice = df_CityAvgPrice.withColumn("current_ts", current_timestamp().cast('string'))


class InvalidRecordId(Exception):
    pass


def _validate_process_row_event(events):
    print(f'Validating procss_row event: {events}')
    print(f'event["record_id"]: {events["record_id"]}')
    try:
        int(events["record_id"])
    except ValueError:
        raise InvalidRecordId(events["record_id"])


def procss_row(event):
    # connector to mysql
    mysql_conn = mc.connect(
        user=mysql_config_user,
        password=mysql_config_password,
        host=mysql_config_host,
        port=mysql_config_port,
        autocommit=True,  # <--
        # database=mysql_database_name)
    )

    print(event)
    try:
        _validate_process_row_event(event)
    except Exception as e:
        print(f'Validation error: {e}')
        return


    insert_statement = """
    INSERT INTO yad2.yad04(current_ts, record_id, ad_number, price, currency, city_code, city, street, AssetClassificationID_text, coordinates, ad_date, date_added, no_of_rooms, floor_no, size_in_sm, price_per_SM)
        VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',
                '{}', '{}', '{}', '{}','{}','{}'); """

    mysql_cursor = mysql_conn.cursor()
    sql = insert_statement.format(event["current_ts"], event["record_id"], event["ad_number"], event["price"], event["currency"], event["city_code"], event["city"], event["street"], event["AssetClassificationID_text"], event["coordinates"], event["date"], event["date_added"], event["rooms"], event["floor"], event["SquareMeter"], event["price_per_SM"])
    mysql_conn.commit()
    mysql_cursor.execute(sql)
    mysql_cursor.close()
    print('add row')
    pass


class InvalidCityCode(Exception):
    pass


def _validate_process_df_event(event):
    print(f'Validating procss_df event: {event}')
    print(f'event["city_code"]: {event["city_code"]}')
    try:
        int(event["city_code"])
    except ValueError:
        raise InvalidCityCode(event["city_code"])


def procss_df(events):
    # connector to mysql
    mysql_conn = mc.connect(
        user=mysql_config_user,
        password=mysql_config_password,
        host=mysql_config_host,
        port=mysql_config_port,
        autocommit=True
    )

    try:
        _validate_process_df_event(events)
    except Exception as e:
        print(f'Validation error: {e}')
        return


    insert_statement = """
    INSERT INTO yad2.df(city_code, avg_SquareMeter, avg_price_per_SM, count_city)
        VALUES ('{}', '{}', '{}', '{}'); """
    update_statement = """
        UPDATE yad2.df set avg_SquareMeter= {}, avg_price_per_SM= {}, count_city= {} where city_code= {}
        """

    mysql_cursor = mysql_conn.cursor()
    sql_update = update_statement.format(events["avg_SquareMeter"], events["avg_price_per_SM"], events["count_city"], events["city_code"])
    sql_insert = insert_statement.format(events["city_code"], events["avg_SquareMeter"], events["avg_price_per_SM"], events["count_city"])
    mysql_conn.commit()
    try:
        mysql_cursor.execute(sql_insert)
        mysql_cursor.close()
        print('insert df')
    except:
        mysql_cursor.execute(sql_update)
        mysql_cursor.close()
        print('update df')



Insert_To_MYSQL_DB=df_kafka\
    .writeStream\
    .foreach(procss_row)\
    .outputMode("append") \
    .start()

df_agg = df_CityAvgPrice\
    .writeStream \
    .foreach(procss_df)\
    .outputMode("complete") \
    .start()


spark.streams.awaitAnyTermination()