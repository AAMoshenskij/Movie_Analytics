#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import hashlib
import uuid

def create_spark_session():
    return SparkSession.builder \
        .appName("AdvancedMovieAnalytics") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.5.0,"
                "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

def create_enhanced_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("device", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_region", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("bitrate", IntegerType(), True)
    ])

def generate_session_id(user_id, movie_id, timestamp):
    return hashlib.md5(f"{user_id}_{movie_id}_{timestamp}".encode()).hexdigest()

def process_with_sessionization(df):
    """Обогащение данных информацией о сессиях"""
    
    # Генерация session_id
    session_udf = udf(lambda u, m, t: generate_session_id(u, m, t), StringType())
    
    processed_df = df \
        .withColumn("original_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("session_id", session_udf(col("user_id"), col("movie_id"), col("timestamp"))) \
        .withColumn("event_date", to_date(col("original_timestamp"))) \
        .withColumn("watch_category",
            when(col("duration_seconds") < 300, "short")
            .when((col("duration_seconds") >= 300) & (col("duration_seconds") < 1800), "medium")
            .otherwise("long")) \
        .withColumn("is_valid", 
            (col("duration_seconds") > 0) & 
            (col("duration_seconds") <= 86400) &  # максимум 24 часа
            col("user_id").isNotNull() &
            col("movie_id").isNotNull())
    
    return processed_df

def calculate_user_metrics(df):
    """Расчет пользовательских метрик"""
    
    window_spec = Window.partitionBy("user_id").orderBy("original_timestamp")
    
    user_metrics = df \
        .withColumn("prev_event_time", lag("original_timestamp").over(window_spec)) \
        .withColumn("time_since_last_event", 
            when(col("prev_event_time").isNotNull(),
                 unix_timestamp(col("original_timestamp")) - unix_timestamp(col("prev_event_time")))
            .otherwise(lit(None))) \
        .withColumn("is_new_session",
            (col("time_since_last_event") > 3600) | col("prev_event_time").isNull()) \
        .withColumn("session_seq_number",
            sum(col("is_new_session").cast("integer")).over(
                Window.partitionBy("user_id").orderBy("original_timestamp").rowsBetween(Window.unboundedPreceding, 0)
            ))
    
    return user_metrics

def write_to_data_lake(batch_df, batch_id):
    """Запись в Data Lake (имитация S3)"""
    if batch_df.count() > 0:
        batch_df.write \
            .mode("append") \
            .parquet(f"data_lake/raw/user_views/batch_{batch_id}")

def write_to_clickhouse(batch_df, batch_id):
    """Запись в ClickHouse"""
    if batch_df.count() > 0:
        batch_df.select(
            "user_id", "movie_id", "duration_seconds", "event_type",
            "device", "watch_category", "event_date", 
            col("original_timestamp").alias("event_timestamp"),
            "session_id", col("is_valid").cast("integer").alias("is_valid")
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://localhost:8123/analytics") \
            .option("dbtable", "user_views_fact") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

def create_business_metrics(df):
    """Создание бизнес-метрик"""
    
    # Метрики удержания (упрощенно)
    retention_metrics = df \
        .filter(col("event_type") == "start") \
        .groupBy("user_id", "event_date") \
        .agg(count("*").alias("daily_starts")) \
        .groupBy("event_date") \
        .agg(
            count("*").alias("daily_active_users"),
            sum(when(col("daily_starts") > 1, 1).otherwise(0)).alias("retained_users")
        ) \
        .withColumn("retention_rate", 
            when(col("daily_active_users") > 0, 
                 col("retained_users") / col("daily_active_users") * 100)
            .otherwise(lit(0)))
    
    return retention_metrics

def main():
    print("🚀 Запуск расширенного Spark Streaming ETL...")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Чтение из Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user_views_topic") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Парсинг JSON
        schema = create_enhanced_schema()
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Обработка данных
        processed_df = process_with_sessionization(parsed_df)
        enriched_df = calculate_user_metrics(processed_df)
        
        # Агрегации для realtime дашбордов
        movie_agg = create_simple_aggregations(processed_df)
        device_agg = create_device_aggregations(processed_df)
        retention_agg = create_business_metrics(processed_df)
        
        print("✅ Все обработчики настроены. Запуск стриминга...")
        
        # Запись в различные цели
        query1 = processed_df.writeStream \
            .foreachBatch(write_to_data_lake) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint-datalake") \
            .start()
        
        query2 = processed_df.writeStream \
            .foreachBatch(write_to_clickhouse) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint-clickhouse") \
            .start()
        
        query1.awaitTermination()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()