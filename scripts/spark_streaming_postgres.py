#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    """Создает локальную Spark сессию"""
    return SparkSession.builder \
        .appName("MovieAnalyticsStreamingPostgres") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def create_kafka_stream(spark):
    """Создает поток данных из Kafka"""
    
    print("📡 Подключаемся к Kafka...")
    
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("device", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_views_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Подключение к Kafka установлено")
    
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df

def process_streaming_data(stream_df):
    """Обрабатывает потоковые данные"""
    print("🔄 Обрабатываем данные...")
    
    processed_df = stream_df.withColumn(
        "watch_category",
        when(col("duration_seconds") < 300, "short")
        .when((col("duration_seconds") >= 300) & (col("duration_seconds") < 1800), "medium")
        .otherwise("long")
    ).withColumn(
        "processing_timestamp", current_timestamp()
    ).withColumn(
        "original_timestamp", to_timestamp(col("timestamp"))
    )
    
    return processed_df

def write_processed_to_postgres(batch_df, batch_id):
    """Записывает обработанные данные в PostgreSQL"""
    if batch_df.count() > 0:
        print(f"💾 Записываем {batch_df.count()} записей в user_views_processed...")
        
        batch_df.select(
            "user_id", "movie_id", "duration_seconds", "event_type",
            "device", "original_timestamp", "watch_category", "processing_timestamp"
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/analytics") \
            .option("dbtable", "user_views_processed") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"✅ Батч {batch_id} записан в PostgreSQL")

def write_movie_stats_to_postgres(batch_df, batch_id):
    """Записывает статистику по фильмам в PostgreSQL"""
    if batch_df.count() > 0:
        print(f"📊 Записываем {batch_df.count()} агрегаций movie_stats...")
        
        batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "movie_id", "view_count", "total_watch_time", "avg_watch_time"
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/analytics") \
            .option("dbtable", "movie_stats_realtime") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

def write_device_stats_to_postgres(batch_df, batch_id):
    """Записывает статистику по устройствам в PostgreSQL"""
    if batch_df.count() > 0:
        print(f"📱 Записываем {batch_df.count()} агрегаций device_stats...")
        
        batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device", "view_count", "avg_watch_time"
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/analytics") \
            .option("dbtable", "device_stats_realtime") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

def create_movie_aggregations(stream_df):
    """Создает агрегации по фильмам"""
    return stream_df \
        .withWatermark("processing_timestamp", "1 minute") \
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("movie_id")
        ) \
        .agg(
            count("user_id").alias("view_count"),
            sum("duration_seconds").alias("total_watch_time"),
            avg("duration_seconds").alias("avg_watch_time")
        )

def create_device_aggregations(stream_df):
    """Создает агрегации по устройствам"""
    return stream_df \
        .withWatermark("processing_timestamp", "1 minute") \
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("device")
        ) \
        .agg(
            count("user_id").alias("view_count"),
            avg("duration_seconds").alias("avg_watch_time")
        )

def main():
    """Основная функция Spark Streaming с записью в PostgreSQL"""
    print("🚀 Запуск Spark Streaming ETL с записью в PostgreSQL...")
    print("   PostgreSQL: localhost:5432/analytics")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Создаем поток из Kafka
        kafka_stream = create_kafka_stream(spark)
        
        # Обрабатываем данные
        processed_stream = process_streaming_data(kafka_stream)
        
        # Создаем агрегации
        movie_aggregations = create_movie_aggregations(processed_stream)
        device_aggregations = create_device_aggregations(processed_stream)
        
        print("✅ Все готово! Запускаем стриминг...")
        print("   Данные будут записываться в PostgreSQL в реальном времени")
        print("   Нажмите Ctrl+C для остановки")
        print("-" * 50)
        
        # Записываем обработанные данные в PostgreSQL
        processed_query = processed_stream.writeStream \
            .foreachBatch(write_processed_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint-processed") \
            .start()
        
        # Записываем агрегации фильмов в PostgreSQL
        movie_query = movie_aggregations.writeStream \
            .foreachBatch(write_movie_stats_to_postgres) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint-movies") \
            .start()
        
        # Записываем агрегации устройств в PostgreSQL
        device_query = device_aggregations.writeStream \
            .foreachBatch(write_device_stats_to_postgres) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint-devices") \
            .start()
        
        # Ждем завершения
        processed_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⏹️ Останавливаем Spark Streaming...")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("🔚 Spark сессия остановлена")

if __name__ == "__main__":
    main()