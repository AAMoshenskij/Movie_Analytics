#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    """Создает локальную Spark сессию"""
    return SparkSession.builder \
        .appName("MovieAnalyticsStreaming") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def create_kafka_stream(spark):
    """Создает поток данных из Kafka"""
    
    print("📡 Подключаемся к Kafka...")
    
    # Схема для наших данных
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("device", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # Чтение потока из Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_views_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Подключение к Kafka установлено")
    
    # Парсим JSON и применяем схему
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df

def process_streaming_data(stream_df):
    """Обрабатывает потоковые данные"""
    print("🔄 Обрабатываем данные...")
    
    # Добавляем категории просмотров
    processed_df = stream_df.withColumn(
        "watch_category",
        when(col("duration_seconds") < 300, "short")
        .when((col("duration_seconds") >= 300) & (col("duration_seconds") < 1800), "medium")
        .otherwise("long")
    )
    
    # Добавляем время обработки
    processed_df = processed_df.withColumn(
        "processing_timestamp", current_timestamp()
    )
    
    return processed_df

def write_to_console(stream_df):
    """Записывает данные в консоль"""
    print("🖥️ Запускаем вывод в консоль...")
    
    return stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()

def create_simple_aggregations(stream_df):
    """Создает простые агрегации (без distinct)"""
    
    # Агрегация по фильмам - только поддерживаемые операции
    movie_stats = stream_df \
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
    
    return movie_stats

def create_device_aggregations(stream_df):
    """Создает агрегации по устройствам"""
    
    device_stats = stream_df \
        .withWatermark("processing_timestamp", "1 minute") \
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("device")
        ) \
        .agg(
            count("user_id").alias("view_count"),
            avg("duration_seconds").alias("avg_watch_time")
        )
    
    return device_stats

def write_aggregations_to_console(agg_df, name):
    """Записывает агрегации в консоль"""
    return agg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .option("checkpointLocation", f"/tmp/checkpoint-{name}") \
        .start()

def main():
    """Основная функция Spark Streaming"""
    print("🚀 Запуск Spark Streaming ETL (исправленная версия)...")
    print("   Режим: local[2]")
    print("   Kafka: localhost:9092")
    
    # Создаем Spark сессию
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Уменьшаем логи
    
    try:
        # Создаем поток из Kafka
        kafka_stream = create_kafka_stream(spark)
        
        # Обрабатываем данные
        processed_stream = process_streaming_data(kafka_stream)
        
        # Создаем агрегации
        movie_aggregations = create_simple_aggregations(processed_stream)
        device_aggregations = create_device_aggregations(processed_stream)
        
        print("✅ Все готово! Ожидаем данные из Kafka...")
        print("   Запустите producer: python3 kafka_producer_fixed.py")
        print("   Нажмите Ctrl+C для остановки")
        print("-" * 50)
        
        # Запускаем стриминг
        console_query = write_to_console(processed_stream)
        movie_query = write_aggregations_to_console(movie_aggregations, "movies")
        device_query = write_aggregations_to_console(device_aggregations, "devices")
        
        # Ждем завершения
        console_query.awaitTermination()
        
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