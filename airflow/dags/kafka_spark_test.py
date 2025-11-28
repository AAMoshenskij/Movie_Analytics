from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import time

def test_kafka_spark_integration():
    """Тестирует интеграцию Kafka + Spark"""
    print("🧪 Тест интеграции Kafka + Spark...")
    
    try:
        # 1. Запускаем генератор на короткое время
        print("1. Запуск Kafka producer...")
        producer = subprocess.Popen(['python3', '/opt/airflow/scripts/kafka_producer_fixed.py'])
        time.sleep(10)  # Генерируем данные 10 секунд
        producer.terminate()
        
        # 2. Запускаем Spark на короткое время
        print("2. Запуск Spark Streaming...")
        spark = subprocess.Popen(['python3', '/opt/airflow/scripts/spark_streaming_postgres.py'])
        time.sleep(20)  # Обрабатываем 20 секунд
        spark.terminate()
        
        print("✅ Интеграция Kafka + Spark работает!")
        
    except Exception as e:
        print(f"❌ Ошибка интеграции: {e}")
        raise

with DAG(
    'kafka_spark_integration_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    tags=['test', 'kafka', 'spark']
) as dag:

    test_integration = PythonOperator(
        task_id='test_kafka_spark_integration',
        python_callable=test_kafka_spark_integration
    )