from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import subprocess
import time
import requests

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def check_services_health():
    """Проверяет доступность всех сервисов"""
    print("🔍 Проверка здоровья сервисов...")
    
    services = {
        'Kafka': ('kafka', 9092),  # Используем имя сервиса вместо localhost
        'PostgreSQL': ('postgres', 5432),
        'Spark Master': ('spark-master', 8080),
        'Zookeeper': ('zookeeper', 2181)
    }
    
    for service, (host, port) in services.items():
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # Увеличиваем таймаут
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"✅ {service} доступен на {host}:{port}")
            else:
                print(f"❌ {service} недоступен на {host}:{port}")
                # Не падаем сразу, продолжаем проверку других сервисов
                if service == 'Kafka':
                    raise Exception(f"{service} недоступен")
                
        except Exception as e:
            print(f"⚠️ Ошибка проверки {service}: {e}")
            if service == 'Kafka':
                raise


def start_kafka_producer():
    """Запускает Kafka producer для генерации данных"""
    print("🚀 Запуск Kafka producer...")
    
    try:
        # Запускаем ваш генератор данных
        process = subprocess.Popen([
            'python3', '/opt/airflow/scripts/kafka_producer_fixed.py'
        ])
        
        # Даем время на генерацию данных
        print("⏳ Генерация данных в Kafka...")
        time.sleep(30)  # Ждем 30 секунд
        
        # Завершаем процесс
        process.terminate()
        process.wait()
        
        print("✅ Данные сгенерированы в Kafka")
        
    except Exception as e:
        print(f"❌ Ошибка запуска producer: {e}")
        raise

def start_spark_streaming():
    """Запускает Spark Streaming для обработки данных из Kafka"""
    print("🔥 Запуск Spark Streaming...")
    
    try:
        # Проверяем, не запущен ли уже Spark
        try:
            with open('/tmp/spark_streaming.pid', 'r') as f:
                old_pid = int(f.read().strip())
                import os
                try:
                    os.kill(old_pid, 0)  # Проверяем существование процесса
                    print(f"⚠️ Spark уже запущен с PID {old_pid}, перезапускаем...")
                    os.kill(old_pid, 9)  # Принудительно завершаем
                    time.sleep(5)
                except OSError:
                    pass  # Процесса нет, продолжаем
        except FileNotFoundError:
            pass
        
        # Запускаем Spark Streaming в фоновом режиме
        spark_process = subprocess.Popen([
            'python3', '/opt/airflow/scripts/spark_streaming_postgres.py'
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Сохраняем PID для последующего завершения
        with open('/tmp/spark_streaming.pid', 'w') as f:
            f.write(str(spark_process.pid))
        
        # Даем время на инициализацию
        print("⏳ Ожидание инициализации Spark...")
        time.sleep(30)
        
        # Проверяем, что процесс еще жив
        if spark_process.poll() is not None:
            stdout, stderr = spark_process.communicate()
            print(f"❌ Spark процесс завершился с кодом {spark_process.returncode}")
            print(f"STDERR: {stderr.decode()}")
            raise Exception("Spark процесс не запустился")
        
        print("✅ Spark Streaming запущен и обрабатывает данные")
        
    except Exception as e:
        print(f"❌ Ошибка запуска Spark: {e}")
        raise


def stop_spark_streaming():
    """Останавливает Spark Streaming"""
    print("🛑 Остановка Spark Streaming...")
    
    try:
        # Читаем PID и останавливаем процесс
        with open('/tmp/spark_streaming.pid', 'r') as f:
            pid = int(f.read().strip())
        
        import os
        import signal
        os.kill(pid, signal.SIGTERM)
        
        print("✅ Spark Streaming остановлен")
        
    except Exception as e:
        print(f"⚠️ Не удалось остановить Spark: {e}")

def verify_processed_data():
    """Проверяет, что данные обработаны и загружены в PostgreSQL"""
    print("📊 Проверка обработанных данных...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Проверяем данные в основных таблицах
        tables_to_check = ['user_views_processed', 'movie_stats_realtime', 'device_stats_realtime']
        
        for table in tables_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"📈 Таблица {table}: {count} записей")
            
            if count == 0:
                raise Exception(f"Таблица {table} пуста!")
        
        # Проверяем свежесть данных
        cursor.execute("""
            SELECT MAX(processing_timestamp) as latest_data 
            FROM user_views_processed
        """)
        latest_timestamp = cursor.fetchone()[0]
        print(f"🕒 Последние данные: {latest_timestamp}")
        
        cursor.close()
        conn.close()
        
        print("✅ Данные успешно обработаны и загружены")
        
    except Exception as e:
        print(f"❌ Ошибка проверки данных: {e}")
        raise

def update_business_metrics():
    """Обновляет бизнес-метрики на основе обработанных данных"""
    print("💼 Обновление бизнес-метрик...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Создаем таблицу для бизнес-метрик
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS business_metrics (
                metric_date DATE PRIMARY KEY,
                total_views INTEGER,
                unique_users INTEGER,
                total_watch_time INTEGER,
                popular_movie VARCHAR(100),
                avg_session_duration FLOAT,
                calculated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Рассчитываем метрики
        cursor.execute("""
            INSERT INTO business_metrics (
                metric_date, total_views, unique_users, total_watch_time, 
                popular_movie, avg_session_duration
            )
            SELECT 
                CURRENT_DATE as metric_date,
                COUNT(*) as total_views,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(duration_seconds) as total_watch_time,
                (SELECT movie_id FROM user_views_processed 
                 GROUP BY movie_id ORDER BY COUNT(*) DESC LIMIT 1) as popular_movie,
                AVG(duration_seconds) as avg_session_duration
            FROM user_views_processed 
            WHERE DATE(processing_timestamp) = CURRENT_DATE
            ON CONFLICT (metric_date) DO UPDATE SET
                total_views = EXCLUDED.total_views,
                unique_users = EXCLUDED.unique_users,
                total_watch_time = EXCLUDED.total_watch_time,
                popular_movie = EXCLUDED.popular_movie,
                avg_session_duration = EXCLUDED.avg_session_duration,
                calculated_at = NOW()
        """)
        
        conn.commit()
        
        # Показываем результаты
        cursor.execute("SELECT * FROM business_metrics ORDER BY metric_date DESC LIMIT 1")
        latest_metrics = cursor.fetchone()
        
        if latest_metrics:
            print("📈 Последние бизнес-метрики:")
            print(f"   - Дата: {latest_metrics[0]}")
            print(f"   - Просмотры: {latest_metrics[1]}")
            print(f"   - Уникальные пользователи: {latest_metrics[2]}")
            print(f"   - Общее время просмотра: {latest_metrics[3]} сек")
            print(f"   - Популярный фильм: {latest_metrics[4]}")
            print(f"   - Средняя длительность: {latest_metrics[5]:.2f} сек")
        
        cursor.close()
        conn.close()
        
        print("✅ Бизнес-метрики обновлены")
        
    except Exception as e:
        print(f"❌ Ошибка обновления метрик: {e}")
        raise

def cleanup_temp_data():
    """Очищает временные данные"""
    print("🧹 Очистка временных данных...")
    
    try:
        # Удаляем PID файл
        import os
        if os.path.exists('/tmp/spark_streaming.pid'):
            os.remove('/tmp/spark_streaming.pid')
        
        print("✅ Временные данные очищены")
        
    except Exception as e:
        print(f"⚠️ Ошибка очистки: {e}")

# Создаем основной DAG
with DAG(
    'full_movie_analytics_pipeline',
    default_args=default_args,
    description='Полный пайплайн аналитики фильмов с Kafka и Spark',
    schedule_interval=timedelta(hours=2),  # Запуск каждые 2 часа
    catchup=False,
    tags=['movie', 'kafka', 'spark', 'analytics', 'etl']
) as dag:

    health_check = PythonOperator(
        task_id='check_services_health',
        python_callable=check_services_health
    )

    kafka_producer = PythonOperator(
        task_id='start_kafka_producer',
        python_callable=start_kafka_producer
    )

    spark_streaming = PythonOperator(
        task_id='start_spark_streaming',
        python_callable=start_spark_streaming
    )

    verify_data = PythonOperator(
        task_id='verify_processed_data',
        python_callable=verify_processed_data
    )

    update_metrics = PythonOperator(
        task_id='update_business_metrics',
        python_callable=update_business_metrics
    )

    stop_spark = PythonOperator(
        task_id='stop_spark_streaming',
        python_callable=stop_spark_streaming,
        trigger_rule='all_done'  # Выполняется всегда, даже при ошибках
    )

    cleanup = PythonOperator(
        task_id='cleanup_temp_data',
        python_callable=cleanup_temp_data,
        trigger_rule='all_done'
    )

    # Определяем порядок выполнения
    health_check >> kafka_producer >> spark_streaming >> verify_data >> update_metrics
    spark_streaming >> stop_spark  # Параллельно с проверкой данных
    [update_metrics, stop_spark] >> cleanup