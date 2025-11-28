from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_database_connection():
    """Проверяет подключение к базе данных"""
    print("🔍 Проверка подключения к базе данных...")
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Простой запрос для проверки
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        print(f"✅ Подключение успешно: {result}")
        
        # Проверим таблицы
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        print("📊 Доступные таблицы:")
        for table in tables:
            print(f"   - {table[0]}")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        raise

def generate_test_data():
    """Генерирует тестовые данные"""
    print("🎲 Генерация тестовых данных...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Добавляем тестовые данные
        from datetime import datetime
        test_data = [
            ('test_user_001', 'movie_001', 300, 'start', datetime.now()),
            ('test_user_002', 'movie_002', 450, 'pause', datetime.now()),
            ('test_user_001', 'movie_001', 600, 'stop', datetime.now()),
        ]
        
        for data in test_data:
            cursor.execute("""
                INSERT INTO user_views (user_id, movie_id, duration_seconds, event_type, event_timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, data)
        
        conn.commit()
        print("✅ Тестовые данные добавлены")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка генерации данных: {e}")
        raise

def calculate_simple_metrics():
    """Рассчитывает простые метрики"""
    print("📊 Расчет метрик...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Создаем таблицу для метрик
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airflow_metrics (
                id SERIAL PRIMARY KEY,
                metric_name VARCHAR(100),
                metric_value FLOAT,
                calculated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Рассчитываем базовые метрики
        cursor.execute("SELECT COUNT(*) FROM user_views")
        total_views = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_views")
        unique_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(duration_seconds) FROM user_views")
        avg_duration = cursor.fetchone()[0] or 0
        
        # Сохраняем метрики
        metrics = [
            ('total_views', total_views),
            ('unique_users', unique_users),
            ('avg_duration', avg_duration)
        ]
        
        for name, value in metrics:
            cursor.execute("""
                INSERT INTO airflow_metrics (metric_name, metric_value)
                VALUES (%s, %s)
            """, (name, value))
        
        conn.commit()
        
        print(f"✅ Метрики рассчитаны:")
        print(f"   - Всего просмотров: {total_views}")
        print(f"   - Уникальных пользователей: {unique_users}")
        print(f"   - Средняя длительность: {avg_duration:.2f} сек")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка расчета метрик: {e}")
        raise

# Создаем DAG
with DAG(
    'movie_analytics_simple',
    default_args=default_args,
    description='Простой DAG для аналитики фильмов',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['movie', 'analytics', 'simple']
) as dag:

    check_connection = PythonOperator(
        task_id='check_database_connection',
        python_callable=check_database_connection
    )

    generate_data = PythonOperator(
        task_id='generate_test_data',
        python_callable=generate_test_data
    )

    calculate_metrics = PythonOperator(
        task_id='calculate_simple_metrics',
        python_callable=calculate_simple_metrics
    )

    # Определяем порядок выполнения
    check_connection >> generate_data >> calculate_metrics