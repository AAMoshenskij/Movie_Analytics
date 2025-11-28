# scripts/simple_etl.py
import psycopg2
import json
import pandas as pd
from datetime import datetime
import sys
import os

def connect_to_postgres():
    """Подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="analytics",
            user="admin",
            password="password"
        )
        print("✅ Успешно подключились к PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return None

def create_tables(conn):
    """Создание таблиц если они не существуют"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_views (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50),
                movie_id VARCHAR(50),
                duration_seconds INTEGER,
                event_type VARCHAR(20),
                event_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_stats (
                movie_id VARCHAR(50) PRIMARY KEY,
                total_views INTEGER DEFAULT 0,
                total_watch_time INTEGER DEFAULT 0,
                unique_users INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        print("✅ Таблицы созданы/проверены")
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка создания таблиц: {e}")
        conn.rollback()

def load_data_to_postgres(conn):
    """Загрузка данных из JSON в PostgreSQL"""
    try:
        # Проверяем существование файла
        if not os.path.exists('sample_data.json'):
            print("❌ Файл sample_data.json не найден")
            return
        
        # Читаем данные
        with open('sample_data.json', 'r') as f:
            raw_data = [json.loads(line) for line in f.readlines()]
        
        df = pd.DataFrame(raw_data)
        print(f"📊 Прочитано {len(df)} записей из файла")
        
        # Преобразуем timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Загружаем в базу
        cursor = conn.cursor()
        inserted_count = 0
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO user_views (user_id, movie_id, duration_seconds, event_type, event_timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, (row['user_id'], row['movie_id'], row['duration'], row['event_type'], row['timestamp']))
                inserted_count += 1
            except Exception as e:
                print(f"⚠️ Ошибка вставки строки: {e}")
                continue
        
        conn.commit()
        cursor.close()
        print(f"✅ Успешно загружено {inserted_count} записей в PostgreSQL")
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        conn.rollback()
        return 0

def verify_data(conn):
    """Проверка загруженных данных"""
    try:
        cursor = conn.cursor()
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM user_views")
        total_records = cursor.fetchone()[0]
        
        # Проверяем уникальных пользователей и фильмы
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT movie_id) as unique_movies
            FROM user_views
        """)
        stats = cursor.fetchone()
        
        print(f"📊 Проверка данных:")
        print(f"   Всего записей: {total_records}")
        print(f"   Уникальных пользователей: {stats[0]}")
        print(f"   Уникальных фильмов: {stats[1]}")
        
        # Показываем последние 5 записей
        cursor.execute("""
            SELECT user_id, movie_id, event_type, event_timestamp 
            FROM user_views 
            ORDER BY event_timestamp DESC 
            LIMIT 5
        """)
        recent_records = cursor.fetchall()
        
        print(f"   Последние 5 записей:")
        for record in recent_records:
            print(f"     {record}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка проверки данных: {e}")

def main():
    """Основная функция"""
    print("🚀 Запуск ETL процесса...")
    
    # Подключаемся к PostgreSQL
    conn = connect_to_postgres()
    if not conn:
        return
    
    try:
        # Создаем таблицы
        create_tables(conn)
        
        # Загружаем данные
        load_data_to_postgres(conn)
        
        # Проверяем данные
        verify_data(conn)
        
    finally:
        # Закрываем соединение
        conn.close()
        print("🔚 Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main()