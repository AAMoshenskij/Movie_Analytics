#!/usr/bin/env python3
"""
Простой оркестратор для управления ETL пайплайном
Заменитель Airflow для нашего проекта
"""
import schedule
import time
import subprocess
import threading
from datetime import datetime
import psycopg2
import json

class MovieAnalyticsOrchestrator:
    def __init__(self):
        self.spark_process = None
        self.is_running = False
        
    def start_spark_streaming(self):
        """Запускает Spark Streaming в отдельном потоке"""
        if self.spark_process and self.spark_process.poll() is None:
            print("⚠️ Spark Streaming уже запущен")
            return
            
        print("🚀 Запуск Spark Streaming...")
        self.spark_process = subprocess.Popen([
            'python3', 'scripts/spark_streaming_postgres_robust.py'
        ])
        
    def stop_spark_streaming(self):
        """Останавливает Spark Streaming"""
        if self.spark_process:
            print("🛑 Остановка Spark Streaming...")
            self.spark_process.terminate()
            self.spark_process.wait()
            print("✅ Spark Streaming остановлен")
            
    def generate_test_data(self):
        """Генерирует тестовые данные"""
        print("🎲 Генерация тестовых данных...")
        try:
            result = subprocess.run([
                'python3', 'scripts/kafka_producer_fixed.py'
            ], capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                print("✅ Тестовые данные сгенерированы")
            else:
                print(f"❌ Ошибка генерации: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("⏰ Таймаут генерации данных")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            
    def run_etl_pipeline(self):
        """Запускает полный ETL пайплайн"""
        print(f"🔧 [{datetime.now()}] Запуск ETL пайплайна...")
        
        try:
            # 1. Запускаем Spark Streaming
            self.start_spark_streaming()
            time.sleep(10)  # Ждем запуска
            
            # 2. Генерируем данные
            self.generate_test_data()
            
            # 3. Ждем обработки
            time.sleep(30)
            
            # 4. Проверяем результат
            self.check_data_quality()
            
            print(f"✅ [{datetime.now()}] ETL пайплайн завершен")
            
        except Exception as e:
            print(f"❌ [{datetime.now()}] Ошибка ETL: {e}")
            
    def check_data_quality(self):
        """Проверяет качество данных"""
        print("🔍 Проверка качества данных...")
        
        try:
            conn = psycopg2.connect(
                host="localhost", port=5432,
                database="analytics", user="admin", password="password"
            )
            
            # Проверяем количество записей
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM user_views_processed")
            count = cur.fetchone()[0]
            
            # Проверяем последние записи
            cur.execute("""
                SELECT COUNT(*), AVG(duration_seconds), COUNT(DISTINCT user_id)
                FROM user_views_processed 
                WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
            """)
            stats = cur.fetchone()
            
            print(f"📊 Статистика данных:")
            print(f"   - Всего записей: {count}")
            print(f"   - За последний час: {stats[0]}")
            print(f"   - Средняя длительность: {stats[1]:.2f} сек")
            print(f"   - Уникальные пользователи: {stats[2]}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка проверки данных: {e}")
            
    def generate_daily_report(self):
        """Генерирует ежедневный отчет"""
        print(f"📈 [{datetime.now()}] Генерация ежедневного отчета...")
        
        try:
            conn = psycopg2.connect(
                host="localhost", port=5432,
                database="analytics", user="admin", password="password"
            )
            
            # Отчет по популярным фильмам
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    movie_id,
                    COUNT(*) as view_count,
                    AVG(duration_seconds) as avg_duration,
                    COUNT(DISTINCT user_id) as unique_users
                FROM user_views_processed 
                WHERE processing_timestamp >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY movie_id
                ORDER BY view_count DESC
                LIMIT 10
            """)
            
            popular_movies = cur.fetchall()
            
            print("🎬 Топ-10 популярных фильмов за день:")
            for i, (movie_id, views, avg_dur, unique_users) in enumerate(popular_movies, 1):
                print(f"   {i}. {movie_id}: {views} просмотров, {avg_dur:.1f} сек в среднем, {unique_users} уникальных пользователей")
            
            # Сохраняем отчет в файл
            report = {
                "generated_at": datetime.now().isoformat(),
                "popular_movies": [
                    {"movie_id": m[0], "views": m[1], "avg_duration": m[2], "unique_users": m[3]}
                    for m in popular_movies
                ]
            }
            
            with open('reports/daily_report.json', 'w') as f:
                json.dump(report, f, indent=2)
                
            print("✅ Ежедневный отчет сохранен в reports/daily_report.json")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка генерации отчета: {e}")
            
    def health_check(self):
        """Проверяет здоровье системы"""
        print(f"❤️  [{datetime.now()}] Проверка здоровья системы...")
        
        services_ok = 0
        total_services = 3
        
        # Проверяем PostgreSQL
        try:
            conn = psycopg2.connect(
                host="localhost", port=5432,
                database="analytics", user="admin", password="password"
            )
            conn.close()
            print("   ✅ PostgreSQL: OK")
            services_ok += 1
        except:
            print("   ❌ PostgreSQL: ERROR")
            
        # Проверяем Kafka (упрощенно)
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            if result == 0:
                print("   ✅ Kafka: OK")
                services_ok += 1
            else:
                print("   ❌ Kafka: ERROR")
        except:
            print("   ❌ Kafka: ERROR")
            
        # Проверяем Spark (упрощенно)
        try:
            import requests
            response = requests.get("http://localhost:8080", timeout=5)
            if response.status_code == 200:
                print("   ✅ Spark: OK")
                services_ok += 1
            else:
                print("   ❌ Spark: ERROR")
        except:
            print("   ❌ Spark: ERROR")
            
        print(f"   📊 Статус: {services_ok}/{total_services} сервисов работают")
        
    def start(self):
        """Запускает оркестратор"""
        print("🎯 Запуск оркестратора Movie Analytics...")
        print("=" * 50)
        
        # Создаем папку для отчетов
        subprocess.run(['mkdir', '-p', 'reports'])
        
        # Настраиваем расписание
        schedule.every(2).hours.do(self.run_etl_pipeline)
        schedule.every().day.at("23:00").do(self.generate_daily_report)
        schedule.every(30).minutes.do(self.health_check)
        
        print("📅 Расписание настроено:")
        print("   - ETL пайплайн: каждые 2 часа")
        print("   - Ежедневный отчет: каждый день в 23:00")
        print("   - Проверка здоровья: каждые 30 минут")
        print("=" * 50)
        
        # Запускаем первую проверку
        self.health_check()
        
        self.is_running = True
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n🛑 Остановка оркестратора...")
            self.stop_spark_streaming()
            
    def stop(self):
        """Останавливает оркестратор"""
        self.is_running = False
        self.stop_spark_streaming()

if __name__ == "__main__":
    orchestrator = MovieAnalyticsOrchestrator()
    orchestrator.start()