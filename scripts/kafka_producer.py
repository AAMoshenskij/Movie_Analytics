from kafka import KafkaProducer, KafkaAdminClient
import json
import time
import random
from datetime import datetime
import socket

def wait_for_kafka(max_retries=12, wait_interval=5):
    """Ждет пока Kafka станет доступной"""
    print("⏳ Ожидание доступности Kafka...")
    
    for i in range(max_retries):
        try:
            # Проверяем порт
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            
            if result == 0:
                # Порт доступен, проверяем Kafka
                admin_client = KafkaAdminClient(
                    bootstrap_servers=['localhost:9092'],
                    request_timeout_ms=5000
                )
                admin_client.list_topics()
                admin_client.close()
                
                print("✅ Kafka полностью доступна!")
                return True
            else:
                print(f"   Попытка {i+1}/{max_retries}: порт 9092 еще не доступен...")
                time.sleep(wait_interval)
                
        except Exception as e:
            print(f"   Попытка {i+1}/{max_retries}: Kafka еще не готова... ({e})")
            time.sleep(wait_interval)
    
    print("❌ Kafka не стала доступной за отведенное время")
    return False

def create_kafka_producer():
    """Создает Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            request_timeout_ms=10000,
            retries=3,
            acks='all'  # Ждем подтверждения от всех реплик
        )
        print("✅ Kafka Producer создан успешно!")
        return producer
    except Exception as e:
        print(f"❌ Ошибка создания Producer: {e}")
        return None

def generate_event():
    """Генерирует одно событие просмотра"""
    return {
        "user_id": f"user_{random.randint(1, 1000)}",
        "movie_id": f"movie_{random.randint(1, 500)}",
        "duration_seconds": random.randint(10, 7200),
        "event_type": random.choice(['start', 'pause', 'stop', 'resume']),
        "device": random.choice(['mobile', 'smart_tv', 'tablet', 'desktop']),
        "timestamp": datetime.now().isoformat()
    }

def send_streaming_data():
    """Отправляет поток событий в Kafka"""
    
    # Ждем пока Kafka станет доступной
    if not wait_for_kafka():
        return
    
    producer = create_kafka_producer()
    if not producer:
        return
        
    print("🚀 Kafka Producer запущен. Отправка данных в topic 'user_views_topic'...")
    
    try:
        message_count = 0
        while message_count < 10:  # Отправим 10 сообщений для теста
            event = generate_event()
            
            # Отправка в Kafka
            future = producer.send('user_views_topic', value=event)
            
            # Ждем подтверждения
            result = future.get(timeout=10)
            
            print(f"📨 Сообщение {message_count + 1}: "
                  f"Пользователь {event['user_id']} {event['event_type']} "
                  f"фильм {event['movie_id']} "
                  f"(partition: {result.partition}, offset: {result.offset})")
            
            message_count += 1
            time.sleep(2)  # Ждем 2 секунды между сообщениями
            
    except KeyboardInterrupt:
        print("\n⏹️ Остановка продюсера...")
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer закрыт")

if __name__ == "__main__":
    send_streaming_data()