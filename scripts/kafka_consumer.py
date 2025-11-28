from kafka import KafkaConsumer
import json

def create_kafka_consumer():
    return KafkaConsumer(
        'user_views_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consume_messages_simple():
    """Простой consumer для тестирования"""
    consumer = create_kafka_consumer()
    print("👂 Kafka Consumer запущен. Ожидание сообщений...")
    print("   Нажмите Ctrl+C для остановки")
    
    try:
        for message in consumer:
            print(f"📥 Получено: {message.value['user_id']} - {message.value['event_type']} - {message.value['movie_id']}")
            
    except KeyboardInterrupt:
        print("\n⏹️ Остановка консьюмера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages_simple()