from confluent_kafka import Consumer, KafkaError
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'notification-service-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['alerts-urgent'])

if __name__ == '__main__':
    print("🚨 Microservice de Notification activé. Écoute du topic 'alerts-urgent'...\n" + "="*60)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Si c'est juste la fin de la partition, on ignore
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # Si le topic n'existe pas ENCORE, on patiente en silence
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                else:
                    print(f"⚠️ Avertissement Kafka: {msg.error()}")
                    continue # On ne s'arrête plus !
            
            # Réception de l'alerte temps réel calculée par Flink
            alert_data = json.loads(msg.value().decode('utf_8'))
            print(f"📱 [SMS SENT TO CLIENT] -> Client: {alert_data['user_id']} | Montant cumulé: {alert_data['amount']}$")
            print(f"💬 Détail : {alert_data['alert_reason']}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()