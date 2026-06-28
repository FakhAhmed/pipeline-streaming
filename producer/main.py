import os
import time
import random
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

fake = Faker()

# 1. Configuration du Schema Registry Client
schema_registry_conf = {'url': 'http://localhost:8082'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Chargement du fichier de schéma Avro
path_to_avsc = os.path.join(os.path.dirname(__file__), 'transaction.avsc')
with open(path_to_avsc, 'r') as f:
    schema_str = f.read()

# 3. Configuration de l'Avro Serializer pour la valeur du message
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    to_dict=lambda obj, ctx: obj
)

# 4. Configuration du Producer Kafka
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'transaction-producer-avro'
}
producer = Producer(producer_conf)
topic = 'transactions-avro' 

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Erreur d'envoi Avro : {err}")
    else:
        print(f"✅ Envoyé (Avro validé) [Topic: {msg.topic()} | Partition: {msg.partition()}]")

def generate_transaction():
    user_id = random.randint(100, 150) 
    is_fraudulent = random.random() < 0.05 
    amount = round(random.uniform(1000.0, 5000.0), 2) if is_fraudulent else round(random.uniform(5.0, 100.0), 2)
    
    return {
        "transaction_id": fake.uuid4(),
        "user_id": f"user_{user_id}",
        "amount": float(amount), 
        "timestamp": int(time.time() * 1000),
        "merchant": fake.company(),
        "location": fake.city()
    }

if __name__ == "__main__":
    print(f"🚀 Démarrage de la banque AVRO... Enregistrement du schéma sur http://localhost:8081")
    print(f"Injection dans le topic sécurisé '{topic}'\n" + "-"*50)
    
    try:
        while True:
            transaction = generate_transaction()
            
            # Utilisation de l'AvroSerializer avec le bon Contexte (topic + type de champ)
            producer.produce(
                topic=topic,
                key=StringSerializer('utf_8')(transaction["user_id"]),
                value=avro_serializer(transaction, SerializationContext(topic, MessageField.VALUE)),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(0.1, 0.8))
            
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du flux Avro.")
    finally:
        producer.flush()