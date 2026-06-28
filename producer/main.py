import json
import time
import random
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# 1. Configuration pour se connecter au Kafka local (via Docker)
conf = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'transaction-producer'
}
producer = Producer(conf)
topic = 'transactions'

def delivery_report(err, msg):
    """Fonction appelée automatiquement quand un message est (ou n'est pas) envoyé."""
    if err is not None:
        print(f"❌ Erreur d'envoi: {err}")
    else:
        # Affichage clair et satisfaisant dans le terminal
        print(f"✅ Envoyé : {msg.value().decode('utf-8')}")

def generate_transaction():
    """Génère une fausse transaction avec 5% de chance de fraude."""
    # On simule un petit groupe de 50 clients pour voir leurs habitudes
    user_id = random.randint(100, 150) 
    
    # 5% de chance de générer une fraude (montant entre 1000 et 5000)
    is_fraudulent = random.random() < 0.05 
    amount = round(random.uniform(1000.0, 5000.0), 2) if is_fraudulent else round(random.uniform(5.0, 100.0), 2)
    
    return {
        "transaction_id": fake.uuid4(),
        "user_id": f"user_{user_id}",
        "amount": amount,
        "timestamp": int(time.time() * 1000), # Timestamp UNIX en millisecondes
        "merchant": fake.company(),
        "location": fake.city()
    }

if __name__ == "__main__":
    print(f"🚀 Démarrage de la banque... Injection dans le topic Kafka '{topic}'")
    print("Appuie sur Ctrl+C pour arrêter le flux.\n" + "-"*50)
    
    try:
        while True:
            # 2. On génère la transaction
            transaction = generate_transaction()
            
            # 3. On l'envoie dans Kafka
            producer.produce(
                topic, 
                key=transaction["user_id"], 
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0) # Gère les callbacks de manière asynchrone
            
            # 4. Pause entre 0.1s et 0.8s pour simuler le trafic réel
            time.sleep(random.uniform(0.1, 0.8))
            
    except KeyboardInterrupt:
        print("\n🛑 Arrêt manuel demandé par l'utilisateur.")
    finally:
        print("⏳ Vidage du buffer Kafka en cours...")
        producer.flush()
        print("🔌 Déconnecté proprement.")