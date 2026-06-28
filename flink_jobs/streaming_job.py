import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Configuration de l'environnement de Streaming Flink
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Déclaration du connecteur Kafka (le fichier .jar)
jar_path = "file:///opt/flink/flink_jobs/flink-sql-connector-kafka-3.1.0-1.18.jar"
t_env.get_config().set("pipeline.jars", jar_path)

# 2. Connexion à la source (Le topic Kafka)
# Note : Flink utilise "kafka:29092" pour parler au broker via le réseau interne de Docker
source_ddl = """
    CREATE TABLE transactions (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        `timestamp` BIGINT,
        merchant STRING,
        location STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'fraud-detector-group',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
"""
t_env.execute_sql(source_ddl)

# 3. La logique métier (Détection de fraude)
print("🔎 Démarrage du moteur Flink : Écoute du flux Kafka en cours...")
query = """
    SELECT 
        user_id, 
        amount, 
        merchant, 
        location
    FROM transactions 
    WHERE amount > 1000.0
"""

# 4. Exécution (Affiche le résultat en direct dans la console)
t_env.execute_sql(query).print()