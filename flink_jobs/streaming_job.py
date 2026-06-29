import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from pyflink.table import DataTypes

# 1. Configuration Flink et chargement des connecteurs (Kafka + Iceberg + GCS)
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# On règle le conflit Java (Jar Hell) entre Flink et Iceberg
t_env.get_config().set("classloader.resolve-order", "parent-first")

#On force Iceberg à "valider" (commit) les données toutes les 10 secondes
t_env.get_config().set("execution.checkpointing.interval", "10000")

jar_paths = ";".join([
    "file:///opt/flink/flink_jobs/flink-sql-connector-kafka-3.1.0-1.18.jar",
    "file:///opt/flink/flink_jobs/iceberg-flink-runtime-1.18-1.5.0.jar",
    "file:///opt/flink/flink_jobs/gcs-connector-hadoop3-2.2.22-shaded.jar",
    "file:///opt/flink/flink_jobs/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar",
    "file:///opt/flink/flink_jobs/flink-sql-avro-confluent-registry-1.18.1.jar",
    "file:///opt/flink/flink_jobs/flink-json-1.18.1.jar"
]) 
t_env.get_config().set("pipeline.jars", jar_paths)

# NOTRE MODÈLE DE ML EMBARQUÉ (UDF)
@udf(result_type=DataTypes.DOUBLE())
def predict_fraud_score(amount: float, tx_count_str: str, city_count_str: str) -> float:
    """
    Simule l'inférence en mémoire d'un modèle XGBoost/Logistique.
    Calcule un score de probabilité de fraude basé sur le profil de la fenêtre.
    """
    try:
        # Extraction des valeurs numériques calculées par les fenêtres Flink
        tx_count = float(tx_count_str.split()[0]) if tx_count_str else 1.0
        city_count = float(city_count_str.split()[0]) if city_count_str else 1.0
        
        # Logique d'activation du modèle (Poids statistiques)
        score = 0.1
        if amount > 3000.0: score += 0.4
        if tx_count >= 2: score += 0.2
        if city_count >= 2: score += 0.3
        
        return min(score, 1.0)
    except:
        return 0.0

# Enregistrement officiel de la fonction ML dans le moteur SQL de Flink
t_env.create_temporary_system_function("predict_fraud_score", predict_fraud_score)

# 2. La Source : Ajout du Moteur Temporel (Watermark)
source_ddl = """
    CREATE TABLE transactions (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        `timestamp` BIGINT,
        merchant STRING,
        location STRING,
        
        -- NOUVEAU : On convertit le timestamp BIGINT en vrai format Date/Heure pour Flink
        event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
        
        -- NOUVEAU : On dit à Flink d'utiliser cette colonne pour gérer le temps, 
        -- et on tolère un retard réseau maximum de 5 secondes (Watermark)
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions-avro',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'fraud-detector-stateful',
        'format' = 'avro-confluent',
        'avro-confluent.url' = 'http://schema-registry:8081',
        'scan.startup.mode' = 'latest-offset'
    )
"""
t_env.execute_sql(source_ddl)

# 3. Sinks : Data Lake (Iceberg) avec ajout de la colonne fraud_score
iceberg_ddl = """
    CREATE TABLE fraud_alerts_v2 (
        user_id STRING,
        amount DOUBLE,
        merchant STRING,
        location STRING,
        fraud_score DOUBLE
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'gcp_catalog',
        'catalog-type' = 'hadoop',
        'warehouse' = 'gs://datalake-fraude-streaming-2026/warehouse'
    )
"""
t_env.execute_sql(iceberg_ddl)

# 4. Sink : Urgent Alerts (Kafka JSON) avec ajout de la probabilité de l'IA
alerts_urgent_ddl = """
    CREATE TABLE alerts_urgent (
        user_id STRING,
        amount DOUBLE,
        alert_reason STRING,
        confidence_score DOUBLE -- Nouvelle colonne pour les microservices
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'alerts-urgent',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json'
    )
"""
t_env.execute_sql(alerts_urgent_ddl)

# 5. Sink : La DLQ
dlq_ddl = """
    CREATE TABLE transactions_dlq (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        `timestamp` BIGINT,
        merchant STRING,
        location STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions-dlq',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json' 
    )
"""
t_env.execute_sql(dlq_ddl)

# 6. Graphe d'Exécution Multi-Sink enrichi à l'IA
print("🚀 Lancement du Pipeline Flink : Inférence ML en cours d'exécution...")
statement_set = t_env.create_statement_set()

# On crée une vue temporaire intermédiaire contenant nos fenêtres glissantes
t_env.create_temporary_view("windowed_profiles", t_env.sql_query("""
    SELECT 
        user_id, 
        SUM(amount) as amount, 
        CAST(COUNT(transaction_id) AS STRING) || ' transactions' as tx_metrics,
        CAST(COUNT(DISTINCT location) AS STRING) || ' villes' as city_metrics
    FROM TABLE(
        HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '30' SECOND)
    )
    GROUP BY user_id, window_start, window_end
"""))

# Branche A : Écriture analytique (Iceberg) avec le score calculé en ligne par l'UDF ML
statement_set.add_insert_sql("""
    INSERT INTO fraud_alerts_v2
    SELECT user_id, amount, tx_metrics, city_metrics, predict_fraud_score(amount, tx_metrics, city_metrics)
    FROM windowed_profiles
    WHERE amount > 2000.0
""")

# Branche B : Notification instantanée (Kafka Alerts) avec indice de confiance ML
statement_set.add_insert_sql("""
    INSERT INTO alerts_urgent
    SELECT 
        user_id, 
        amount, 
        'ALERTE IA : Suspicion de fraude industrielle élevée',
        predict_fraud_score(amount, tx_metrics, city_metrics)
    FROM windowed_profiles
    WHERE amount > 2000.0
""")

# Branche C : Isolement dans la DLQ (Inchangé)
statement_set.add_insert_sql("""
    INSERT INTO transactions_dlq
    SELECT transaction_id, user_id, amount, `timestamp`, merchant, location
    FROM transactions 
    WHERE amount < 0.0
""")

statement_set.execute()