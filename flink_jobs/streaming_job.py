import os
from pyflink.table import EnvironmentSettings, TableEnvironment

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
    "file:///opt/flink/flink_jobs/flink-sql-avro-confluent-registry-1.18.1.jar"
]) 
t_env.get_config().set("pipeline.jars", jar_paths)

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

# 3. Destination 1 : Data Lake (GCP / Iceberg)
iceberg_ddl = """
    CREATE TABLE fraud_alerts (
        user_id STRING,
        amount DOUBLE,
        merchant STRING,
        location STRING
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'gcp_catalog',
        'catalog-type' = 'hadoop',
        'warehouse' = 'gs://datalake-fraude-streaming-2026/warehouse'
    )
"""
t_env.execute_sql(iceberg_ddl)

# 4. Destination 2 : La DLQ (Kafka)
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

# 5. Le Pipeline de Routage (Stateful Multi-Sink)
print("🚀 Lancement du Pipeline Flink : Fenêtres Temporelles + DLQ...")

statement_set = t_env.create_statement_set()

# Règle Métier Avancée : Fenêtre Glissante (HOP Window)
# On calcule la somme sur 30 secondes, et on fait avancer la fenêtre toutes les 5 secondes
statement_set.add_insert_sql("""
    INSERT INTO fraud_alerts
    SELECT 
        user_id, 
        SUM(amount) as amount, 
        CAST(COUNT(transaction_id) AS STRING) || ' transactions' as merchant,
        CAST(COUNT(DISTINCT location) AS STRING) || ' villes' as location
    FROM TABLE(
        HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '30' SECOND)
    )
    GROUP BY user_id, window_start, window_end
    HAVING SUM(amount) > 2000.0
""")

# Règle DLQ : Inchangée, mais on précise les colonnes pour ignorer la colonne virtuelle event_time
statement_set.add_insert_sql("""
    INSERT INTO transactions_dlq
    SELECT transaction_id, user_id, amount, `timestamp`, merchant, location
    FROM transactions 
    WHERE amount < 0.0
""")

statement_set.execute()