import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Configuration Flink et chargement des connecteurs (Kafka + Iceberg + GCS)
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

#On force Iceberg à "valider" (commit) les données toutes les 10 secondes
t_env.get_config().set("execution.checkpointing.interval", "10000")

jar_paths = ";".join([
    "file:///opt/flink/flink_jobs/flink-sql-connector-kafka-3.1.0-1.18.jar",
    "file:///opt/flink/flink_jobs/iceberg-flink-runtime-1.18-1.5.0.jar",
    "file:///opt/flink/flink_jobs/gcs-connector-hadoop3-2.2.22-shaded.jar",
    "file:///opt/flink/flink_jobs/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
]) 
t_env.get_config().set("pipeline.jars", jar_paths)

# 2. La Source (Lecture depuis Kafka)
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

# 3. La Destination (Table Iceberg dans Google Cloud Storage)
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

# 4. Le Pipeline d'Écriture
print("🚀 Lancement du Pipeline Flink : Écriture des fraudes vers Google Cloud Storage (Iceberg)...")

insert_query = """
    INSERT INTO fraud_alerts
    SELECT 
        user_id, 
        amount, 
        merchant, 
        location
    FROM transactions 
    WHERE amount > 1000.0
"""

t_env.execute_sql(insert_query)