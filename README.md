# 🚀 Pipeline Streaming & ML Embarqué (Détection de Fraude)

Ce projet implémente une architecture complète de traitement de données en streaming à haute vélocité couplée à un système d'intelligence artificielle en temps réel (In-Line ML Inference). Il intègre la gouvernance stricte des données, l'analyse temporelle complexe et un routage asynchrone pour la détection et l'alerte instantanée de fraudes bancaires.

## 🚀 Fonctionnalités Clés
- **Contrat d'Interface Strict (Gouvernance) :** Utilisation d'**Apache Avro** et de **Confluent Schema Registry** pour valider et rejeter à la source toute donnée ne respectant pas le schéma établi, protégeant ainsi le Data Lake.
- **Stateful Processing & Fenêtres Temporelles :** Utilisation d'**Apache Flink** pour traquer le comportement des fraudeurs sur la durée grâce au calcul d'agrégations complexes sur des fenêtres glissantes (Hop Windows) et la gestion des retards réseau (Watermarks).
- **Inférence ML Embarquée (In-Line ML) :** Injection d'un modèle d'Intelligence Artificielle (UDF Python) directement dans la mémoire de l'exécuteur Flink (TaskManager) pour scorer la probabilité de fraude en moins d'une milliseconde sans appel réseau externe.
- **Résilience & Dead Letter Queue (DLQ) :** Isolation automatique des événements techniques corrompus (ex: montants négatifs) vers une file d'attente Kafka dédiée pour analyse ultérieure, garantissant zéro interruption du flux principal.
- **Architecture Multi-Sink (Alerting) :** Routage asynchrone parallèle pour expédier simultanément la donnée froide (Stockage analytique Iceberg) et la donnée chaude (Notifications temps réel via Kafka).

## 🏗️ Architecture Technique
- **Ingestion & Broker :** Apache Kafka
- **Sérialisation & Gouvernance :** Apache Avro & Confluent Schema Registry
- **Stream Processing & ML :** Apache Flink (PyFlink SQL) & Python UDFs
- **Data Lake (Format Table) :** Apache Iceberg
- **Stockage Cloud :** Google Cloud Storage (GCS)
- **Data Warehouse / Serverless BI :** Google BigQuery
- **Infrastructure & Conteneurisation :** Docker Compose (JobManager, TaskManager, Kafka cluster)

## 📁 Structure du Projet
- **`/producer/`** : "Le Générateur". Contient le script de simulation bancaire (`main.py`), la définition stricte du schéma de données (`transaction.avsc`) et le microservice de simulation d'envoi de SMS en temps réel (`alerts_consumer.py`).
- **`/flink_jobs/`** : "Le Moteur de Streaming". Contient le cœur de l'architecture (`streaming_job.py`) qui définit les requêtes SQL Flink, la fonction de Machine Learning (UDF), ainsi que les connecteurs JAR nécessaires pour faire le pont entre Kafka, Iceberg et Google Cloud.
- **`docker-compose.yml`** : "L'Infrastructure". Fichier d'orchestration locale déployant les nœuds de calcul Flink, le cluster Kafka et le Schema Registry.

## 💡 Pourquoi ce projet ?
Ce PoC démontre la capacité à créer un pont ultra-robuste entre le Data Engineering de flux (Streaming) et l'industrialisation de modèles IA (MLOps). Il prouve aux entreprises qu'il est possible de stopper une fraude en quelques millisecondes (grâce à Flink et Kafka) tout en garantissant des propriétés transactionnelles ACID sur le Data Lake (grâce à Iceberg) pour les analyses BigQuery futures. C'est l'essence même d'une architecture Big Data moderne de niveau production.