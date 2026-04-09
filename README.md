# 🚀 Pipeline de Données RFM (Docker & Airflow)

> **Statut** : ✅ Production Ready  
> **Contexte** : Projet pédagogique Ingénieur Data (Bac+5)  
> **Auteur** : David Brimeux

## 📖 Description

Ce projet implémente un pipeline **ETL (Extract, Transform, Load)** complet et automatisé pour la segmentation client selon la méthode **RFM** (Récence, Fréquence, Montant).

L'objectif est de démontrer la maîtrise de l'orchestration de données avec **Apache Airflow**, de la conteneurisation avec **Docker**, et du traitement de données avec **Python** et **PostgreSQL**. Le pipeline transforme un fichier brut de ventes (Excel) en une table de scores clients exploitable pour le marketing.

### ✨ Points Forts de l'Architecture
- **100% Reproductible** : Configuration externalisée dans `docker-compose.yml` (variables d'environnement `AIRFLOW_VAR_*`).
- **Zéro Intervention Manuelle** : Création automatique de l'utilisateur admin et des variables au premier lancement.
- **Robuste** : Mécanisme de `retries` automatiques et gestion d'erreurs détaillée.
- **Documenté** : Documentation intégrée directement dans l'interface Airflow (`doc_md`).

---

## 🛠️ Stack Technique

| Composant | Technologie | Rôle |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.7.3 | Planification et exécution des tâches ETL |
| **Base de Données** | PostgreSQL 13 | Stockage des données brutes et résultats RFM |
| **Conteneurisation** | Docker & Docker Compose | Isolation et déploiement de l'infrastructure |
| **Traitement** | Python 3.8 (Pandas, SQLAlchemy) | Logique métier d'ingestion et de calcul RFM |
| **Source de Données** | Excel (Online Retail II) | Dataset historique des ventes (UCI Repository) |

---

## 🚀 Démarrage Rapide (Quick Start)

Grâce à l'automatisation mise en place, le déploiement tient en **une seule commande**.

### 1. Prérequis
Assurez-vous d'avoir installé :
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Git

### 2. Cloner le projet
```bash
git clone https://github.com/DBX-76/data-pipeline-rfm.git
cd data-pipeline-rfm