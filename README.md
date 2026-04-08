# mon_projet_rfm

Ce projet contient un pipeline Airflow pour calculer des scores RFM à partir de données transactionnelles.

Structure du projet:

- `docker-compose.yml` : lance toute l'infrastructure
- `.env` : variables d'environnement (ne pas committer les secrets)
- `dags/` : définitions des workflows Airflow
- `scripts/` : logique métier ETL
- `data/` : données locales optionnelles

Usage rapide:

1. Copier `.env` et y ajouter les secrets si nécessaire.
2. Lancer `docker compose up -d`.
3. Accéder à Airflow sur `http://localhost:8080`.
