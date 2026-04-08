import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_rfm():
    logger.info(">>> DÉBUT DE LA TRANSFORMATION RFM")

    # 1. Configuration DB
    db_user = os.getenv("DB_USER", "rfm_user")
    db_password = os.getenv("DB_PASSWORD", "rfm_password")
    db_host = os.getenv("DB_HOST", "postgres-business")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "rfm_db")
    
    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    try:
        engine = create_engine(connection_string)
        
        # 2. Lecture des données brutes depuis Postgres
        logger.info("📖 Lecture de la table 'raw_online_retail'...")
        query = "SELECT * FROM raw_online_retail"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            logger.warning("⚠️ La table source est vide ! Impossible de calculer le RFM.")
            return

        logger.info(f"✅ {len(df)} lignes chargées en mémoire pour calcul.")

        # 3. Nettoyage Métier (Spécifique RFM)
        logger.info("🧹 Filtrage des retours et annulations...")
        
        # S'assurer que les colonnes sont au bon format
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
        
        avant_filtre = len(df)
        # On garde uniquement les ventes réelles (quantité > 0 et prix > 0)
        df = df[(df['quantity'] > 0) & (df['price'] > 0)]
        logger.info(f"   - {avant_filtre - len(df)} lignes supprimées (retours/annulations).")
        
        if len(df) == 0:
            logger.error("❌ Plus aucune donnée après filtrage ! Vérifiez vos données brutes.")
            return

        # 4. Calculs RFM
        logger.info("📊 Calcul des scores R, F, M...")
        
        # Date de référence (Jour suivant la date max du dataset)
        snapshot_date = df['invoicedate'].max() + pd.Timedelta(days=1)
        logger.info(f"📅 Date de référence (Snapshot) : {snapshot_date}")

        # Agrégation par client
        rfm = df.groupby('customer_id').agg(
            Recency=('invoicedate', lambda x: (snapshot_date - x.max()).days), # Jours depuis dernier achat
            Frequency=('invoice', 'nunique'),                                  # Nombre de factures DISTINCTES
            Monetary=('price', lambda x: (x * df.loc[x.index, 'quantity']).sum()) # CA Total
        ).reset_index()

        logger.info("📊 Aperçu des scores RFM bruts :")
        logger.info(rfm.head())
        logger.info(rfm.describe()) # Stats rapides

        # 5. Sauvegarde dans une nouvelle table
        logger.info("💾 Sauvegarde dans la table 'rfm_result'...")
        rfm.to_sql('rfm_result', engine, if_exists='replace', index=False)
        
        logger.info(f"🚀 SUCCÈS ! Table 'rfm_result' créée avec {len(rfm)} clients uniques.")

    except Exception as e:
        logger.error(f"❌ Erreur critique : {e}")
        raise e

if __name__ == "__main__":
    transform_rfm()