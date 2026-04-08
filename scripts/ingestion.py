import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging

# Configuration des logs pour voir ce qui se passe dans Airflow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_data():
    logger.info(">>> DÉBUT DE L'INGESTION (Excel -> Postgres)")

    # 1. Configuration de la connexion DB
    db_user = os.getenv("DB_USER", "rfm_user")
    db_password = os.getenv("DB_PASSWORD", "rfm_password")
    db_host = os.getenv("DB_HOST", "postgres-business")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "rfm_db")
    
    # 2. Chemins des fichiers (dans le conteneur Docker)
    # Le fichier source est l'Excel brut
    excel_path = "/opt/airflow/data/online_retail_II.xlsx"
    
    # Vérification de l'existence du fichier
    if not os.path.exists(excel_path):
        logger.error(f"❌ Fichier Excel introuvable : {excel_path}")
        logger.error("💡 Assurez-vous d'avoir placé 'online_retail_II.xlsx' dans le dossier 'data/' à la racine.")
        raise FileNotFoundError(f"Le fichier {excel_path} est introuvable.")

    logger.info(f"📖 Lecture du fichier Excel : {excel_path}")
    
    try:
        # 3. Lecture des deux feuilles Excel
        # On lit tout le fichier Excel pour accéder aux noms des feuilles
        xls = pd.ExcelFile(excel_path)
        sheet_names = xls.sheet_names
        logger.info(f"📄 Feuilles détectées : {sheet_names}")
        
        if len(sheet_names) < 2:
            logger.warning("⚠️ Attention : Moins de 2 feuilles trouvées dans l'Excel.")
        
        df_list = []
        for sheet in sheet_names:
            logger.info(f"   - Chargement de la feuille : {sheet}")
            df_temp = pd.read_excel(excel_path, sheet_name=sheet)
            df_list.append(df_temp)
            logger.info(f"     -> {len(df_temp)} lignes lues.")

        # 4. Combinaison des DataFrames (Concaténation)
        logger.info("🔗 Combinaison des feuilles en un seul DataFrame...")
        df_combined = pd.concat(df_list, ignore_index=True)
        logger.info(f"✅ DataFrame combiné : {df_combined.shape[0]} lignes totales, {df_combined.shape[1]} colonnes.")

        # 5. Nettoyage "Brut" (Préparation avant chargement)
        logger.info("🧹 Nettoyage initial...")
        
        # Uniformiser les noms de colonnes (enlever les espaces, minuscules)
        # Ex: 'Customer ID' -> 'customer_id', 'InvoiceDate' -> 'invoicedate'
        df_combined.columns = df_combined.columns.str.strip().str.lower().str.replace(' ', '_')
        
        required_cols = ['invoice', 'customer_id', 'quantity', 'price', 'invoicedate']
        
        missing_cols = [col for col in required_cols if col not in df_combined.columns]
        
        if missing_cols:
            logger.error(f"❌ Colonnes manquantes : {missing_cols}")
            logger.error(f"Colonnes trouvées : {list(df_combined.columns)}")
            raise ValueError(f"Structure de fichier inattendue. Manque : {missing_cols}")

        # Supprimer les lignes sans CustomerID (essentiel pour le RFM)
        before_count = len(df_combined)
        df_combined = df_combined.dropna(subset=['customer_id']) # Attention : utiliser 'customer_id' avec underscore
        logger.info(f"   - Suppression des lignes sans CustomerID : {before_count - len(df_combined)} lignes retirées.")
        
        # Convertir CustomerID en entier
        df_combined['customer_id'] = df_combined['customer_id'].astype(int)
        
        # Convertir la date
        df_combined['invoicedate'] = pd.to_datetime(df_combined['invoicedate'], errors='coerce')

        # 6. Chargement dans PostgreSQL
        logger.info("💾 Chargement dans la table 'raw_online_retail'...")
        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        try:
            engine = create_engine(connection_string)
            # if_exists='replace' : On écrase la table à chaque exécution du DAG (idéal pour le dev)
            df_combined.to_sql('raw_online_retail', engine, if_exists='replace', index=False)
            logger.info(f"🚀 SUCCÈS ! {len(df_combined)} lignes insérées dans 'raw_online_retail'.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion SQL : {e}")
            raise e

    except Exception as e:
        logger.error(f"❌ Erreur critique durant l'ingestion : {e}")
        raise e

if __name__ == "__main__":
    ingest_data()