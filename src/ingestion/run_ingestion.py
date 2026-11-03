import os
import sys

# Ensure local package import works inside Databricks workspace tasks
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bls_client import BLSDataIngestor

if __name__ == "__main__":
    volume_path = os.getenv("TARGET_VOLUME_PATH", "/Volumes/macroeconomic_data/raw_data/bls_raw_landing")
    email = os.getenv("CONTACT_EMAIL", "praveenr.vi@gmail.com")
    
    ingestor = BLSDataIngestor(target_volume_path=volume_path, user_email=email)
    
    print("Starting BLS Directory Ingestion...")
    ingestor.ingest_bls_directory()
    
    print("Starting US Population API Ingestion...")
    ingestor.ingest_population_api()
    
    print("Ingestion complete. Raw files landed in Unity Catalog Volume.")
