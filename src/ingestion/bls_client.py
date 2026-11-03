import os
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class BLSDataIngestor:
    """
    Idempotent data ingestion client for BLS productivity dataset and US Population API.
    Lands raw files into Databricks Unity Catalog Volume.
    """
    
    BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
    POPULATION_API_URL = (
        "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
        "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    )

    def __init__(self, target_volume_path: str, user_email: str):
        self.target_volume_path = target_volume_path
        # Custom User-Agent header with email required by BLS policy to bypass 403 Forbidden
        self.headers = {"User-Agent": f"DataEngineeringQuest ({user_email})"}
        
        if not os.path.exists(self.target_volume_path):
            os.makedirs(self.target_volume_path, exist_ok=True)

    def _get_remote_file_list(self) -> list[str]:
        """Scrape BLS directory index page dynamically without hardcoding filenames."""
        logger.info(f"Scraping remote directory index from {self.BLS_BASE_URL}")
        response = requests.get(self.BLS_BASE_URL, headers=self.headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        files = []
        for link in soup.find_all("a"):
            href = link.get("href", "")
            if not href:
                continue
            
            # Extract clean filename regardless of relative or absolute paths
            filename = href.rstrip("/").split("/")[-1]
            
            # BLS productivity dataset files start with 'pr.' (e.g., pr.data.1.AllData, pr.series)
            if filename.startswith("pr."):
                files.append(filename)
                
        return sorted(list(set(files)))

    def ingest_bls_directory(self) -> None:
        """Download remote BLS files idempotently into target Volume."""
        remote_files = self._get_remote_file_list()
        logger.info(f"Found {len(remote_files)} files on BLS source server.")

        for filename in remote_files:
            remote_file_url = urljoin(self.BLS_BASE_URL, filename)
            local_file_path = os.path.join(self.target_volume_path, filename)

            # Idempotency check: Skip download if local file exists and is non-empty
            if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 0:
                logger.info(f"Skipping existing file: {filename}")
                continue

            logger.info(f"Downloading: {filename} -> {local_file_path}")
            res = requests.get(remote_file_url, headers=self.headers, timeout=60)
            res.raise_for_status()

            with open(local_file_path, "wb") as f:
                f.write(res.content)

    def ingest_population_api(self, output_filename: str = "us_population.json") -> None:
        """Fetch US Population API payload and land as raw JSON in target Volume."""
        local_file_path = os.path.join(self.target_volume_path, output_filename)
        logger.info(f"Fetching population data from {self.POPULATION_API_URL}")
        
        response = requests.get(self.POPULATION_API_URL, headers=self.headers, timeout=30)
        response.raise_for_status()

        with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info(f"Saved population payload to {local_file_path}")
