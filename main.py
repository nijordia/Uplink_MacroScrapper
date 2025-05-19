import json
import logging
import importlib
import argparse
import os
from typing import Dict, Any
from dotenv import load_dotenv

from interfaces.data_fetchers.base_fetcher import BaseDataFetcher
from interfaces.preprocessors.base_preprocessor import BasePreprocessor
from interfaces.database.aws_uploader import AWSUploader
from interfaces.database.db_model_mapper import DBModelMapper
from frameworks.aws.dynamodb_client import DynamoDBClient
from use_cases.fetch_country_data import FetchCountryDataUseCase
from use_cases.preprocess_data import PreprocessDataUseCase
from use_cases.upload_to_database import UploadToDatabaseUseCase
from use_cases.generate_and_store_summary import GenerateIndicatorTableUseCase
from interfaces.database.economic_data_repository import EconomicDataRepository

UPLINK_ASCII = r"""

    Macro_   _
        | | | |____  _      ___ _   _ _  __
        | | | |  _ \| |    |_ _| \ | | |/ /
        | | | | |_) | |     | ||  \| | ' / 
        | |_| |  __/| |___  | || |\  | . \ 
         \___/| |   |_____||_ _|_|_\_|_|\_\
              |_|      
                                    uplink
    
    """
#         for metric in metrics:


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("macro_scraper.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def print_table(data, title):
    print(f"\n{title}")
    print(f"{'Indicator':80} {'Value':>15} {'Date':>12} {'Unit':>10}")
    print("-" * 80)
    for d in data:
        print(f"{d.indicator_name:80} {d.value:15,.2f} {d.date} {d.unit:>10}")



def load_config(config_path: str) -> Dict[str, Any]:
    """Load country configuration from JSON file."""
    with open(config_path, 'r',encoding='utf-8') as f:
        return json.load(f)

def load_metrics_for_country(country_code: str) -> list:
    """Load metric names from the indicator metadata file for a country."""
    metadata_path = os.path.join("config", f"{country_code.lower()}_indicator_metadata.json")
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Indicator metadata file not found for country: {country_code}")
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return list(metadata.keys())



def create_fetcher(country_code: str, fetcher_config: Dict[str, Any]) -> BaseDataFetcher:
    """Dynamically create a data fetcher instance based on configuration."""
    class_name = fetcher_config.get("class_name")
    params = fetcher_config.get("params", {})
    
    # Check if we need to replace API keys from environment variables
    if class_name == "USAPIFetcher" and "api_key" in params:
        # Get API key from environment if it's a placeholder
        if params["api_key"] == "YOUR_API_KEY_HERE":
            params["api_key"] = os.getenv("US_API_KEY", "")
    
    try:
        # Import the module containing the fetcher class
        module = importlib.import_module(f"interfaces.data_fetchers.{class_name.lower()}")
        # Get the class from the module
        fetcher_class = getattr(module, class_name)
        # Create an instance with parameters
        return fetcher_class(**params)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Error creating fetcher for {country_code}: {str(e)}")


def create_preprocessor(country_code: str, preprocessor_config: Dict[str, Any]) -> BasePreprocessor:
    """Dynamically create a preprocessor instance based on configuration."""
    class_name = preprocessor_config.get("class_name")
    
    try:
        # Import the module containing the preprocessor class
        module = importlib.import_module(f"interfaces.preprocessors.{class_name.lower()}")
        # Get the class from the module
        preprocessor_class = getattr(module, class_name)
        # Create an instance
        return preprocessor_class()
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Error creating preprocessor for {country_code}: {str(e)}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Uplink: Economic Data Pipeline CLI"
    )
    parser.add_argument(
        "--country", "-c", action="append", metavar="CODE",
        help="Country code(s) to process (e.g. US, EU, CL). Can be used multiple times"
    )
    parser.add_argument(
        "--steps", "-s", nargs="+", choices=["fetch", "preprocess", "upload"], default=["fetch", "preprocess", "upload"],
        help = "Pipeline steps to run. Choose from: fetch, preprocess, upload. Default: all steps"
    )
    return parser.parse_args()

def country_menu(country_code, db_path):
    repo = EconomicDataRepository(db_path)
    table_use_case = GenerateIndicatorTableUseCase(repo)
    while True:
        print(f"\nSelected country: {country_code}")
        print("1. Fetch & preprocess & upload data (all indicators)")
        print("2. Refresh import/export tables")
        print("3. Refresh data for a single indicator")
        print("4. Back to country selection")
        choice = input("Choose an option: ")
        if choice == "1":
            # ...existing pipeline for all indicators...
            try:
                config_path = os.path.join("config", "countries_config.json")
                config = load_config(config_path)
                country_config = config[country_code]
                fetcher = create_fetcher(country_code, country_config["fetcher"])
                fetch_use_case = FetchCountryDataUseCase(fetcher)
                metrics = load_metrics_for_country(country_code)
                raw_data = fetch_use_case.execute(country_code, metrics)
                preprocessor = create_preprocessor(country_code, country_config["preprocessor"])
                preprocess_use_case = PreprocessDataUseCase(preprocessor)
                processed_data = preprocess_use_case.execute(country_code, raw_data)
                from interfaces.database.sqlite_uploader import SQLiteUploader
                uploader = SQLiteUploader(db_path)
                upload_use_case = UploadToDatabaseUseCase(uploader)
                upload_success = upload_use_case.execute(processed_data)
                if upload_success:
                    print("Data uploaded successfully.")
                else:
                    print("No data uploaded.")
            except Exception as e:
                print(f"Error in pipeline: {e}")

        elif choice == "2":
            imports = table_use_case.execute(country_code, "import")
            exports = table_use_case.execute(country_code, "exports")
            print_table(imports, f"{country_code} - Latest Imports")
            print_table(exports, f"{country_code} - Latest Exports")

        elif choice == "3":
            # --- Refresh data for a single indicator ---
            try:
                indicators = load_metrics_for_country(country_code)
                print("\nAvailable indicators:")
                for idx, ind in enumerate(indicators):
                    print(f"{idx+1}. {ind}")
                sel = input("Enter indicator number or name: ").strip()
                if sel.isdigit():
                    sel_idx = int(sel) - 1
                    if 0 <= sel_idx < len(indicators):
                        indicator = indicators[sel_idx]
                    else:
                        print("Invalid selection.")
                        continue
                else:
                    if sel in indicators:
                        indicator = sel
                    else:
                        print("Invalid selection.")
                        continue
                # Run pipeline for single indicator
                config_path = os.path.join("config", "countries_config.json")
                config = load_config(config_path)
                country_config = config[country_code]
                fetcher = create_fetcher(country_code, country_config["fetcher"])
                fetch_use_case = FetchCountryDataUseCase(fetcher)
                raw_data = fetch_use_case.execute(country_code, [indicator])
                preprocessor = create_preprocessor(country_code, country_config["preprocessor"])
                preprocess_use_case = PreprocessDataUseCase(preprocessor)
                processed_data = preprocess_use_case.execute(country_code, raw_data)
                from interfaces.database.sqlite_uploader import SQLiteUploader
                uploader = SQLiteUploader(db_path)
                upload_use_case = UploadToDatabaseUseCase(uploader)
                upload_success = upload_use_case.execute(processed_data)
                if upload_success:
                    print(f"Data for '{indicator}' uploaded successfully.")
                else:
                    print("No data uploaded.")
            except Exception as e:
                print(f"Error refreshing indicator: {e}")

        elif choice == "4":
            break


def main():
    """Main entry point for the data ingestion pipeline."""
    print(UPLINK_ASCII)
    args = parse_args()
    load_dotenv()
    logger = setup_logging()
    logger.info("Starting macro economic data ingestion pipeline")

    try:
        # Load configuration
        config_path = os.path.join("config", "countries_config.json")
        config = load_config(config_path)

        # --- Modular database backend selection ---
        db_backend = os.getenv("DB_BACKEND", "aws")
        if db_backend == "aws":
            table_name = os.getenv("DYNAMODB_TABLE", "economic_data")
            db_client = DynamoDBClient(table_name=table_name)
            model_mapper = DBModelMapper()
            uploader = AWSUploader(db_client, model_mapper)
            db_path = None  # Not used for AWS
        elif db_backend == "sqlite":
            from interfaces.database.sqlite_uploader import SQLiteUploader
            sqlite_path = os.getenv("SQLITE_DB_PATH", "local_economic_data.db")
            uploader = SQLiteUploader(sqlite_path)
            db_path = sqlite_path
        else:
            raise ValueError(f"Unsupported DB_BACKEND: {db_backend}")
        # --- End modular backend selection ---

        # Check database connection
        if not uploader.check_connection():
            logger.error("Failed to connect to database. Aborting.")
            return

        # --- INTERACTIVE COUNTRY MENU ---
        available_countries = list(config.keys())
        while True:
            print("\nAvailable countries:", ", ".join(available_countries))
            selected = input("Enter country code to process (or 'exit' to quit): ").strip().upper()
            if selected == "EXIT":
                break
            if selected not in available_countries:
                print("Invalid country code.")
                continue

            # Show country-specific menu
            country_menu(selected, db_path or "local_economic_data.db")  # fallback for AWS

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()