import os
import logging
import json
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from utils.metrics_scraper import MetricsScraper
from utils.content_plan_aggregation import ContentPlanAggregator
from utils.organization_aggregation import OrganizationMetricsAggregator

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)

def initialize_firebase():
    firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS_JSON')
    logging.info(f"Firebase Credentials Path: {firebase_creds_path}")

    if not firebase_creds_path or not os.path.exists(firebase_creds_path):
        raise ValueError("Firebase credentials JSON file path is empty or does not exist")

    with open(firebase_creds_path, 'r') as f:
        firebase_creds = json.load(f)

    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)
    logging.info("Firebase initialized successfully")
    return firestore.client()

# Initialize Firebase
db = initialize_firebase()

def metrics_scraper_http(request):
    logging.info("Starting metrics scraping job...")
    scraper = MetricsScraper()
    scraper.run()
    logging.info("Metrics scraping job completed successfully.")

    logging.info("Starting content plan aggregation...")
    aggregator = ContentPlanAggregator()
    aggregator.run()
    logging.info("Content plan aggregation completed successfully.")

    logging.info("Starting content plan aggregation...")
    aggregator = OrganizationMetricsAggregator()
    aggregator.run()
    logging.info("Content plan aggregation completed successfully.")

    return "Metrics scraping and content plan aggregation jobs completed successfully."

if __name__ == '__main__':
    metrics_scraper_http(None)
