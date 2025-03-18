import logging
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
import pytz

# Initialize Firebase Admin SDK
cred = credentials.Certificate("firebase_credentials.json")
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

logging.basicConfig(level=logging.INFO)

class MetricsFixer:
    def __init__(self):
        self.db = firestore.client()
        self.eastern = pytz.timezone('America/New_York')

    def fix_metrics_for_collection(self, collection_ref):
        """Fix the metrics for a given collection, removing invalid entries and adjusting new_view_count."""
        entries = collection_ref.order_by('timestamp').stream()
        previous_entry_data = None

        for entry in entries:
            entry_data = entry.to_dict()
            timestamp = entry_data['timestamp']

            # Convert timestamp if it's a DatetimeWithNanoseconds object
            if hasattr(timestamp, 'to_pydatetime'):
                timestamp = timestamp.to_pydatetime()
            elif isinstance(timestamp, datetime):
                pass  # It's already a datetime object
            else:
                logging.error(f"Unknown timestamp type: {type(timestamp)}")
                continue

            # Delete entries not on minute 0 or 30
            if timestamp.minute not in [0, 30]:
                logging.info(f"Deleting entry with timestamp: {timestamp}")
                entry.reference.delete()
                continue

            # Adjust new_view_count based on the previous entry
            if previous_entry_data:
                # Calculate the new_view_count based on the difference from the previous entry's view_count
                entry_data['new_view_count'] = max(
                    0, entry_data.get('view_count', 0) - previous_entry_data.get('view_count', 0)
                )
                logging.info(f"Adjusted new_view_count for entry at {timestamp}: {entry_data['new_view_count']}")

            # Update the entry with the corrected new_view_count and timestamp
            entry.reference.set(entry_data, merge=True)
            previous_entry_data = entry_data

    def process_organization_metrics(self, org_id):
        """Process and fix metrics for the organization-level collection."""
        logging.info(f"Processing organization metrics for {org_id}")
        org_metrics_ref = self.db.collection('organizations').document(org_id).collection('metrics').document('hourly').collection('data')
        self.fix_metrics_for_collection(org_metrics_ref)

    def process_content_plan_metrics(self, org_id, plan_id):
        """Process and fix metrics for the content plan-level collection."""
        logging.info(f"Processing content plan {plan_id} in organization {org_id}")
        content_plan_metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('hourly').collection('data')
        self.fix_metrics_for_collection(content_plan_metrics_ref)

    def run(self):
        """Process all organizations and content plans to fix their metrics."""
        logging.info("Starting metrics processing...")

        orgs_ref = self.db.collection('organizations').stream()

        for org in orgs_ref:
            org_id = org.id
            # Process organization metrics
            try:
                self.process_organization_metrics(org_id)
            except Exception as e:
                logging.error(f"An error occurred while processing metrics for organization {org_id}: {e}")

            # Process content plan metrics
            content_plans_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').stream()
            for plan in content_plans_ref:
                plan_id = plan.id
                try:
                    self.process_content_plan_metrics(org_id, plan_id)
                except Exception as e:
                    logging.error(f"An error occurred while processing metrics for content plan {plan_id} in organization {org_id}: {e}")

        logging.info("Metrics processing completed for all content plans and organizations.")

if __name__ == "__main__":
    fixer = MetricsFixer()
    fixer.run()
