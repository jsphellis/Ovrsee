import logging
from datetime import datetime, timedelta
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
from firebase_admin import firestore
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class OrganizationMetricsAggregator:
    def __init__(self, max_workers=10):
        self.db = firestore.client()
        self.max_workers = max_workers
        self.thread_local = threading.local()

    def get_db(self):
        if not hasattr(self.thread_local, "db"):
            self.thread_local.db = firestore.client()
        return self.thread_local.db

    def format_timestamp(self, timestamp):
        return timestamp.strftime('%Y%m%d-%H%M')

    def aggregate_content_plan_metrics(self, org_id):
        db = self.get_db()
        current_timestamp = datetime.utcnow()
        formatted_timestamp = self.format_timestamp(current_timestamp)
        current_date = current_timestamp.date()

        logging.info(f"Aggregating metrics for organization: {org_id}")

        # Initialize aggregated metrics for the organization
        aggregated_metrics = {
            'comment_count': 0,
            'like_count': 0,
            'view_count': 0,
            'share_count': 0,
            'new_view_count': 0,
            'timestamp': SERVER_TIMESTAMP  # Add timestamp for hourly entries
        }

        # Fetch all active content plans for the organization
        plans_ref = db.collection('organizations').document(org_id).collection('contentPlans')
        active_plans = plans_ref.where('status', '==', 'active').stream()

        # Aggregate metrics from each active content plan
        for plan in active_plans:
            plan_id = plan.id
            logging.info(f"Processing content plan {plan_id}")

            # Retrieve metrics from the content plan
            videos_ref = plans_ref.document(plan_id).collection('videos')
            videos = videos_ref.stream()

            for video in videos:
                video_data = video.to_dict()
                original_video_ref = video_data.get('originalVideoRef')
                if original_video_ref and original_video_ref.get().exists:
                    metrics_ref = original_video_ref.collection('Metrics')
                    latest_metric = metrics_ref.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

                    if latest_metric:
                        metric_data = latest_metric[0].to_dict()
                        for key in aggregated_metrics:
                            if key != 'timestamp':  # Don't add timestamp twice
                                aggregated_metrics[key] += metric_data.get(key, 0)

        org_metrics_ref = db.collection('organizations').document(org_id).collection('metrics')

        # Hourly aggregation logic (unchanged)
        hourly_metrics_ref = org_metrics_ref.document('hourly')
        previous_hourly_entry = hourly_metrics_ref.collection('data').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

        if previous_hourly_entry and previous_hourly_entry[0].exists:
            previous_entry_data = previous_hourly_entry[0].to_dict()
            previous_view_count = previous_entry_data.get('view_count', 0)
            current_view_count = aggregated_metrics.get('view_count', 0)
            aggregated_metrics['new_view_count'] = max(0, current_view_count - previous_view_count)
        else:
            logging.warning(f"No valid previous hourly entry for organization {org_id}. Setting new_view_count to 0.")
            aggregated_metrics['new_view_count'] = 0

        # Save the hourly aggregation
        hourly_metrics_ref.collection('data').document(formatted_timestamp).set(aggregated_metrics)
        logging.info(f"Stored hourly aggregation for organization {org_id} at timestamp {formatted_timestamp}")

        # Update the most recent hourly entry
        hourly_metrics_ref.set({
            'most_recent_entry': aggregated_metrics,
            'updated_at': SERVER_TIMESTAMP
        }, merge=True)

        # Process and store daily metrics (unchanged)
        self.process_daily_metrics(org_metrics_ref, current_date, org_id)

        # Process weekly, monthly, and quarterly metrics
        self.process_aggregated_metrics(org_metrics_ref, org_id, 'weekly', 7, current_date)
        self.process_aggregated_metrics(org_metrics_ref, org_id, 'monthly', 30, current_date)
        self.process_aggregated_metrics(org_metrics_ref, org_id, 'quarterly', 90, current_date)

    def process_daily_metrics(self, metrics_ref, current_date, org_id):
        daily_metrics_ref = metrics_ref.document('daily')
        hourly_metrics_ref = metrics_ref.document('hourly').collection('data')

        # Convert current_date to a string format to store in Firestore
        current_date_str = current_date.strftime('%Y%m%d')

        # Get the first and last hourly entries of the day
        first_hourly_entry = hourly_metrics_ref \
            .where('timestamp', '>=', datetime.combine(current_date, datetime.min.time())) \
            .where('timestamp', '<', datetime.combine(current_date + timedelta(days=1), datetime.min.time())) \
            .order_by('timestamp', direction=firestore.Query.ASCENDING) \
            .limit(1) \
            .get()

        last_hourly_entry = hourly_metrics_ref \
            .where('timestamp', '>=', datetime.combine(current_date, datetime.min.time())) \
            .where('timestamp', '<', datetime.combine(current_date + timedelta(days=1), datetime.min.time())) \
            .order_by('timestamp', direction=firestore.Query.DESCENDING) \
            .limit(1) \
            .get()

        if first_hourly_entry and last_hourly_entry:
            first_entry_data = first_hourly_entry[0].to_dict()
            last_entry_data = last_hourly_entry[0].to_dict()

            new_view_count = max(0, last_entry_data['view_count'] - first_entry_data['view_count'])

            previous_day = current_date - timedelta(days=1)
            previous_day_str = previous_day.strftime('%Y%m%d')  # Convert previous day to string

            last_prev_day_entry = daily_metrics_ref.collection('data').document(previous_day_str).get()

            if last_prev_day_entry.exists:
                last_prev_day_view_count = last_prev_day_entry.to_dict().get('view_count', 0)
                new_view_count = max(0, last_entry_data['view_count'] - last_prev_day_view_count)

            daily_metrics = {
                'comment_count': last_entry_data['comment_count'],
                'like_count': last_entry_data['like_count'],
                'view_count': last_entry_data['view_count'],
                'share_count': last_entry_data['share_count'],
                'new_view_count': new_view_count,
                'timestamp': SERVER_TIMESTAMP
            }

            # Store daily entry with current_date as string
            daily_metrics_ref.collection('data').document(current_date_str).set(daily_metrics)

            daily_metrics_ref.set({
                'most_recent_entry': daily_metrics,
                'updated_at': SERVER_TIMESTAMP
            }, merge=True)

            logging.info(f"Stored daily aggregation for organization {org_id} for date {current_date_str}")

    def process_aggregated_metrics(self, metrics_ref, org_id, period, days, current_date):
        data_sub_collection_ref = metrics_ref.document(period).collection('data')
        daily_metrics_ref = metrics_ref.document('daily').collection('data')

        earliest_date = current_date - timedelta(days=days)

        # Get the first and last daily entries within the specified period
        first_entry = daily_metrics_ref \
            .where('timestamp', '>=', datetime.combine(earliest_date, datetime.min.time())) \
            .order_by('timestamp', direction=firestore.Query.ASCENDING) \
            .limit(1).get()

        last_entry = daily_metrics_ref \
            .where('timestamp', '>=', datetime.combine(earliest_date, datetime.min.time())) \
            .order_by('timestamp', direction=firestore.Query.DESCENDING) \
            .limit(1).get()

        if first_entry and last_entry:
            first_entry_data = first_entry[0].to_dict()
            last_entry_data = last_entry[0].to_dict()

            earliest_date = first_entry_data['timestamp'].date() if 'timestamp' in first_entry_data else earliest_possible_date
            earliest_date_str = earliest_date.strftime('%Y-%m-%d')

            # Calculate the differences for all relevant fields
            view_count_diff = max(0, last_entry_data['view_count'] - first_entry_data['view_count'])
            like_count_diff = max(0, last_entry_data['like_count'] - first_entry_data['like_count'])
            share_count_diff = max(0, last_entry_data['share_count'] - first_entry_data['share_count'])
            comment_count_diff = max(0, last_entry_data['comment_count'] - first_entry_data['comment_count'])

            # Create the aggregated metrics
            aggregated_metrics = {
                'new_view_count': view_count_diff,
                'new_like_count': like_count_diff,
                'new_share_count': share_count_diff,
                'new_comment_count': comment_count_diff,
                'timestamp': SERVER_TIMESTAMP,  # Add timestamp for when this was updated
                'period_start': earliest_date.strftime('%Y-%m-%d'),  # Include the start date of the aggregation period
                'period_end': current_date.strftime('%Y-%m-%d'),  # Include the end date of the aggregation period
                'earliest_date': earliest_date_str
            }

            # Store the aggregated metrics inside the 'data' sub-collection
            data_sub_collection_ref.document(period).set(aggregated_metrics, merge=True)

            # Update the most recent entry with details of the aggregation
            metrics_ref.document(period).set({
                'most_recent_entry': aggregated_metrics,
                'updated_at': SERVER_TIMESTAMP
            })

            logging.info(f"Updated {period} aggregation for organization {org_id} for period {earliest_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")


    def run(self):
        db = self.get_db()
        orgs_ref = db.collection('organizations')
        orgs = orgs_ref.stream()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for org in orgs:
                org_id = org.id
                futures.append(executor.submit(self.aggregate_content_plan_metrics, org_id))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An error occurred while processing an organization: {e}")

        logging.info("Organization metrics aggregation completed for all organizations.")
