import logging
from datetime import datetime, timedelta
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
from firebase_admin import firestore
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logging.basicConfig(level=logging.INFO)

class ContentPlanAggregator:
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

    def process_content_plan(self, org_id, plan_id, plan_data):
        db = self.get_db()
        current_timestamp = datetime.utcnow()
        formatted_timestamp = self.format_timestamp(current_timestamp)
        current_date = current_timestamp.date()

        logging.info(f"\n  Processing Content Plan: {plan_id}")
        logging.info(f"  Brand: {plan_data.get('brand', 'N/A')}")

        videos_ref = db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('videos')
        videos = videos_ref.stream()

        aggregated_metrics = {
            'comment_count': 0,
            'like_count': 0,
            'view_count': 0,
            'share_count': 0,
            'new_view_count': 0
        }

        for video in videos:
            video_data = video.to_dict()
            original_video_ref = video_data.get('originalVideoRef')
            if original_video_ref and original_video_ref.get().exists:
                metrics_ref = original_video_ref.collection('Metrics')
                first_metric = metrics_ref.order_by('timestamp', direction=firestore.Query.ASCENDING).limit(1).get()
                latest_metric = metrics_ref.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

                if first_metric and latest_metric:
                    first_metric_data = first_metric[0].to_dict()
                    latest_metric_data = latest_metric[0].to_dict()

                    aggregated_metrics['new_view_count'] += max(0, latest_metric_data['view_count'] - first_metric_data['view_count'])
                    for key in aggregated_metrics:
                        if key != 'new_view_count':
                            aggregated_metrics[key] += latest_metric_data.get(key, 0)

        self.process_hourly_metrics(org_id, plan_id, aggregated_metrics, formatted_timestamp)
        self.process_daily_metrics(org_id, plan_id, current_date)
        self.process_aggregated_metrics(org_id, plan_id, "weekly", 7, current_date)
        self.process_aggregated_metrics(org_id, plan_id, "monthly", 30, current_date)
        self.process_aggregated_metrics(org_id, plan_id, "quarterly", 90, current_date)

    def process_hourly_metrics(self, org_id, plan_id, aggregated_metrics, formatted_timestamp):
        hourly_metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('hourly')
        previous_hourly_entry = hourly_metrics_ref.collection('data').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

        # Add timestamp to the hourly metrics
        aggregated_metrics['timestamp'] = SERVER_TIMESTAMP

        if previous_hourly_entry:
            previous_entry_data = previous_hourly_entry[0].to_dict()
            aggregated_metrics['new_view_count'] = max(0, aggregated_metrics['view_count'] - previous_entry_data.get('view_count', 0))

        hourly_metrics_ref.collection('data').document(formatted_timestamp).set(aggregated_metrics)
        hourly_metrics_ref.set({
            'most_recent_entry': aggregated_metrics,
            'updated_at': SERVER_TIMESTAMP
        }, merge=True)

    def process_daily_metrics(self, org_id, plan_id, current_date):
        daily_metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('daily')
        hourly_metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('hourly').collection('data')

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

            # Calculate new_view_count as the difference between the last and first entry's view_count
            new_view_count = max(0, last_entry_data['view_count'] - first_entry_data['view_count'])

            # Check for previous day's last entry
            previous_day = current_date - timedelta(days=1)
            last_prev_day_entry = daily_metrics_ref.collection('data') \
                .document(previous_day.strftime('%Y%m%d')).get()

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

            daily_metrics_ref.collection('data').document(current_date.strftime('%Y%m%d')).set(daily_metrics)
            daily_metrics_ref.set({
                'most_recent_entry': daily_metrics,
                'updated_at': SERVER_TIMESTAMP
            }, merge=True)

            logging.info(f"  Stored daily aggregation for content plan {plan_id} in organization {org_id} for date {current_date}")

    def process_aggregated_metrics(self, org_id, plan_id, period, days, current_date):
        metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document(period)
        data_sub_collection_ref = metrics_ref.collection('data')
        daily_metrics_ref = self.db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('daily').collection('data')

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
            metrics_ref.set({
                'most_recent_entry': aggregated_metrics,
                'updated_at': SERVER_TIMESTAMP
            })

            logging.info(f"Updated {period} aggregation for content plan {plan_id} in organization {org_id} for period {earliest_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")


    def run(self):
        db = self.get_db()
        orgs_ref = db.collection('organizations')
        orgs = orgs_ref.stream()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for org in orgs:
                plans_ref = orgs_ref.document(org.id).collection('contentPlans')
                active_plans = plans_ref.where('status', '==', 'active').stream()

                for plan in active_plans:
                    futures.append(executor.submit(self.process_content_plan, org.id, plan.id, plan.to_dict()))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An error occurred while processing a content plan: {e}")

        logging.info("Content plan aggregation completed for all plans.")
