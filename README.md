# Ovrsee

This project is designed to integrate with TikTok's API and manage user data using Firebase. It includes several components for handling authentication, data retrieval, and storage.

## Folder Structure

### Authorization

- **`tiktokAuth.js`**: Handles the TikTok authentication strategy using Passport.js. It loads TikTok API keys from `env.yaml` and sets up the authentication strategy.
- **`tiktokCallback.js`**: Manages the callback from TikTok's OAuth process, initializes Firebase, and saves TikTok video data to Firestore.
- **`function.js`**: Sets up an Express server with session management and routes for handling TikTok OAuth requests.

### TokenRefresh

- **`utils/token_refresher.py`**: Contains the `TokenRefresher` class, which manages the refreshing of TikTok access tokens and interacts with Firestore to store user data.
- **`utils/tiktok_api.py`**: Provides methods for interacting with TikTok's API, including refreshing access tokens and retrieving user information.

### Refresh

- **`main.py`**: Initializes Firebase and sets up the environment for running the token refresh process.

### Automation

- **`main.py`**: Initializes Firebase and sets up the environment for running various automation tasks.
- **`utils/metrics_scraper.py`**: Contains the `MetricsScraper` class, which retrieves user metrics from Firestore.
- **`utils/tiktok_api.py`**: Similar to the `TokenRefresh` version, this file provides methods for interacting with TikTok's API.

### ContentPlanHistory

- **`main.py`**: Initializes Firebase and sets up the environment for managing content plan history.

### DocumentFiller

- **`main.py`**: Initializes Firebase and sets up the environment for filling documents with data.

### Utils

- **`clean.py`**: Provides utility functions for loading Firebase credentials.

