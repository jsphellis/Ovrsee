import os
import logging
from dotenv import load_dotenv  # Import load_dotenv to load environment variables from .env file
from utils.token_refresher import TokenRefresher

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.DEBUG)

def token_refresher_http(request):
    refresher = TokenRefresher()
    refresher.run()
    return "Token refresh job completed successfully."

if __name__ == '__main__':
    token_refresher_http(None)
