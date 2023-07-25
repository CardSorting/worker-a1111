import os
import logging
import requests
from b2sdk.v1 import InMemoryAccountInfo, B2Api
from requests.adapters import HTTPAdapter, Retry

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HTTPRequestHandler:
    """
    This class handles HTTP requests with automatic retries for intermittent network failures.
    """
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))

    def request(self, method, url, timeout=600, json=None):
        """
        Performs the HTTP request and returns the response as JSON.
        """
        response = self.session.request(method, url, timeout=timeout, json=json)
        return response.json()

class B2Bucket:
    """
    This class provides a wrapper for interacting with a Backblaze B2 bucket.
    """
    def __init__(self, account_id, application_key, bucket_name):
        info = InMemoryAccountInfo()
        self.b2_api = B2Api(info)
        self.b2_api.authorize_account('production', account_id, application_key)
        self.bucket = self.b2_api.get_bucket_by_name(bucket_name)

    def upload_file(self, file_path, file_name=None):
        """
        Uploads a file to the B2 bucket.
        """
        file_name = file_name or os.path.basename(file_path)
        with open(file_path, 'rb') as file:
            file_data = file.read()
        file_info = self.bucket.upload_bytes(file_data, file_name)
        return file_info

def handler(event, api_config, request_handler, b2_bucket):
    """
    Handler function that performs API inference and uploads the result to B2 bucket.
    """
    # Run inference using the API
    api_name = event["input"]["api_name"]
    api_verb, api_path = api_config["api"][api_name]
    url = f"{api_config['baseurl']}{api_path}"
    response = request_handler.request(api_verb, url, json=event["input"])

    # Save the response to a file (assuming it's text data)
    with open('response.txt', 'w') as file:
        file.write(str(response))

    # Upload the file to Backblaze B2
    file_info = b2_bucket.upload_file('response.txt')

    # Return the URL of the uploaded file
    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{b2_bucket.bucket.name}/{file_info.file_name}"]

# Setup configuration
api_config = {
    "baseurl": "http://127.0.0.1:3000",
    "api": {
        "txt2img":  ("POST", "/sdapi/v1/txt2img"),
        "img2img":  ("POST", "/sdapi/v1/img2img"),
        "getModels": ("GET", "/sdapi/v1/sd-models"),
        "getOptions": ("GET", "/sdapi/v1/options"),
        "setOptions": ("POST", "/sdapi/v1/options"),
    },
    "timeout": 600
}

# Initialize request handler and B2 bucket
request_handler = HTTPRequestHandler()
b2_bucket = B2Bucket(os.getenv('B2_ACCOUNT_ID'), os.getenv('B2_APP_KEY'), os.getenv('B2_BUCKET_NAME'))

# Start the serverless service
runpod.serverless.start({"handler": lambda event: handler(event, api_config, request_handler, b2_bucket)})
