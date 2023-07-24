import os
import logging
import time
import requests
from b2sdk.v1 import InMemoryAccountInfo, B2Api, FileVersionInfo
from requests.adapters import HTTPAdapter, Retry

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
#   HTTP Requests Configuration
# -----------------------------------------------------------------------------
# Setup automatic session for HTTP requests with retries.
# This is to ensure that intermittent failures in network connectivity
# do not cause the requests to fail immediately.
automatic_session = requests.Session()
retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
automatic_session.mount('http://', HTTPAdapter(max_retries=retries))

# -----------------------------------------------------------------------------
#   HTTP API Interactions
# -----------------------------------------------------------------------------
def run_inference(params):
    config = {
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

    api_name = params["api_name"]

    if api_name in config["api"]:
        api_config = config["api"][api_name]
    else:
        raise Exception("Method '%s' not yet implemented")

    api_verb = api_config[0]
    api_path = api_config[1]

    response = {}

    if api_verb == "GET":
        response = automatic_session.get(
                url='%s%s' % (config["baseurl"], api_path),
                timeout=config["timeout"])

    if api_verb == "POST":
        response = automatic_session.post(
                url='%s%s' % (config["baseurl"], api_path),
                json=params, 
                timeout=config["timeout"])

    return response.json()

# -----------------------------------------------------------------------------
#   Backblaze B2 Configuration and Interactions
# -----------------------------------------------------------------------------
def get_b2_bucket():
    account_id = os.getenv('005b2784557c8a40000000002')
    application_key = os.getenv('K005eKedENd+DOf1StTM0hsSMty8Q3g')
    bucket_name = '11AABees'

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account('production', account_id, application_key)
    bucket = b2_api.get_bucket_by_name(bucket_name)

    return bucket

def upload_file_to_b2(file_path, file_name=None):
    file_name = file_name or os.path.basename(file_path)
    with open(file_path, 'rb') as file:
        try:
            logger.info(f"Uploading {file_name} to Backblaze B2...")
            file_info = bucket.upload_bytes(file.read(), file_name)
            logger.info(f"Upload successful: {file_info.file_name}")
            return file_info
        except Exception as e:
            logger.error(f"Failed to upload {file_name} to Backblaze B2: {e}")
            return None

# Initialize Backblaze B2 bucket
bucket = get_b2_bucket()

# -----------------------------------------------------------------------------
#   Serverless Function Handler
# -----------------------------------------------------------------------------
def handler(event):
    """
    Handler function that performs API inference and uploads the result to B2 bucket.

    Args:
        event (dict): The event data passed into the handler. Expected to contain
                      the key 'input' with the input data for the API.

    Returns:
        list: A list containing the URL of the uploaded file in the B2 bucket.
    """
    # Run inference using the API
    response = run_inference(event["input"])

    # Save the response to a file (assuming it's text data)
    with open('response.txt', 'w') as file:
        file.write(str(response))

    # Upload the file to Backblaze B2
    file_info = upload_file_to_b2('response.txt')

    # Return the URL of the uploaded file
    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{bucket.name}/{file_info.file_name}"]

# Start the serverless service
runpod.serverless.start({"handler": handler})
