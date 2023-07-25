import os
import time
import base64
import requests
from b2sdk.v1 import InMemoryAccountInfo, B2Api
from requests.adapters import HTTPAdapter, Retry
import runpod

# Creating a global HTTP session with automatic retries for intermittent issues
automatic_session = requests.Session()
retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
automatic_session.mount('http://', HTTPAdapter(max_retries=retries))


# ---------------------------------------------------------------------------- #
#                             Utility Functions                                #
# ---------------------------------------------------------------------------- #

def wait_for_service(url):
    '''
    Continuously checks if the specified service is ready to receive requests.
    '''
    while True:
        try:
            requests.get(url)
            return
        except requests.exceptions.RequestException:
            print("Service not ready yet. Retrying...")
            time.sleep(0.2)


def run_inference(params, api_config):
    '''
    Executes an inference request on the specified API.
    '''
    api_name = params["api_name"]

    if api_name not in api_config["api"]:
        raise Exception(f"Method '{api_name}' not yet implemented")

    api_verb, api_path = api_config["api"][api_name]
    url = f'{api_config["baseurl"]}{api_path}'

    # Differentiating between GET and POST requests
    if api_verb == "GET":
        response = automatic_session.get(url, timeout=api_config["timeout"])
    elif api_verb == "POST":
        response = automatic_session.post(url, json=params, timeout=api_config["timeout"])

    return response.json()


def initialize_b2(account_id, application_key):
    '''
    Initializes and authorizes a Backblaze B2 API instance.
    '''
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account('production', account_id, application_key)
    return b2_api


def upload_to_b2(b2_api, bucket_name, file_path, file_name=None):
    '''
    Uploads a file to the specified Backblaze B2 bucket.
    '''
    file_name = file_name or os.path.basename(file_path)
    bucket = b2_api.get_bucket_by_name(bucket_name)
    with open(file_path, 'rb') as file:
        file_info = bucket.upload_bytes(file.read(), file_name)
        return file_info


def base64_to_image(base64_string, output_path):
    '''
    Decodes a base64 string and writes it to an image file.
    '''
    image_data = base64.b64decode(base64_string)
    with open(output_path, 'wb') as file:
        file.write(image_data)


# ---------------------------------------------------------------------------- #
#                             Serverless Handler                               #
# ---------------------------------------------------------------------------- #

def handler(event, api_config, b2_api, bucket_name):
    '''
    The handler function to be called by the serverless platform.
    '''
    # Run the inference and get a base64 string
    base64_string = run_inference(event["input"], api_config)

    # Convert the base64 string to an image
    base64_to_image(base64_string, 'output_image.png')

    # Upload the image to Backblaze B2
    file_info = upload_to_b2(b2_api, bucket_name, 'output_image.png')

    # Return the URL of the uploaded file
    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{bucket_name}/{file_info.file_name}"]


# ---------------------------------------------------------------------------- #
#                                    Main                                      #
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
    # Define the API configuration
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

    # Wait for the service to be ready
    wait_for_service(url='http://127.0.0.1:3000/sdapi/v1/txt2img')
    print("WebUI API Service is ready. Starting RunPod...")

    # Initialize B2 API
    b2_api = initialize_b2(os.getenv('B2_ACCOUNT_ID'), os.getenv('B2_APP_KEY'))
    bucket_name = os.getenv('B2_BUCKET_NAME')

    # Start the serverless service with our custom handler
    runpod.serverless.start({"handler": lambda event: handler(event, api_config, b2_api, bucket_name)})
