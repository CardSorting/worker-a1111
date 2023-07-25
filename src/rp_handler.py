import os
import time
import base64
import requests
from b2sdk.v1 import InMemoryAccountInfo, B2Api
from requests.adapters import HTTPAdapter, Retry
import runpod
import uuid

automatic_session = requests.Session()
retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
automatic_session.mount('http://', HTTPAdapter(max_retries=retries))


# ---------------------------------------------------------------------------- #
#                              Automatic Functions                             #
# ---------------------------------------------------------------------------- #

def wait_for_service(url):
    '''
    Check if the service is ready to receive requests.
    '''
    while True:
        try:
            requests.get(url)
            return
        except requests.exceptions.RequestException:
            print("Service not ready yet. Retrying...")
        except Exception as err:
            print("Error: ", err)

        time.sleep(0.2)


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
    path = None

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

def base64_to_image(base64_string, output_path):
    image_data = base64.b64decode(base64_string)
    with open(output_path, 'wb') as file:
        file.write(image_data)

def initialize_b2(account_id, application_key):
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account('production', account_id, application_key)
    return b2_api

def upload_to_b2(b2_api, bucket_name, file_path, file_name=None):
    file_name = file_name or os.path.basename(file_path)
    bucket = b2_api.get_bucket_by_name(bucket_name)
    with open(file_path, 'rb') as file:
        file_info = bucket.upload_bytes(file.read(), file_name)
        return file_info


# ---------------------------------------------------------------------------- #
#                                RunPod Handler                                #
# ---------------------------------------------------------------------------- #

def handler(event):
    '''
    This is the handler function that will be called by the serverless.
    '''

    response = run_inference(event["input"])
    
    # Check if 'images' is in the response and throw a meaningful error if not
    if "images" not in response or not response["images"]:
        raise Exception("The response from run_inference does not contain 'images' key or the 'images' list is empty.")

    # The base64 string is the first item in the 'images' list
    base64_string = response["images"][0]

    # Generate a unique id for the filename
    file_id = uuid.uuid4()

    # Decode base64 to image
    base64_to_image(base64_string, f'{file_id}.png')

    # Initialize B2 API
    b2_api = initialize_b2(os.getenv('B2_ACCOUNT_ID'), os.getenv('B2_APP_KEY'))

    # Upload the image to Backblaze B2
    file_info = upload_to_b2(b2_api, os.getenv('B2_BUCKET_NAME'), f'{file_id}.png')

    # Return the URL of the uploaded file
    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{os.getenv('B2_BUCKET_NAME')}/{file_id}.png"]

        
if __name__ == "__main__":
    wait_for_service(url='http://127.0.0.1:3000/sdapi/v1/txt2img')

    print("WebUI API Service is ready. Starting RunPod...")

    runpod.serverless.start({"handler": handler})
