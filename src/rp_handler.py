import os
import time
import requests
from b2sdk.v1 import InMemoryAccountInfo, B2Api
from requests.adapters import HTTPAdapter, Retry
import runpod

# Setup automatic session
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


def run_inference(params, api_config):
    api_name = params["api_name"]
    path = None

    if api_name in api_config["api"]:
        api_method_config = api_config["api"][api_name]
    else:
        raise Exception("Method '%s' not yet implemented")

    api_verb = api_method_config[0]
    api_path = api_method_config[1]

    response = {}

    if api_verb == "GET":
        response = automatic_session.get(
                url='%s%s' % (api_config["baseurl"], api_path),
                timeout=api_config["timeout"])

    if api_verb == "POST":
        response = automatic_session.post(
                url='%s%s' % (api_config["baseurl"], api_path),
                json=params, 
                timeout=api_config["timeout"])

    return response.json()


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

def handler(event, api_config, b2_api, bucket_name):
    '''
    This is the handler function that will be called by the serverless.
    '''

    json = run_inference(event["input"], api_config)

    # Save the response to a file
    with open('response.txt', 'w') as file:
        file.write(str(json))

    # Upload the file to Backblaze B2
    file_info = upload_to_b2(b2_api, bucket_name, 'response.txt')

    # Return the URL of the uploaded file
    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{bucket_name}/{file_info.file_name}"]


# ---------------------------------------------------------------------------- #
#                                    Main                                      #
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
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

    wait_for_service(url='http://127.0.0.1:3000/sdapi/v1/txt2img')

    print("WebUI API Service is ready. Starting RunPod...")

    b2_api = initialize_b2(os.getenv('B2_ACCOUNT_ID'), os.getenv('B2_APP_KEY'))
    bucket_name = os.getenv('B2_BUCKET_NAME')

    runpod.serverless.start({"handler": lambda event: handler(event, api_config, b2_api, bucket_name)})
