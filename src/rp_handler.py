import os
import time
import base64
import requests
import uuid
from b2sdk.v1 import InMemoryAccountInfo, B2Api
from requests.adapters import HTTPAdapter, Retry
import runpod

class ServiceHandler:
    """
    A class to handle interactions with the service.
    """

    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.config = {
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

    def wait_for_service(self, url):
        """
        Check if the service is ready to receive requests.
        """
        while True:
            try:
                requests.get(url)
                return
            except requests.exceptions.RequestException:
                print("Service not ready yet. Retrying...")
            except Exception as err:
                print(f"Error: {err}")

            time.sleep(0.2)

    def run_inference(self, params):
        """
        Run the inference operation.
        """
        api_name = params["api_name"]

        if api_name in self.config["api"]:
            api_config = self.config["api"][api_name]
        else:
            raise Exception("Method '%s' not yet implemented")

        api_verb = api_config[0]
        api_path = api_config[1]

        response = {}

        if api_verb == "GET":
            response = self.session.get(
                    url='%s%s' % (self.config["baseurl"], api_path),
                    timeout=self.config["timeout"])

        if api_verb == "POST":
            response = self.session.post(
                    url='%s%s' % (self.config["baseurl"], api_path),
                    json=params, 
                    timeout=self.config["timeout"])

        return response.json()

    @staticmethod
    def base64_to_image(base64_string, output_path):
        """
        Convert a base64 string to an image.
        """
        image_data = base64.b64decode(base64_string)
        with open(output_path, 'wb') as file:
            file.write(image_data)

    @staticmethod
    def initialize_b2(account_id, application_key):
        """
        Initialize the B2 API.
        """
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account('production', account_id, application_key)
        return b2_api

    @staticmethod
    def upload_to_b2(b2_api, bucket_name, file_path, file_name=None):
        """
        Upload a file to B2.
        """
        file_name = file_name or os.path.basename(file_path)
        bucket = b2_api.get_bucket_by_name(bucket_name)
        with open(file_path, 'rb') as file:
            file_info = bucket.upload_bytes(file.read(), file_name)
            return file_info

    def handler(self, event):
        """
        This is the handler function that will be called by the serverless.
        """
        response = self.run_inference(event["input"])
        
        if "images" not in response or not response["images"]:
            raise Exception("The response from run_inference does not contain 'images' key or the 'images' list is empty.")

        base64_string = response["images"][0]
        file_id = uuid.uuid4()
        self.base64_to_image(base64_string, f'{file_id}.png')
        b2_api = self.initialize_b2(os.getenv('B2_ACCOUNT_ID'), os.getenv('B2_APP_KEY'))
        file_info = self.upload_to_b2(b2_api, os.getenv('B2_BUCKET_NAME'), f'{file_id}.png')

        if file_info is not None:
            return [f"https://s3.us-east-005.backblazeb2.com/file/{os.getenv('B2_BUCKET_NAME')}/{file_id}.png"]


if __name__ == "__main__":
    handler = ServiceHandler()
    handler.wait_for_service(url='http://127.0.0.1:3000/sdapi/v1/txt2img')

    print("WebUI API Service is ready. Starting RunPod...")

    runpod.serverless.start({"handler": handler.handler})
