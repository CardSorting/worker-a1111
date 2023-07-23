import os
import logging
from b2sdk.v1 import InMemoryAccountInfo, B2Api, FileVersionInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_b2_bucket():
    account_id = os.getenv('005b2784557c8a40000000001')
    application_key = os.getenv('K005e9GQkwR8qYSN9NGv7Uw1u6FNhOE')
    bucket_name = '11AABees'  # Replace with your actual bucket name

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account('production', account_id, application_key)
    bucket = b2_api.get_bucket_by_name(bucket_name)
    return bucket

bucket = get_b2_bucket()

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

def handler(job):
    image_path = "./image.png"
    file_info = upload_file_to_b2(image_path)

    if file_info is not None:
        return [f"https://s3.us-east-005.backblazeb2.com/file/{bucket.name}/{file_info.file_name}"]

runpod.serverless.start({"handler": handler})