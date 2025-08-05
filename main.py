import time
import cv2
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import os
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from apscheduler.schedulers.background import BackgroundScheduler
import os
import socket
from urllib.parse import urlparse
from dotenv import load_dotenv
import docker
load_dotenv()
# Config
HASURA_URL = os.getenv("HASURA_URL")
HASURA_ADMIN_SECRET = os.getenv("HASURA_ADMIN_SECRET")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")


docker_client = docker.from_env()
def invoke_docker_kinesis_stream(camera_id, rtsp_link, stream_name):
    container_name = f"stream_{stream_name}"  # make sure it's unique & valid

    # Check if container with the same name is already running, stop/remove it first
    try:
        existing = docker_client.containers.get(container_name)
        print(f"⚠️ Container {container_name} already running, stopping...")
        existing.stop()
        existing.remove()
    except docker.errors.NotFound:
        # Container does not exist, no problem
        pass

    env_vars = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "STREAM_NAME": stream_name,
        "RTSP_URI": rtsp_link,
    }

    try:
        container = docker_client.containers.run(
            "mostofa62/rtspdockertest",
            detach=True,
            name=container_name,
            environment=env_vars,
            remove=True,  # auto-remove after container stops
            # You can add more options here (volumes, ports) if needed
        )
        print(f"✅ Started Docker container '{container_name}' for camera {camera_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to start container '{container_name}': {e}")
        return False

# Set up GraphQL client
transport = RequestsHTTPTransport(
    url=HASURA_URL,
    headers={"x-hasura-admin-secret": HASURA_ADMIN_SECRET}
)
client = Client(transport=transport, fetch_schema_from_transport=True)

kvs = boto3.client(
    'kinesisvideo',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION
)


def create_stream(name):
    try:
        # First, check if the stream already exists
        response = kvs.describe_stream(StreamName=name)
        print(f"⚠️ Stream '{name}' already exists: {response['StreamInfo']['StreamARN']}")
        return True  # Stream already exists

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == "ResourceNotFoundException":
            # Stream does not exist, so create it
            try:
                response = kvs.create_stream(
                    StreamName=name,
                    MediaType='video/h264',
                    DataRetentionInHours=24
                )
                print(f"✅ Created stream '{name}': {response['StreamARN']}")
                return True
            except ClientError as ce:
                print(f"❌ ClientError during create_stream: {ce.response['Error']['Code']} - {ce.response['Error']['Message']}")
            except BotoCoreError as be:
                print(f"❌ BotoCoreError during create_stream: {be}")
        else:
            print(f"❌ ClientError during describe_stream: {error_code} - {e.response['Error']['Message']}")
    except BotoCoreError as e:
        print(f"❌ BotoCoreError during describe_stream: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during describe/create: {e}")

    return False


# Fetch top 5 inactive, not-running cameras
def fetch_cameras():
    query = gql("""
        query {
          cameras_rtsp(
            where: {running_status: {_eq: 0}}
            limit: 5
          ) {
            id
            name
            link
          }
        }
    """)
    return client.execute(query)["cameras_rtsp"]


def is_stream_accessible(url, timeout=5):
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 554  # Default RTSP port

        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as e:
        print(f"⚠️ Unable to reach host {host}:{port} - {e}")
        return False

def is_stream_working(url, timeout=10):
    if not is_stream_accessible(url):
        return False

    try:
        cap = cv2.VideoCapture(url)
        if not cap.isOpened():
            print(f"⚠️ Unable to open stream: {url}")
            return False

        start_time = time.time()
        while time.time() - start_time < timeout:
            ret, frame = cap.read()
            if ret:
                cap.release()
                return True
            time.sleep(1)

        cap.release()
        return False
    except Exception as e:
        print(f"❌ Exception while checking stream {url}: {e}")
        return False
    


    # Update Hasura status
def update_camera_status(camera_id):
    mutation = gql("""
        mutation UpdateStatus($id: uuid!) {
          update_cameras_rtsp_by_pk(
            pk_columns: {id: $id},
            _set: {running_status: 1, health_status:1}
          ) {
            id
          }
        }
    """)
    client.execute(mutation, variable_values={"id": camera_id})

# Scheduler Job
def job():
    print("Running scheduler job...")
    cameras = fetch_cameras()
    for cam in cameras:
        print('Camera details:', cam)
        if is_stream_working(cam["link"]):
            print(f"✅ Stream OK: {cam['name']}")
            stream_name = f'kvs-{cam["name"]}'
            if create_stream(stream_name):
                container_status = invoke_docker_kinesis_stream(cam["id"], cam["link"],stream_name)
                if container_status:
                    update_camera_status(cam["id"])
        else:
            print(f"❌ Stream down: {cam['name']}")
        
        time.sleep(2)  # Small delay between cameras to prevent overload

if __name__ == "__main__":
    print("📡 Stream checker scheduler running...")
    interval_type = os.getenv("INTERVAL_TYPE", "seconds").lower()
    interval_duration = int(os.getenv("INTERVAL_DURATION", 5))
    # Validate the interval type
    valid_interval_types = {"seconds", "minutes", "hours", "days", "weeks"}
    if interval_type not in valid_interval_types:
        print(f"❌ Invalid INTERVAL_TYPE '{interval_type}', defaulting to 'seconds'")
        interval_type = "seconds"

    scheduler = BackgroundScheduler()
    #scheduler.add_job(job, 'interval', minutes=interval_duration)
    scheduler.add_job(job, 'interval', **{interval_type: interval_duration})
    
    scheduler.start()

    # Run job once at startup
    job()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler stopped.")