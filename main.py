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

REKOGNITION_ROLE_ARN = os.getenv("REKOGNITION_SERVICE_ROLE_ARN")
KDS_OUTPUT_STREAM_NAME = os.getenv("KDS_OUTPUT_STREAM_NAME", "rekognition-output")

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
            "mostofa62/kinesis_gst_python",
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

rekognition = boto3.client(
    "rekognition", 
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET, 
    region_name=AWS_REGION)

kds = boto3.client(
    "kinesis", 
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET, 
    region_name=AWS_REGION)


# ---------------------- SHARED FUNCTIONS ----------------------

def create_shared_data_stream():
    try:
        response = kds.describe_stream(StreamName=KDS_OUTPUT_STREAM_NAME)
        print(f"⚠️ KDS stream '{KDS_OUTPUT_STREAM_NAME}' already exists")
        return response["StreamDescription"]["StreamARN"]
    except kds.exceptions.ResourceNotFoundException:
        response = kds.create_stream(StreamName=KDS_OUTPUT_STREAM_NAME, ShardCount=1)
        print(f"✅ Created KDS stream '{KDS_OUTPUT_STREAM_NAME}'")
        waiter = kds.get_waiter("stream_exists")
        waiter.wait(StreamName=KDS_OUTPUT_STREAM_NAME)
        response = kds.describe_stream(StreamName=KDS_OUTPUT_STREAM_NAME)
        return response["StreamDescription"]["StreamARN"]
    except Exception as e:
        print(f"❌ Error creating/describing KDS stream: {e}")
        return None

def create_stream_processor_for_camera(camera_name, kvs_stream_arn, kds_output_stream_arn, collection_id):
    processor_name = f"processor-{camera_name}"
    try:
        rekognition.describe_stream_processor(Name=processor_name)
        print(f"⚠️ Stream processor '{processor_name}' already exists")
        return
    except rekognition.exceptions.ResourceNotFoundException:
        pass  # continue to create

    try:
        response = rekognition.create_stream_processor(
            Name=processor_name,
            Input={'KinesisVideoStream': {'Arn': kvs_stream_arn}},
            Output={
                'KinesisDataStream': {'Arn': kds_output_stream_arn}
            },
            Settings={
                'FaceSearch': {
                    'CollectionId': collection_id,  # your existing collection
                    'FaceMatchThreshold': 75.0
                }
            },
            RoleArn=REKOGNITION_ROLE_ARN,
            # NotificationChannel={
            #     "SNSTopicArn": "arn:aws:sns:us-east-1:898709018953:SafeScopeRekognitionNotifications"
            # }
        )



        print(f"✅ Created stream processor: {processor_name}")
    except Exception as e:
        print(f"❌ Failed to create stream processor for {camera_name}: {e}")

# ---------------------- ORIGINAL FUNCTIONS ----------------------

def create_stream(name):
    try:
        response = kvs.describe_stream(StreamName=name)
        print(f"⚠️ Stream '{name}' already exists: {response['StreamInfo']['StreamARN']}")
        return response["StreamInfo"]["StreamARN"]
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceNotFoundException":
            response = kvs.create_stream(
                StreamName=name,
                MediaType='video/h264',
                DataRetentionInHours=24
            )
            print(f"✅ Created stream '{name}': {response['StreamARN']}")
            return response["StreamARN"]
        else:
            print(f"❌ ClientError during create_stream: {e}")
    except Exception as e:
        print(f"❌ Unexpected error in create_stream: {e}")
    return None


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
            agenecy {
                person_datastores(limit: 1, order_by: { id: asc }) {
                    id
                }
            }
          }
        }
    """)
    
    cameras = client.execute(query)["cameras_rtsp"]

    # Flatten person_datastores to datastore_id
    for cam in cameras:
        agency = cam.get("agenecy", {})
        person_list = agency.get("person_datastores", [])
        cam["datastore_id"] = person_list[0]["id"] if person_list else None
        # Remove the original array if not needed
        cam["agenecy"].pop("person_datastores", None)

    return cameras



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
    print("📸 Running camera check job...")
    kds_output_arn = create_shared_data_stream()
    if not kds_output_arn:
        print("❌ KDS output stream not available — skipping job")
        return
    cameras = fetch_cameras()
    for cam in cameras:
        print('Camera details:', cam)
        print("Datastore ID:", cam["datastore_id"])

        if is_stream_working(cam["link"]):
            print(f"✅ Stream OK: {cam['name']}")
            stream_name = f'kvs-{cam["name"]}'
            kvs_arn = create_stream(stream_name)
            if kvs_arn:
                container_status = invoke_docker_kinesis_stream(cam["id"], cam["link"], stream_name)
                if container_status:
                    create_stream_processor_for_camera(cam["name"], kvs_arn, kds_output_arn,cam["datastore_id"])
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