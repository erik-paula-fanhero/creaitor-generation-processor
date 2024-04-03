import asyncio
import os

from fastapi import FastAPI

from creaitor_core.generate_video import create_video_with_ai
from dotenv import load_dotenv

# Corrected imports for aiobotocore
import aiobotocore.session

load_dotenv()

app = FastAPI()

async def process_message(message):
    # Process the message asynchronously
    print("Processing message:", message['Body'])
    await create_video_with_ai(message['Body'])

async def listen_to_queue(queue_url):
    # Create a session
    session = aiobotocore.session.get_session()
    async with session.create_client('sqs', region_name=os.getenv("AWS_REGION")) as sqs:
        print("Listening to the queue")
        while True:
            # Receive messages from the queue asynchronously
            response = await sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    # Process each message asynchronously
                    await process_message(message)

                    # Delete the message from the queue asynchronously
                    await sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

@app.on_event("startup")
async def startup_event():
    queue_url = os.getenv("SQS_QUEUE")
    if queue_url:
        asyncio.create_task(listen_to_queue(queue_url))
    else:
        print("SQS_QUEUE environment variable is not set.")

@app.get("/health")
async def health_check():
    return {"status": "ok"}
