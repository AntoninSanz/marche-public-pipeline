import requests
import re
import os
import asyncio
import httpx
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from datetime import timedelta

semaphore = asyncio.Semaphore(20)
BUCKET = "publicspendingdata"


async def download_file(url, dest_path, s3=None):
    """Download a file from a URL and save it to the specified destination path
    """

    async with semaphore:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                content = response.content
                # Clean up XML content by removing attributes from <marches> tag
                content_updated = re.sub(
                    r'<marches[^>]*>', '<marches>', content.decode('utf-8'))
                # Save the cleaned content to s3
                s3.Bucket(BUCKET).put_object(
                    Key=f'{dest_path}', Body=content_updated.encode('utf-8'))
        except httpx.RequestError as e:
            print(f"error during download {url}: {e}")
        except Exception as e:
            print(f"unknown error for {url}: {e}")


async def ingestion_from_data_gouv(DATASET_URL, s3=None, client=None):
    """Ingest data from data.gouv.fr dataset
    """

    response = requests.get(DATASET_URL)
    r = response.json()

    count = 0
    tasks = []
    max_date = "1900-01-01"
    # Read the last ingestion date
    try:
        obj = client.get_object(Bucket=BUCKET, Key="last_ingestion.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        last_ingestion_date = data["last_ingestion_date"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            last_ingestion_date = (pd.Timestamp.now() - timedelta(days=1)
                                   ).strftime("%Y-%m-%d")

    # Iterate dataset and their resources
    for dataset in r["data"]:
        for res in dataset["resources"][:10000]:
            print(res["url"])
            filename = res["title"]
            # Extract date from filename
            match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
            if match:
                date_folder = match.group(1)
                # Skip files that have already been ingested
                if last_ingestion_date >= date_folder:
                    continue

            count += 1

            new_max_date = date_folder

            if new_max_date > max_date:
                max_date = new_max_date
            print(date_folder)
            # Create directory for the date if it doesn't exist
            base_dir = os.path.join("raw", f"decp_{date_folder}")
            os.makedirs(base_dir, exist_ok=True)
            # Schedule the download task
            tasks.append(download_file(
                res["url"], os.path.join(base_dir, filename), s3=s3))

    await asyncio.gather(*tasks)

    print(
        f"Total new files ingested: {count}, last ingestion date: {max_date}")
    # Update the last ingestion date in s3
    new_state = {"last_ingestion_date": max_date,
                 "last_run": pd.Timestamp.now()}
    s3.Bucket(BUCKET).put_object(
        Key=f'last_ingestion.json', Body=pd.DataFrame([new_state]).to_json(orient='records').encode('utf-8'))

if __name__ == "__main__":
    client = boto3.client("s3")
    s3 = boto3.resource('s3')
    DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/?q=donnees-essentielles-de-la-commande-publique"

    asyncio.run(ingestion_from_data_gouv(DATASET_URL, s3=s3, client=client))
