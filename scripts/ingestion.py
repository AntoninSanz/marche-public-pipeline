import requests
import re
import os
import asyncio
import httpx


semaphore = asyncio.Semaphore(20)


async def download_file(url, dest_path):
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
                # Save the cleaned content to the destination path
                with open(dest_path, "wb") as f:
                    f.write(content_updated.encode('utf-8'))
        except httpx.RequestError as e:
            print(f"error during download {url}: {e}")
        except Exception as e:
            print(f"unknown error for {url}: {e}")


async def ingestion_from_data_gouv(DATASET_URL):
    """Ingest data from data.gouv.fr dataset
    """

    response = requests.get(DATASET_URL)
    r = response.json()

    count = 0
    tasks = []
    # Read the last ingestion date
    with open('last_ingestion.txt', 'r') as f:
        last_ingestion_date = f.read()
    max_date = last_ingestion_date
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
                res["url"], os.path.join(base_dir, filename)))

    await asyncio.gather(*tasks)
    print(
        f"Total new files ingested: {count}, last ingestion date: {max_date}")
    with open('last_ingestion.txt', 'w') as f:
        f.write(max_date)


if __name__ == "__main__":
    DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/?q=donnees-essentielles-de-la-commande-publique"

    asyncio.run(ingestion_from_data_gouv(DATASET_URL))
