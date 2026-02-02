import requests
import re
import os


def ingestion_from_data_gouv(DATASET_URL):
    response = requests.get(DATASET_URL)
    r = response.json()
    for dataset in r["data"]:
        for res in dataset["resources"]:
            print(res["url"])
            filename = res["title"]

            match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
            if match:
                date_folder = match.group(1)

            print(date_folder)
            base_dir = os.path.join("raw", f"decp_{date_folder}")
            os.makedirs(base_dir, exist_ok=True)
            r = requests.get(res["url"])
            content = r.content
            content_updated = re.sub(
                r'<marches[^>]*>', '<marches>', content.decode('utf-8'))
            with open(os.path.join(base_dir, filename), "wb") as f:
                f.write(content_updated.encode('utf-8'))


if __name__ == "__main__":
    DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/?q=donnees-essentielles-de-la-commande-publique"

    ingestion_from_data_gouv(DATASET_URL)
