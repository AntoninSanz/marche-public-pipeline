import os
import csv
import xml.etree.ElementTree as ET

FIELDS = [
    "id_marche",
    "date_notification",
    "objet_marche",
    "montant_marche",
    "id_acheteur",
    "code_postal_acheteur",
]


def parse_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    marches_data = []
    for marche in root.findall(".//marche"):
        marches_data.append({
            "id_marche": marche.findtext("id"),
            "objet_marche": marche.findtext("objet"),
            "date_notification": marche.findtext("dateNotification"),
            "montant_marche": marche.findtext("montant"),
            "id_acheteur": marche.findtext("acheteur/id"),
            "code_postal_acheteur": marche.findtext("lieuExecution/code"),
        })
    return marches_data


if __name__ == "__main__":
    raw_dir = "raw"
    output_file = "staging/decp.csv"

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for date_folder in os.listdir(raw_dir):
            folder_path = os.path.join(raw_dir, date_folder)
            if os.path.isdir(folder_path):
                for filename in os.listdir(folder_path):

                    file_path = os.path.join(folder_path, filename)
                    print(file_path)
                    parsed_data_list = parse_xml(file_path)
                    print(parsed_data_list)
                    for parsed_data in parsed_data_list:
                        writer.writerow(parsed_data)
