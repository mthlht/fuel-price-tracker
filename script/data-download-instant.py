import os
import requests
import zipfile
import tempfile
import pandas as pd
from lxml import etree
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# chemin vers le dossier du script
BASE_DIR = Path(__file__).resolve().parent.parent  # remonte d'un niveau depuis script/

# chemin vers data/
DATA_DIR = BASE_DIR / "data"

# ------------------------
# 1. Téléchargement (sync)
# ------------------------
url = "https://donnees.roulez-eco.fr/opendata/instantane"

zip_path = DATA_DIR / "fuel-prices-instant.zip"
os.makedirs(os.path.dirname(zip_path), exist_ok=True)

response = requests.get(url)
with open(zip_path, "wb") as f:
    f.write(response.content)

# ------------------------
# 2. Extraction
# ------------------------
temp_dir = tempfile.mkdtemp()

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(temp_dir)

# Trouver le XML
xml_file = None
for root_dir, _, files in os.walk(temp_dir):
    for file in files:
        if file.endswith(".xml"):
            xml_file = os.path.join(root_dir, file)
            break

# ------------------------
# 3. Parsing XML
# ------------------------
tree = etree.parse(xml_file)
root = tree.getroot()
pdv_nodes = root.findall(".//pdv")

# ------------------------
# 4. Fonction de parsing
# ------------------------
def parse_pdv(pdv):
    rows = []

    pdv_id = pdv.get("id")
    lat = pdv.get("latitude")
    lon = pdv.get("longitude")
    pop = pdv.get("pop")

    adresse = pdv.findtext("adresse")
    ville = pdv.findtext("ville")

    prix_nodes = pdv.findall("prix")

    for prix in prix_nodes:
        nom_carbu = prix.get("nom")
        id_carbu = prix.get("id")
        maj = prix.get("maj") or prix.get("maaj")
        prix_carbu = prix.get("valeur")

        rows.append({
            "id_pdv": pdv_id,
            "adresse": adresse,
            "ville": ville,
            "latitude": float(lat) / 100000 if lat else None,
            "longitude": float(lon) / 100000 if lon else None,
            "pop": pop,
            "nom_carbu": nom_carbu,
            "id_carbu": id_carbu,
            "maj": maj,
            "prix_carbu": float(prix_carbu) if prix_carbu else None
        })

    return rows

# ------------------------
# 5. Async + parallélisation
# ------------------------
async def main():
    loop = asyncio.get_running_loop()

    # ThreadPool pour CPU-bound léger (XML parsing)
    with ThreadPoolExecutor(max_workers=8) as executor:
        tasks = [
            loop.run_in_executor(executor, parse_pdv, pdv)
            for pdv in pdv_nodes
        ]

        results = await asyncio.gather(*tasks)

    # Flatten
    all_rows = [row for sublist in results for row in sublist]

    df = pd.DataFrame(all_rows)
    return df

# ------------------------
# 6. Exécution
# ------------------------
df = asyncio.run(main())

# Nom du fichier : YYYY-MM-fuel-prices.csv
output_file = DATA_DIR / f"fuel-prices-instant.csv"

# Sauvegarde
df.to_csv(output_file, index = False, sep = ";")

print(f"Fichier sauvegardé : {output_file}")
