import pandas as pd
from pathlib import Path
from datetime import datetime
import pytz

# chemin vers le dossier du script
BASE_DIR = Path(__file__).resolve().parent.parent  # remonte d'un niveau depuis script/

# chemin vers data/
DATA_DIR = BASE_DIR / "data"

# Dossier contenant les fichiers mensuels
PRICE_DATA_DIR = DATA_DIR / "fuel-prices-csv"

# Liste de tous les fichiers CSV
csv_files = list(PRICE_DATA_DIR.glob("*.csv"))

# Lecture + concaténation
df_list = [
    pd.read_csv(file, sep=";")
    for file in csv_files
]

raw_prix = pd.concat(df_list, ignore_index=True).drop_duplicates()

# Tri des données
tidy_roll_mean = (
    raw_prix
    # filtre pop == "R"
    .loc[raw_prix["pop"] == "R"]
    # conversion date
    .assign(date=lambda df: pd.to_datetime(df["maj"]).dt.date)
    # moyenne par date et carburant
    .groupby(["date", "nom_carbu"], as_index=False)
    .agg(mean_price=("prix_carbu", "mean"))
)

# rolling mean par carburant
tidy_roll_mean = tidy_roll_mean.sort_values("date")

tidy_roll_mean["roll_mean_price"] = (
    tidy_roll_mean
    .groupby("nom_carbu")["mean_price"]
    .rolling(7, min_periods=7)
    .mean()
    .reset_index(level=0, drop=True)
)

# arrondi
tidy_roll_mean["roll_mean_price"] = tidy_roll_mean["roll_mean_price"].round(3)


# filtrer valeurs non nulles
tidy_roll_mean = tidy_roll_mean.dropna(subset=["nom_carbu"])

# sélection + pivot (équivalent de spread)
tidy_roll_mean = (
    tidy_roll_mean[["date", "roll_mean_price", "nom_carbu"]]
    .pivot(index="date", columns="nom_carbu", values="roll_mean_price")
    .reset_index()
)

# renommage des colonnes
tidy_roll_mean = tidy_roll_mean.rename(columns={
    "E10": "SP95-E10"
})

# sélection finale (si colonnes présentes)
cols = ["date", "SP95-E10", "Gazole", "SP98"]
tidy_roll_mean = tidy_roll_mean[[c for c in cols if c in tidy_roll_mean.columns]]


# Noms des mois (équivalent label = TRUE, abbr = FALSE en R)
months_fr = [
  "janvier", "février", "mars", "avril", "mai", "juin",
  "juillet", "août", "septembre", "octobre", "novembre", "décembre"
]

# date_for_axis
tidy_roll_mean["date_for_axis"] = tidy_roll_mean["date"].apply(
  lambda d: months_fr[d.month - 1] if d.day == 1 else d.strftime("%Y-%m-%d")
)

# date_for_popup
def format_popup(d):
  month = months_fr[d.month - 1]

  if d.day == 1:
    return f"1er {month} {d.year}"
  else:
    return f"{d.day} {month} {d.year}"

tidy_roll_mean["date_for_popup"] = tidy_roll_mean["date"].apply(format_popup)

# filtre année 2026
tidy_roll_mean["date"] = pd.to_datetime(tidy_roll_mean["date"], errors="coerce")

tidy_roll_mean = tidy_roll_mean[tidy_roll_mean["date"].dt.year >= 2026]

# Heure actuelle
now = datetime.now(pytz.utc)

# Convertir en heure de Paris (gère CEST / CET automatiquement)
paris_tz = pytz.timezone("Europe/Paris")
now_paris = now.astimezone(paris_tz)

# Extraire heure et minute
heure = now_paris.hour

# Filtre affichage des données du jour, uniquement après 19h
if heure < 10:
  tidy_roll_mean = tidy_roll_mean[
        tidy_roll_mean["date"].dt.date < now_paris.date()
    ]

# export
output_file = DATA_DIR / "roll_mean_2026.csv"

tidy_roll_mean.to_csv(output_file, index=False)
