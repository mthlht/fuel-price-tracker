import pandas as pd
from pathlib import Path

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

raw_prix = pd.concat(df_list, ignore_index=True)

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

tidy_roll_mean["roll_mean_left"] = (
    tidy_roll_mean
    .groupby("nom_carbu")["mean_price"]
    .apply(lambda x: x[::-1].rolling(7, min_periods=7).mean()[::-1])
    .reset_index(level=0, drop=True)
)

# remplacer NA par version left
tidy_roll_mean["roll_mean_price"] = tidy_roll_mean["roll_mean_price"].fillna(
    tidy_roll_mean["roll_mean_left"]
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

# export
output_file = DATA_DIR / "roll_mean_2026.csv"

tidy_roll_mean.to_csv(output_file, index=False)
