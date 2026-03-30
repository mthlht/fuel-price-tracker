import pandas as pd

# Chargement des données
raw_prix = pd.read_csv(
    "data/fuel-prices-2026.csv",
    sep=";"
)

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
tidy_roll_mean = (
    tidy_roll_mean
    .sort_values("date")
    .groupby("nom_carbu", group_keys=False)
    .apply(lambda df: df.assign(
        roll_mean_price=df["mean_price"].rolling(window=7, min_periods=7).mean(),
        roll_mean_left=df["mean_price"][::-1].rolling(window=7, min_periods=7).mean()[::-1]
    ))
    .reset_index(drop=True)
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
tidy_roll_mean.to_csv("data/roll_mean_2026.csv", index=False)
