# %%
from pathlib import Path
from tqdm import tqdm
import requests
import csv

# Better than pandas! Has actual types
import polars as pl

# %%
with Path("./landlocked 2023-06-26 - Лист1.csv").open() as file:
    reader = csv.DictReader(file)
    countries = list(map(lambda row: row["landlocked country"], reader))
countries


# %%
def wbsearchentities(name: str):
    response = requests.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbsearchentities",
            "search": name,
            "language": "en",
            "format": "json",
        },
    )
    response.raise_for_status()
    return response.json()


# %%
