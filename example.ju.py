# %%
from pathlib import Path
from tqdm import tqdm
import requests
import csv

# Better than pandas
import polars as pl

# %%
pl.Config.set_tbl_rows(100)
pl.Config.set_fmt_str_lengths(100)
# %%
countries = (
    pl.scan_csv("./landlocked 2023-06-26 - Лист1.csv")
    .select(pl.col("landlocked country").alias("name"))
    .collect()
)
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
search_results_by_name = {}
for name in tqdm(countries["name"], total=len(countries)):
    search_results_by_name[name] = wbsearchentities(name)


# %%
def min_description(search_result: dict):
    return {
        "code": search_result["id"],
        "label": search_result["label"],
        "description": search_result["description"],
    }


countries.with_columns(
    pl.col("name")
    .apply(lambda name: min_description(search_results_by_name[name]["search"][0]))
    .alias("found")
).unnest("found")
