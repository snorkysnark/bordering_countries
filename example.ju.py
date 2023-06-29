# %%
from tqdm import tqdm
import requests

# Better than pandas
import polars as pl


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
def load_entity(id: str):
    response = requests.get(
        "https://www.wikidata.org/w/rest.php/wikibase/v0/entities/items/" + id
    )
    response.raise_for_status()
    return response.json()


# %%
countries = (
    pl.scan_csv("./landlocked 2023-06-26 - Лист1.csv")
    .select(pl.col("landlocked country").alias("name"))
    .collect()
)
countries

# %%
search_results_by_name = {}
for name in tqdm(countries["name"], total=len(countries)):
    search_results_by_name[name] = wbsearchentities(name)

# Cache
# %store search_results_by_name
# %%
# Load from cache
# %store -r search_results_by_name
# %%
countries = countries.with_columns(
    pl.col("name")
    .apply(lambda name: search_results_by_name[name]["search"][0]["id"])
    .alias("code")
)
countries

# %%
entity_by_code = {}
for code in tqdm(countries["code"], total=len(countries)):
    entity_by_code[code] = load_entity(code)


# Cache
# %store entity_by_code
# %%
# Load from cache
# %store -r entity_by_code
# %%
def property_values(entity: dict, prop_name: str):
    return list(
        map(lambda prop: prop["value"]["content"], entity["statements"][prop_name])
    )


# Property:P47 - shares border with
bordering = (
    countries.select("code")
    .with_columns(
        pl.col("code")
        .apply(lambda code: property_values(entity_by_code[code], "P47"))
        .alias("bordering_code")
    )
    .explode("bordering_code")
)
bordering
# %%
names_not_found = bordering.join(
    countries, left_on="bordering_code", right_on="code", how="left"
).filter(pl.col("name").is_null())

for code in tqdm(names_not_found["bordering_code"], total=len(names_not_found)):
    entity_by_code[code] = load_entity(code)


# Cache
# %store entity_by_code
# %%
# Property:P31 - instance of
# Q6256 - country
def is_country(entity: dict):
    for instance_of in entity["statements"].get("P31", []):
        if instance_of["value"]["content"] == "Q6256":
            return True
    return False


countries = (
    pl.Series(
        values=list(
            map(
                lambda item: {"name": item[1]["labels"]["en"], "code": item[0]},
                filter(
                    lambda item: is_country(item[1]),
                    entity_by_code.items(),
                ),
            )
        )
    )
    .to_frame("a")
    .unnest("a")
)
countries
# %%
final = countries.join(bordering, on="code").join(
    countries, left_on="bordering_code", right_on="code"
)
final
# %%
final.write_csv("bordering.csv")
