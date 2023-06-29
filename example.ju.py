# %%
import polars as pl  # Better than pandas
from SPARQLWrapper import SPARQLWrapper, JSON as JsonReturn
from tqdm import tqdm

# %%
pl.Config.set_fmt_str_lengths(100)

# %%

input_countries = (
    pl.scan_csv("./data/landlocked 2023-06-26 - Лист1.csv")
    .select(pl.col("landlocked country").alias("name"))
    .collect()
    .to_series()
    .to_list()
)
input_countries


# %%
class WikidataRequests:
    def __init__(self) -> None:
        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        self.sparql.setReturnFormat(JsonReturn)

    def query(self, query: str, schema=None):
        self.sparql.setQuery(query)
        result: dict = self.sparql.queryAndConvert()  # type:ignore

        return pl.DataFrame(result["results"]["bindings"], schema=schema).select(
            pl.col("*").apply(lambda var: var["value"])
        )

    def find_country(self, name: str):
        return self.query(
            f"""\
SELECT ?country ?countryLabel ?countryDescription WHERE {{
    ?country rdfs:label "{name}"@en;
        wdt:P31 wd:Q3624078. # 'instance of' 'sovereign state'
    FILTER(NOT EXISTS {{ ?country (p:P31/ps:P31) wd:Q3024240. }}) # is NOT a 'historical country'
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 1""",
            schema=["country", "countryLabel", "countryDescription"],
        )

    def find_bordering(self, id: str):
        return self.query(
            f"""\
SELECT ?bordering ?borderingLabel WHERE {{
    ?bordering wdt:P31 wd:Q3624078. # 'instance of' 'sovereign state'
    ?bordering wdt:P47 wd:{id}. # 'shares border with'
    FILTER(NOT EXISTS {{ ?bordering (p:P31/ps:P31) wd:Q3024240. }}) # is NOT a 'historical country'
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}""",
            schema=["bordering", "borderingLabel"],
        )


wikidata = WikidataRequests()


# %%
def find_countries(countries):
    dfs = []
    for country in tqdm(countries, total=len(countries)):
        dfs.append(wikidata.find_country(country))
    return pl.DataFrame({"query": countries}).join(
        pl.concat(dfs), left_on="query", right_on="countryLabel", how="left"
    )


# %%
found_countries = find_countries(input_countries)
found_countries


# %%
print(
    "Found",
    len(found_countries.filter(pl.col("country").is_not_null())),
    "/",
    len(found_countries),
)


# %%
def extract_id(uri: str):
    return uri.rsplit("/", 1)[1]


found_countries.with_columns(pl.col("country").apply(extract_id).alias("id"))
# %%
b = wikidata.find_bordering("Q403")
b
# %%
b.with_columns(pl.lit("Q403").alias("id"))
