# %%
import polars as pl  # Better than pandas
from SPARQLWrapper import SPARQLWrapper, JSON as JsonReturn
from tqdm import tqdm

# %%
# Display longer strings
pl.Config.set_fmt_str_lengths(100)


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
SELECT (?country as ?uri) WHERE {{
    ?country rdfs:label "{name}"@en;
        wdt:P31 wd:Q3624078. # 'instance of' 'sovereign state'
    FILTER(NOT EXISTS {{ ?country (p:P31/ps:P31) wd:Q3024240. }}) # is NOT a 'historical country'
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 1""",
            schema=["uri"],
        ).select(pl.lit(name).alias("label"), pl.col("*"))

    def find_bordering(self, id: str):
        return self.query(
            f"""\
SELECT (?bordering as ?borderingUri) ?borderingLabel WHERE {{
    ?bordering wdt:P31 wd:Q3624078. # 'instance of' 'sovereign state'
    ?bordering wdt:P47 wd:{id}. # 'shares border with'
    FILTER(NOT EXISTS {{ ?bordering (p:P31/ps:P31) wd:Q3024240. }}) # is NOT a 'historical country'
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}""",
            schema=["borderingUri", "borderingLabel"],
        ).select(pl.lit(id).alias("id"), pl.col("*"))


wikidata = WikidataRequests()


# %%
def id_from_uri(uri: str):
    # http://www.wikidata.org/entity/Q39 -> Q39
    return uri.rsplit("/", 1)[1]


# %%
input_countries = (
    pl.scan_csv("./data/landlocked 2023-06-26 - Лист1.csv")
    .select(pl.col("landlocked country").alias("label"))
    .collect()
    .to_series()
    .to_list()
)
input_countries


# %%
def find_countries(countries):
    dfs = []
    for country in tqdm(countries, total=len(countries)):
        dfs.append(wikidata.find_country(country))
    return pl.DataFrame({"label": countries}).join(
        pl.concat(dfs), on="label", how="left"
    )


# %%
country_codes = find_countries(input_countries)
print(
    "Found",
    len(country_codes.filter(pl.col("label").is_not_null())),
    "/",
    len(country_codes),
)

# %%
country_codes = country_codes.with_columns(pl.col("uri").apply(id_from_uri).alias("id"))
country_codes


# %%
def find_bordering_all(ids: list[str]):
    dfs = []
    for id in tqdm(ids, total=len(ids)):
        dfs.append(wikidata.find_bordering(id))
    return pl.concat(dfs)


# %%
bordering = find_bordering_all(country_codes["id"].to_list())
bordering
# %%
final = country_codes.join(bordering, on="id").select(
    [
        pl.col("label"),
        pl.col("id"),
        pl.col("borderingLabel"),
        pl.col("borderingUri").apply(id_from_uri).alias("borderingId"),
    ]
)
final
# %%
final.write_csv("./data/bordering.csv")
