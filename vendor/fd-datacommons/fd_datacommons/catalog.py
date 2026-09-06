"""Datasource manifest catalog for fd-datacommons (Google Data Commons)."""

from typing import Any

# Curated seed of country-level statistical variables. Each entry maps a Google
# Data Commons Statistical Variable DCID (used as the column name, which the
# adapter sends as ``variable_dcids``) to an additive ``dc.*`` concept code.
# DCIDs are stable Data Commons identifiers; the set is intentionally small and
# high-confidence — extend by appending here.
# ponytail: curated seed, not a dynamic variable sweep (DC has no list-all endpoint).
_VARIABLES: list[dict[str, str]] = [
    # dcid                                                   concept                           measure    unit      label
    {"dcid": "Count_Person",                                  "concept": "dc.population.count",  "measure": "value",  "unit": "person",  "label": "Total population"},
    {"dcid": "Count_Person_Male",                             "concept": "dc.population.male",   "measure": "value",  "unit": "person",  "label": "Male population"},
    {"dcid": "Count_Person_Female",                           "concept": "dc.population.female", "measure": "value",  "unit": "person",  "label": "Female population"},
    {"dcid": "Amount_EconomicActivity_GrossDomesticProduct",  "concept": "dc.gdp.nominal",       "measure": "amount", "unit": "USD",      "label": "Nominal GDP"},
    {"dcid": "Amount_EconomicActivity_GrossDomesticProductPerCapita", "concept": "dc.gdp.per_capita", "measure": "amount", "unit": "USD",  "label": "GDP per capita"},
    {"dcid": "LifeExpectancy_Person",                         "concept": "dc.life_expectancy",   "measure": "value",  "unit": "year",     "label": "Life expectancy at birth"},
    {"dcid": "Median_Age_Person",                             "concept": "dc.age.median",        "measure": "value",  "unit": "year",     "label": "Median age"},
    {"dcid": "Median_Income_Person",                          "concept": "dc.income.median",     "measure": "amount", "unit": "USD",      "label": "Median income"},
    {"dcid": "Count_Person_BelowPovertyLevel",                "concept": "dc.poverty.count",     "measure": "value",  "unit": "person",  "label": "Population below poverty level"},
    {"dcid": "UnemploymentRate_Person",                       "concept": "dc.unemployment.rate", "measure": "rate",   "unit": "percent",  "label": "Unemployment rate"},
]

# Empirically-verified global DC stat vars (data for >=2 of USA/CHN/IND/BRA/DEU),
# auto-generated from datacommonsorg/data .mcf files. See statvars.py for provenance
# and the filtering rationale (excludes US-census-granular, weather, and the
# worldBank/* vars that wbgapi already covers with proper names).
from .statvars import STATVARS

_ALL_VARIABLES = _VARIABLES + STATVARS

CATALOG: dict[str, Any] = {
    "version": "1",
    "name": "datacommons",
    "label": "Google Data Commons (country-level statistics)",
    "source_url": "https://datacommons.org",
    "scanner_mode": "upstream-curated",
    "requires": [],
    "ranking_seed": [0.5, 0.5],
    "functions": [
        {
            "command": "get_observation",
            "category": "macroeconomics",
            "description": (
                "Fetch a country-level statistical observation from the Google "
                "Data Commons /v2/observation API. One column per curated DCID "
                "variable; the bound column's name is the variable DCID sent to "
                "the API."
            ),
            "parameters": [
                {"name": "variable_dcids", "type": "list", "required": True,
                 "description": "DCID(s) of the statistical variable(s) to fetch"},
                {"name": "entity_dcids", "type": "list", "required": True,
                 "description": "Entity DCID(s), e.g. country/USA"},
                {"name": "date", "type": "str", "required": True,
                 "description": "Observation date (e.g. '2023') or 'LATEST'"},
            ],
            "columns": [
                {
                    "name": v["dcid"],
                    "type": "float",
                    "description": v["label"],
                    "meaning": v["label"],
                    "frequency": "yearly",
                }
                for v in _ALL_VARIABLES
            ],
            "frequency": "yearly",
            "verified": True,
        },
    ],
    "concepts": [
        {
            "column": v["dcid"],
            "concept": v["concept"],
            "entity_type": "country",
            "measure": v["measure"],
            "unit": v["unit"],
            "frequency": "yearly",
            "confidence": 0.9,
        }
        for v in _ALL_VARIABLES
    ],
    "entities": [
        {"entity_type": "country", "coverage": "universe"},
    ],
    "fetch": {
        "module": "fd_datacommons.provider:DCProvider",
    },
}
