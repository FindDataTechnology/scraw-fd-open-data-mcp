# fd-datacommons

Google Data Commons datasource for [fd-open-data-mcp](https://github.com/FindDataTechnology/fd-open-data-mcp).

Exposes a curated seed of country-level statistical variables (population, GDP,
life expectancy, median income, GHG emissions, etc.) from the
[Google Data Commons](https://datacommons.org) `/v2/observation` API as a
`DatasourceManifest`, auto-discovered by `fd-open-data-mcp register-discovered`.

## Install

```bash
pip install -e fd-datacommons
```

## Configure

Set the Data Commons API key (free tier — get one at
https://developers.google.com/datacommons):

```bash
export DC_API_KEY=your_key_here
```

## What it provides

- **Source name:** `datacommons`
- **Entity coverage:** countries (ISO3 → `country/<ISO3>` DCID, e.g. `country/USA`)
- **Variables:** a curated seed — `Count_Person`, GDP (nominal + per capita),
  `LifeExpectancy_Person`, `Median_Age_Person`, `Median_Income_Person`,
  poverty count, unemployment rate, GHG emissions. Each variable is a column on
  the shared `get_observation` function; concept hints bind each column to a
  `dc.*` concept code.
- **Fetch path:** `fd-open-data-mcp read --concept-id <id> --entity-type country
  --entity-id <id> --date 2023` (after `register-discovered` + `seed-entities`).

DC earns its place on what the World Bank (`wbgapi`) lacks or covers thinly —
US Census ACS variables, NOAA climate, and DC's cross-source facet aggregation.
Concepts are DCID-coded (`dc.population.count`, etc.), purely additive: they do
not collide with `wbgapi`'s indicator-coded concepts.
