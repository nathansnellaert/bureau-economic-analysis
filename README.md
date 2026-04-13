# bureau-economic-analysis

Connector for the U.S. Bureau of Economic Analysis (BEA) API. Publishes core U.S. macroeconomic series — national accounts, fixed assets, GDP by industry, international transactions / investment position, and state-level regional aggregates.

## Coverage

See [`catalog.json`](./catalog.json) for the authoritative list of which BEA API datasets are covered, which are deferred, and why.

Currently covered BEA API datasets:

- **NIPA** — National Income and Product Accounts
- **NIUnderlyingDetail** — underlying NIPA detail tables
- **FixedAssets** — fixed asset stocks and investment
- **GDPbyIndustry** — value added / gross output by industry
- **ITA** — International Transactions Accounts (AllCountries aggregate)
- **IIP** — International Investment Position
- **Regional** — state-level regional economic data

Deferred: `UnderlyingGDPbyIndustry`, `IntlServTrade`, `IntlServSTA`, `MNE`, `InputOutput`. See `catalog.json` for rationale.

## License

BEA data is a U.S. federal government work and is in the public domain under 17 U.S.C. § 105. Every published dataset carries:

```
license: "Public Domain (U.S. Government Work, 17 U.S.C. § 105)"
source:  "U.S. Bureau of Economic Analysis"
```

## Configuration

| Env var | Required | Default | Description |
|---|---|---|---|
| `BEA_API_KEY` | yes | — | BEA API user key. Sign up at https://apps.bea.gov/api/signup/ |
| `BEA_STALE_CUTOFF_YEARS` | no | 3 | Transforms drop datasets whose latest observation is older than `current_year - N`. |
| `BEA_DOWNLOAD_TTL_DAYS` | no | 7 | Download nodes refetch tables/indicators older than this many days. |

## Freshness

Because BEA publishes revisions and new vintages on its own schedule, the connector tracks **per-table last-download timestamps** and refetches after `BEA_DOWNLOAD_TTL_DAYS` days. Transform nodes short-circuit via `data_hash` when the raw payload has not changed — so unchanged datasets are not re-merged or re-published.
