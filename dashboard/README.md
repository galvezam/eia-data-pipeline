# EIA Energy Dashboard

An interactive U.S. energy data visualization dashboard built with Streamlit + Plotly.

## Features

- **US map** — hover any state for quick stats; click to open a drill-down detail panel
- **Time filters** — year slider (+ month slider for monthly datasets)
- **Metric selector** — switch between primary and secondary metrics within a dataset
- **State detail panel** — metric snapshot, time-series chart, breakdown by origin country or crude grade
- **US trend chart** — area chart of the total across all states over time
- **State rankings table** — all states sorted by selected metric for the chosen period

## Quick Start

### 1. Install dependencies (and create virtual environment)

```bash
cd eia-data-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r dashboard/requirements.txt
```

### 2. Set your AWS credentials

Open `dashboard/config.py` and fill in:

```python
AWS_ACCESS_KEY = "YOUR_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_SECRET_KEY"
AWS_REGION     = "us-east-1"          # or your actual bucket region
BUCKET_NAME    = "cs4266-eia-big-data-bucket"
```

### 3. Run the app

```bash
streamlit run dashboard/app.py
```

The dashboard will open automatically at `http://localhost:8501`.

---

## Adding a New Dataset

When you add a new processed Parquet prefix to S3 (e.g. `processed/electricity_generation/`), you only need to add **one entry** to the `DATASETS` dict in `dashboard/data_loader.py`. No other changes are required.

### Minimum required entry

```python
"your_dataset_key": {
    "label": "Electricity — Generation by State",  # shown in sidebar
    "s3_prefix": "processed/electricity_generation/",
    "state_col": "state_name",          # column name in your parquet that holds the state
    "value_col": "generation_mwh",      # primary numeric column (colors the map)
    "value_label": "Generation",        # human-readable label for the primary value
    "unit": "MWh",                      # unit string shown in tooltips/axes
    "time_col": "period",               # time column name in your parquet
    "time_granularity": "monthly",      # "monthly" | "annual"
    "category": "Electricity",          # groups datasets in the sidebar category dropdown
    "extra_cols": {},                   # additional columns to show in state detail panel
},
```

### Optional keys

| Key | Description |
|---|---|
| `extra_cols` | Dict of `{parquet_col: "Human Label (Unit)"}` — shown as metric cards in the state detail panel |
| `breakdown_col` | A categorical column (e.g. `"origin_country"`, `"fuel_type"`) that will get a bar chart in the detail panel |
| `breakdown_value` | The numeric column paired with `breakdown_col` |

### State column naming

The `state_col` field tells the loader which column contains the state name. Supported formats:
- Full state names: `"California"`, `"Texas"`
- EIA ALL-CAPS: `"CALIFORNIA"`, `"TEXAS"`
- EIA prefix codes: `"USA-TX"`, `"USA-CA"`

---

## Project Structure

```
dashboard/
├── app.py            # Main Streamlit application
├── data_loader.py    # S3 data fetching + DATASETS registry
├── config.py         # AWS credentials & bucket config
├── requirements.txt  # Python dependencies
└── README.md         # This file
```
