"""
app.py — EIA U.S. Energy Data Visualization Dashboard

Run with:
    streamlit run dashboard/app.py

Interactive US choropleth map with hover tooltips and click-to-drill-down
state detail panels. Designed to expand gracefully as new datasets are added
to the S3 processed/ prefix — see data_loader.py for the registry.
"""

import calendar
from collections import defaultdict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import data_loader as dl
from data_loader import DATASETS, STATE_ABBREV, filter_df, get_months, get_years, load_dataset

# ── Page configuration ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EIA Energy Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
      /* Import Inter font */
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

      html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
      }

      /* Dark gradient background */
      .stApp {
        background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #0d1117 100%);
      }

      /* Sidebar */
      section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161b22 0%, #0d1117 100%);
        border-right: 1px solid #30363d;
      }

      /* Metric cards are rendered as custom HTML — see render_metric() below */

      /* Section headers */
      h1, h2, h3 {
        color: #e6edf3 !important;
      }

      h1 {
        background: linear-gradient(90deg, #58a6ff, #bc8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
        font-size: 2rem !important;
      }

      /* Dividers */
      hr {
        border-color: #30363d;
      }

      /* Pill badges */
      .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }
      .badge-blue  { background: rgba(88,166,255,0.15); color: #58a6ff; border: 1px solid rgba(88,166,255,0.3); }
      .badge-green { background: rgba(63,185,80,0.15);  color: #3fb950; border: 1px solid rgba(63,185,80,0.3);  }
      .badge-orange{ background: rgba(210,153,34,0.15); color: #d2993e; border: 1px solid rgba(210,153,34,0.3); }

      /* Plotly chart container */
      .stPlotlyChart { border-radius: 12px; overflow: hidden; }

      /* Info box */
      .info-box {
        background: rgba(88,166,255,0.08);
        border: 1px solid rgba(88,166,255,0.25);
        border-radius: 10px;
        padding: 14px 18px;
        color: #8b949e;
        font-size: 0.85rem;
        line-height: 1.6;
      }

      /* Selectbox / slider labels */
      label[data-testid="stWidgetLabel"] {
        color: #8b949e !important;
        font-size: 0.8rem !important;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# STATE_ABBREV is imported from data_loader (canonical full name → 2-letter abbrev).
# _eia_code_to_abbrev: after normalization in load_dataset, df["state"] always
# holds proper full names ("New Mexico", "Texas", etc.), so this is now a
# straightforward dict lookup.
def _eia_code_to_abbrev(name: str) -> str | None:
    """Return 2-letter abbreviation for a canonical state full name, or None."""
    return STATE_ABBREV.get(name.strip()) if isinstance(name, str) else None


def add_abbrev_col(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'state_abbrev' column derived from 'state'; drops unrecognized rows."""
    if "state" not in df.columns:
        return df
    df = df.copy()
    df["state_abbrev"] = df["state"].apply(_eia_code_to_abbrev)
    return df[df["state_abbrev"].notna()]


def render_metric(col, label: str, value: str, help_text: str = "") -> None:
    """
    Render a styled metric card as custom HTML inside a Streamlit column.
    Using inline HTML/CSS bypasses Streamlit's Emotion stylesheet so our
    label and value colors are always applied correctly.
    """
    help_icon = (
        f' <span title="{help_text}" '
        f'style="cursor:help;color:#58a6ff;font-size:0.8rem;">&#9432;</span>'
        if help_text else ""
    )
    col.markdown(
        f"""
        <div style="
            background: rgba(22,27,34,0.85);
            border: 1px solid #30363d;
            border-radius: 10px;
            padding: 14px 18px;
            margin-bottom: 8px;
        ">
            <div style="
                color: #c9d1d9;
                font-size: 0.72rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.06em;
                margin-bottom: 6px;
            ">{label}{help_icon}</div>
            <div style="
                color: #e6edf3;
                font-size: 1.35rem;
                font-weight: 600;
                line-height: 1.2;
            ">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Plotly theme helpers ───────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#8b949e"),
    margin=dict(l=0, r=0, t=32, b=0),
)

CATEGORY_COLOR_SCALES = {
    "Natural Gas": "Blues",
    "Crude Oil": "Oranges",
    "Electricity": "Greens",   # ready for partner expansion
    "Coal": "Greys",
    "Petroleum": "Reds",
}

def category_of(dataset_key: str) -> str:
    return DATASETS[dataset_key]["category"]


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ EIA Energy Explorer")
    st.markdown("---")

    # Group datasets by category
    categories: dict[str, list[str]] = defaultdict(list)
    for key, meta in DATASETS.items():
        categories[meta["category"]].append(key)

    cat_names = list(categories.keys())
    selected_category = st.selectbox(
        "Energy Category",
        options=cat_names,
        key="category_select",
    )

    dataset_options = {DATASETS[k]["label"]: k for k in categories[selected_category]}
    selected_label = st.selectbox(
        "Dataset",
        options=list(dataset_options.keys()),
        key="dataset_select",
    )
    selected_key = dataset_options[selected_label]
    meta = DATASETS[selected_key]

    st.markdown("---")

    # Load data (cached)
    with st.spinner("Loading data from S3…"):
        try:
            df_full = load_dataset(selected_key)
            load_error = None
        except Exception as e:
            df_full = pd.DataFrame()
            load_error = str(e)

    if load_error:
        st.error(f"Failed to load data:\n{load_error}")
        st.stop()

    if df_full.empty:
        st.warning("No data returned from S3 for this dataset.")
        st.stop()

    years = get_years(df_full)
    if not years:
        st.warning("No year data found.")
        st.stop()

    # Use selectbox instead of select_slider — avoids RangeError when only
    # one year/month is available (min == max == 0 crashes the slider widget).
    selected_year = st.selectbox(
        "Year",
        options=sorted(years, reverse=True),
        index=0,
        key="year_select",
    )

    selected_month = None
    if meta["time_granularity"] == "monthly":
        months = get_months(df_full)
        month_labels = {calendar.month_abbr[m]: m for m in sorted(months, reverse=True)}
        selected_month_label = st.selectbox(
            "Month",
            options=list(month_labels.keys()),
            index=0,
            key="month_select",
        )
        selected_month = month_labels[selected_month_label]

    st.markdown("---")

    # Map metric selector (only relevant for datasets with extra cols)
    metric_options = {meta["value_label"]: meta["value_col"]}
    metric_options.update({v: k for k, v in meta["extra_cols"].items()})
    selected_metric_label = st.selectbox(
        "Map Metric",
        options=list(metric_options.keys()),
        key="metric_select",
    )
    selected_metric_col = metric_options[selected_metric_label]

    st.markdown("---")
    st.markdown(
        '<div class="info-box">💡 <strong>Hover</strong> over states for quick stats.<br>'
        '📌 <strong>Click</strong> a state to drill into detail.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("")
    # Reset selected state button
    # We set a _clear_requested flag so the plotly click handler below
    # doesn't immediately overwrite the cleared state on the same re-run.
    if st.button("🔄 Clear state selection", width="stretch"):
        st.session_state["selected_state"] = None
        st.session_state["_clear_requested"] = True


# ── Main content ───────────────────────────────────────────────────────────────
badge_cat = selected_category.replace(" ", "_").lower()
badge_colors = {"natural_gas": "blue", "crude_oil": "orange"}
badge_cls = f"badge-{badge_colors.get(badge_cat, 'green')}"

period_str = (
    f"{calendar.month_name[selected_month]} {selected_year}"
    if selected_month
    else str(selected_year)
)

st.markdown(
    f'<h1>U.S. Energy Dashboard</h1>'
    f'<span class="badge {badge_cls}">{selected_category}</span> &nbsp;'
    f'<span style="color:#8b949e; font-size:0.9rem;">{meta["label"]} — {period_str}</span>',
    unsafe_allow_html=True,
)
st.markdown("")

# ── Filter to selected period ──────────────────────────────────────────────────
df_map = filter_df(df_full, selected_key, selected_year, selected_month)
df_map = add_abbrev_col(df_map)

if df_map.empty or selected_metric_col not in df_map.columns:
    st.warning("No data available for the selected filters.")
    st.stop()

# Aggregate if multiple rows per state remain (e.g. after adding abbrev col)
df_map = df_map.groupby(["state", "state_abbrev"], as_index=False)[selected_metric_col].sum()

# ── US Choropleth Map ──────────────────────────────────────────────────────────
color_scale = CATEGORY_COLOR_SCALES.get(selected_category, "Viridis")
unit = meta["unit"]

hover_template = (
    "<b>%{customdata[0]}</b><br>"
    f"{selected_metric_label}: %{{z:,.1f}} {unit}"
    "<extra></extra>"
)

fig_map = go.Figure(
    go.Choropleth(
        locations=df_map["state_abbrev"],
        z=df_map[selected_metric_col],
        locationmode="USA-states",
        colorscale=color_scale,
        colorbar=dict(
            title=dict(text=f"{selected_metric_label}<br>({unit})", side="right"),
            tickfont=dict(color="#8b949e", size=11),
            bgcolor="rgba(13,17,23,0.7)",
            bordercolor="#30363d",
            len=0.75,
        ),
        hovertemplate=hover_template,
        customdata=df_map[["state"]].values,
        marker_line_color="#30363d",
        marker_line_width=0.8,
    )
)

fig_map.update_layout(
    geo=dict(
        scope="usa",
        bgcolor="rgba(0,0,0,0)",
        lakecolor="rgba(13,17,23,0.8)",
        landcolor="rgba(22,27,34,0.6)",
        showlakes=True,
        showcoastlines=False,
        projection_type="albers usa",
    ),
    height=520,
    clickmode="event+select",
    **PLOTLY_LAYOUT,
)

# Render the map and capture click events
# Column ratio: wider detail panel when a state is active, map-dominant otherwise
_state_active = bool(st.session_state.get("selected_state"))
map_col, detail_col = st.columns([2, 1] if _state_active else [3, 1], gap="large")

with map_col:
    selected_points = st.plotly_chart(
        fig_map,
        width="stretch",
        key="choropleth_map",
        on_select="rerun",
        selection_mode="points",
    )
    st.markdown(
        '<p style="color:#484f58; font-size:0.75rem; text-align:center;">'
        'Click any state to open the detail panel →</p>',
        unsafe_allow_html=True,
    )

# ── Detail Panel ───────────────────────────────────────────────────────────────
with detail_col:
    # Determine which state was clicked
    clicked_abbrev: str | None = None
    if (
        selected_points
        and hasattr(selected_points, "selection")
        and selected_points.selection
        and selected_points.selection.get("points")
    ):
        pt = selected_points.selection["points"][0]
        # Plotly returns location (abbrev) for choropleth clicks
        clicked_abbrev = pt.get("location") or pt.get("customdata", [None])[0]

    # Allow persisting click across re-runs
    if "selected_state" not in st.session_state:
        st.session_state["selected_state"] = None

    # Consume the clear flag (if the clear button was just pressed, skip
    # applying any click from the chart so the cleared state persists).
    clear_was_requested = st.session_state.pop("_clear_requested", False)

    if clicked_abbrev and not clear_was_requested:
        # Look up the canonical state name from df_map by abbreviation.
        # df_map["state"] is guaranteed normalized after load_dataset() runs
        # normalize_eia_state(), so this lookup is always correct.
        match = df_map[df_map["state_abbrev"] == clicked_abbrev]
        if not match.empty:
            new_state = match.iloc[0]["state"]
            # Only rerun if the selected state actually changed.
            # st.rerun() forces an immediate second pass so the column ratio
            # (decided at the top of the script) reflects the new selection
            # right away, rather than lagging one click behind.
            if new_state != st.session_state.get("selected_state"):
                st.session_state["selected_state"] = new_state
                st.rerun()

    selected_state: str | None = st.session_state.get("selected_state")

    if not selected_state:
        # Show a leaderboard when no state is selected
        st.markdown("### 📊 Top States")
        st.markdown(
            f'<p style="color:#8b949e; font-size:0.85rem;">Ranked by {selected_metric_label} ({period_str})</p>',
            unsafe_allow_html=True,
        )
        top_n = df_map.nlargest(10, selected_metric_col)[["state", selected_metric_col]]
        top_n.columns = ["State", f"{selected_metric_label} ({unit})"]
        top_n = top_n.reset_index(drop=True)
        top_n.index += 1

        fig_bar = px.bar(
            top_n,
            x=f"{selected_metric_label} ({unit})",
            y="State",
            orientation="h",
            color=f"{selected_metric_label} ({unit})",
            color_continuous_scale=color_scale,
        )
        fig_bar.update_layout(
            height=380,
            showlegend=False,
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            xaxis=dict(tickfont=dict(size=10)),
            **PLOTLY_LAYOUT,
        )
        st.plotly_chart(fig_bar, width="stretch")

    else:
        st.markdown(f"### 📌 {selected_state}")
        period_badge = f'<span class="badge badge-blue">{period_str}</span>'
        st.markdown(period_badge, unsafe_allow_html=True)
        st.markdown("")

        # ── Metric snapshot ────────────────────────────────────────────────────
        state_row = df_map[df_map["state"] == selected_state]

        if not state_row.empty:
            primary_val = state_row.iloc[0][selected_metric_col]
            cols_m = st.columns(2)
            render_metric(cols_m[0], selected_metric_label, f"{primary_val:,.1f}", unit)

            # Show extra cols from the full (unfiltered) snapshot for this period
            df_snap = df_full[df_full["year"] == selected_year].copy()
            if selected_month:
                df_snap = df_snap[df_snap["month"] == selected_month]
            df_snap = df_snap[df_snap["state"] == selected_state]

            extra_shown = 0
            for col_key, col_label in meta["extra_cols"].items():
                if col_key in df_snap.columns and not df_snap.empty:
                    val = df_snap[col_key].sum()
                    label_short = col_label.split("(")[0].strip()
                    this_unit = col_label.split("(")[-1].rstrip(")") if "(" in col_label else ""
                    disp_val = f"{val:,.1f}" if isinstance(val, float) else str(val)
                    render_metric(cols_m[(extra_shown + 1) % 2], label_short, disp_val, this_unit)
                    extra_shown += 1
        else:
            st.info(f"No data for {selected_state} in {period_str}.")

        st.markdown("")

        # ── Time series chart ──────────────────────────────────────────────────
        st.markdown(f"**{selected_metric_label} over time**")

        df_state_ts = df_full[df_full["state"] == selected_state].copy()
        if not df_state_ts.empty and selected_metric_col in df_state_ts.columns:
            if meta["time_granularity"] == "monthly":
                ts = (
                    df_state_ts.groupby("period", as_index=False)[selected_metric_col]
                    .sum()
                    .sort_values("period")
                )
                x_col = "period"
            else:
                ts = (
                    df_state_ts.groupby("year", as_index=False)[selected_metric_col]
                    .sum()
                    .sort_values("year")
                )
                x_col = "year"

            fig_ts = px.line(
                ts,
                x=x_col,
                y=selected_metric_col,
                labels={selected_metric_col: f"{selected_metric_label} ({unit})", x_col: ""},
                color_discrete_sequence=["#58a6ff"],
            )
            fig_ts.update_traces(line=dict(width=2.5), mode="lines+markers", marker=dict(size=4))
            fig_ts.update_layout(height=200, **PLOTLY_LAYOUT)
            st.plotly_chart(fig_ts, width="stretch")

        # ── Breakdown chart (origin country / crude grade) ─────────────────────
        breakdown_col = meta.get("breakdown_col")
        breakdown_val = meta.get("breakdown_value")

        if breakdown_col and breakdown_val:
            df_bd = df_full[df_full["state"] == selected_state].copy()
            df_bd = df_bd[df_bd["year"] == selected_year]
            if selected_month:
                df_bd = df_bd[df_bd["month"] == selected_month]

            if not df_bd.empty and breakdown_col in df_bd.columns:
                bd_agg = (
                    df_bd.groupby(breakdown_col, as_index=False)[breakdown_val]
                    .sum()
                    .sort_values(breakdown_val, ascending=False)
                    .head(10)
                )
                label_map = {
                    "origin_country": "Top Import Origins",
                    "crude_grade": "Crude Grade Mix",
                }
                bd_title = label_map.get(breakdown_col, breakdown_col.replace("_", " ").title())
                st.markdown(f"**{bd_title}**")
                fig_bd = px.bar(
                    bd_agg,
                    x=breakdown_val,
                    y=breakdown_col,
                    orientation="h",
                    color=breakdown_col,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    labels={breakdown_val: unit, breakdown_col: ""},
                )
                fig_bd.update_layout(
                    height=260,
                    showlegend=False,
                    yaxis=dict(autorange="reversed"),
                    **PLOTLY_LAYOUT,
                )
                st.plotly_chart(fig_bd, width="stretch")

# ── Bottom row: US trend + data table ─────────────────────────────────────────
st.markdown("---")
trend_col, table_col = st.columns([3, 2], gap="large")

with trend_col:
    st.markdown("### 📈 US Total Over Time")
    if meta["time_granularity"] == "monthly":
        us_ts = (
            df_full.groupby("period", as_index=False)[meta["value_col"]]
            .sum()
            .sort_values("period")
        )
        x_col_us = "period"
    else:
        us_ts = (
            df_full.groupby("year", as_index=False)[meta["value_col"]]
            .sum()
            .sort_values("year")
        )
        x_col_us = "year"

    fig_us = px.area(
        us_ts,
        x=x_col_us,
        y=meta["value_col"],
        labels={meta["value_col"]: f"{meta['value_label']} ({unit})", x_col_us: ""},
        color_discrete_sequence=["#58a6ff"],
    )
    fig_us.update_traces(
        line=dict(width=2, color="#58a6ff"),
        fillcolor="rgba(88,166,255,0.12)",
    )
    fig_us.update_layout(height=260, **PLOTLY_LAYOUT)
    st.plotly_chart(fig_us, width="stretch")

with table_col:
    st.markdown("### 📋 State Rankings")
    st.markdown(
        f'<p style="color:#8b949e;font-size:0.82rem;">All states — {period_str}</p>',
        unsafe_allow_html=True,
    )
    display_df = (
        df_map[["state", selected_metric_col]]
        .sort_values(selected_metric_col, ascending=False)
        .reset_index(drop=True)
    )
    display_df.index += 1
    display_df.columns = ["State", f"{selected_metric_label} ({unit})"]
    st.dataframe(
        display_df,
        width="stretch",
        height=260,
    )
