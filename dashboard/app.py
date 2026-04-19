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
from data_loader import DATASETS, STATE_ABBREV, agg_for_map, filter_df, get_months, get_weeks, get_years, load_dataset, seds_col_label

# Page configuration:
st.set_page_config(
    page_title="EIA Energy Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
      html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
      .stApp { background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #0d1117 100%); }
      section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161b22 0%, #0d1117 100%);
        border-right: 1px solid #30363d;
      }
      h1, h2, h3 { color: #e6edf3 !important; }
      h1 {
        background: linear-gradient(90deg, #58a6ff, #bc8cff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
        font-size: 2rem !important;
      }
      hr { border-color: #30363d; }
      .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }
      .badge-blue   { background: rgba(88,166,255,0.15);  color: #58a6ff; border: 1px solid rgba(88,166,255,0.3);  }
      .badge-green  { background: rgba(63,185,80,0.15);   color: #3fb950; border: 1px solid rgba(63,185,80,0.3);   }
      .badge-orange { background: rgba(210,153,34,0.15);  color: #d2993e; border: 1px solid rgba(210,153,34,0.3);  }
      .badge-grey   { background: rgba(139,148,158,0.15); color: #8b949e; border: 1px solid rgba(139,148,158,0.3); }
      .badge-red    { background: rgba(248,81,73,0.15);   color: #f85149; border: 1px solid rgba(248,81,73,0.3);   }
      .badge-purple { background: rgba(188,140,255,0.15); color: #bc8cff; border: 1px solid rgba(188,140,255,0.3); }
      .stPlotlyChart { border-radius: 12px; overflow: hidden; }
      .info-box {
        background: rgba(88,166,255,0.08);
        border: 1px solid rgba(88,166,255,0.25);
        border-radius: 10px;
        padding: 14px 18px;
        color: #8b949e;
        font-size: 0.85rem;
        line-height: 1.6;
      }
      .no-map-box {
        background: rgba(139,148,158,0.08);
        border: 1px solid rgba(139,148,158,0.25);
        border-radius: 10px;
        padding: 14px 18px;
        color: #8b949e;
        font-size: 0.85rem;
      }
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

# Constants:
CATEGORY_COLOR_SCALES = {
    "Natural Gas":  "Blues",
    "Crude Oil":    "Oranges",
    "Petroleum":    "Reds",
    "Coal":         "Greys",
    "Electricity":  "Greens",
    "Total Energy": "Purples",
    "SEDS":         "Teal",
}

CATEGORY_BADGE = {
    "Natural Gas":  "badge-blue",
    "Crude Oil":    "badge-orange",
    "Petroleum":    "badge-red",
    "Coal":         "badge-grey",
    "Electricity":  "badge-green",
    "Total Energy": "badge-purple",
    "SEDS":         "badge-purple",
}

# Human-readable titles for breakdown columns across all schemas
BREAKDOWN_TITLES = {
    "origin_country": "Top Import Origins",
    "crude_grade":    "Crude Grade Mix",
    "process_id":     "Movement Type Breakdown",
    "product_id":     "Product Breakdown",
    "fuel_type_id":   "Fuel Type Breakdown",
    "fuel_prefix":    "Fuel Category Breakdown",
    "energy_source":  "Energy Source Breakdown",
    "sectorid":       "Sector Breakdown",
    "country_desc":   "Top Trade Partners",
    "series_code":    "Series Breakdown",
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#8b949e"),
    margin=dict(l=0, r=0, t=32, b=0),
)


# Helpers
def _eia_code_to_abbrev(name: str) -> str | None:
    return STATE_ABBREV.get(name.strip()) if isinstance(name, str) else None


def add_abbrev_col(df: pd.DataFrame) -> pd.DataFrame:
    """Add state_abbrev; rows that don't resolve to a US state are kept but
    abbrev will be None — they're filtered out only in choropleth rendering."""
    if "state" not in df.columns:
        return df
    df = df.copy()
    df["state_abbrev"] = df["state"].apply(_eia_code_to_abbrev)
    return df


def render_metric(col, label: str, value: str, help_text: str = "") -> None:
    help_icon = (
        f' <span title="{help_text}" style="cursor:help;color:#58a6ff;font-size:0.8rem;">&#9432;</span>'
        if help_text else ""
    )
    col.markdown(
        f"""
        <div style="background:rgba(22,27,34,0.85);border:1px solid #30363d;
                    border-radius:10px;padding:14px 18px;margin-bottom:8px;">
            <div style="color:#c9d1d9;font-size:0.72rem;font-weight:600;
                        text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">
                {label}{help_icon}
            </div>
            <div style="color:#e6edf3;font-size:1.35rem;font-weight:600;line-height:1.2;">
                {value}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _period_str(meta: dict, year: int, month: int | None, week: int | None) -> str:
    if meta["time_granularity"] == "monthly" and month:
        return f"{calendar.month_name[month]} {year}"
    if meta["time_granularity"] == "weekly" and week:
        return f"Week {week}, {year}"
    return str(year)


# Sidebar:
with st.sidebar:
    st.markdown("## ⚡ EIA Energy Explorer")
    st.markdown("---")

    categories: dict[str, list[str]] = defaultdict(list)
    for key, m in DATASETS.items():
        categories[m["category"]].append(key)

    selected_category = st.selectbox(
        "Energy Category",
        options=list(categories.keys()),
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
    prev_dataset_key = st.session_state.get("_active_dataset_key")
    if prev_dataset_key != selected_key:
        st.session_state["selected_state"] = None
    st.session_state["_active_dataset_key"] = selected_key

    st.markdown("---")

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

    selected_year = st.selectbox(
        "Year",
        options=sorted(years, reverse=True),
        index=0,
        key=f"year_select_{selected_key}",
    )

    selected_month = None
    selected_week  = None
    df_year = df_full[df_full["year"] == selected_year] if "year" in df_full.columns else df_full

    if meta["time_granularity"] == "monthly":
        months = get_months(df_year)
        month_labels = {calendar.month_abbr[m]: m for m in sorted(months, reverse=True)}
        if month_labels:
            selected_month = month_labels[st.selectbox(
                "Month",
                options=list(month_labels.keys()),
                index=0,
                key=f"month_select_{selected_key}",
            )]
    elif meta["time_granularity"] == "weekly":
        weeks = get_weeks(df_year)
        if weeks:
            selected_week = st.selectbox(
                "Week",
                options=sorted(weeks, reverse=True),
                index=0,
                key=f"week_select_{selected_key}",
            )

    st.markdown("---")

    # Metric selector — only numeric columns make sense here.
    # For wide-format datasets (seds_fuel_pivot) value_col is None;
    # we auto-discover numeric columns from the actual dataframe.
    value_col = meta.get("value_col")
    metric_options: dict[str, str] = {}
    if value_col and meta.get("value_label"):
        metric_options[meta["value_label"]] = value_col
    for col_k, col_l in meta["extra_cols"].items():
        if col_k in df_full.columns and pd.api.types.is_numeric_dtype(df_full[col_k]):
            metric_options[col_l] = col_k
    # Auto-discover for wide/pivot datasets with no declared value_col.
    # Use seds_col_label() to map raw column names (AV, CO, te_pct) to
    # human-readable names ("Aviation Gasoline", "Coal", "Total Energy (% of Total)").
    if not metric_options:
        skip = {"year", "month", "week", "state", "state_abbrev", "num_series"}
        is_seds = selected_key.startswith("seds")
        for col in df_full.columns:
            if col not in skip and pd.api.types.is_numeric_dtype(df_full[col]):
                label = seds_col_label(col) if is_seds else col.replace("_", " ").title()
                metric_options[label] = col

    if metric_options:
        selected_metric_label = st.selectbox(
            "Map Metric",
            options=list(metric_options.keys()),
            key=f"metric_select_{selected_key}",
        )
        selected_metric_col = metric_options[selected_metric_label]
    else:
        selected_metric_label = meta.get("value_label", "Value") or "Value"
        selected_metric_col   = value_col

    st.markdown("---")
    st.markdown(
        '<div class="info-box">💡 <strong>Hover</strong> over states for quick stats.<br>'
        '📌 <strong>Click</strong> a state to drill into detail.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("")
    if st.button("🔄 Clear state selection", use_container_width=True):
        st.session_state["selected_state"] = None
        st.session_state["_clear_requested"] = True


# Main header
period_label = _period_str(meta, selected_year, selected_month, selected_week)
badge_cls    = CATEGORY_BADGE.get(selected_category, "badge-blue")
no_map       = meta.get("no_map", False)

st.markdown(
    f'<h1>U.S. Energy Dashboard</h1>'
    f'<span class="badge {badge_cls}">{selected_category}</span> &nbsp;'
    f'<span style="color:#8b949e;font-size:0.9rem;">{meta["label"]} — {period_label}</span>',
    unsafe_allow_html=True,
)
st.markdown("")

# Filter to selected period 
df_map = filter_df(df_full, selected_key, selected_year, selected_month, selected_week)

color_scale = CATEGORY_COLOR_SCALES.get(selected_category, "Viridis")
unit        = meta["unit"]

# Decide whether we can render a choropleth.
# Conditions to render map:
#   1. Dataset does NOT have no_map=True, OR it has state data despite no_map
#      (seds_fuel_pivot has state col even though value_col=None)
#   2. selected_metric_col is set and present in df_map
#   3. The state column exists and has values that resolve to US abbreviations
_has_metric   = bool(selected_metric_col and selected_metric_col in df_map.columns
                     and pd.api.types.is_numeric_dtype(df_map[selected_metric_col]))
_has_state    = "state" in df_map.columns and df_map["state"].notna().any()
_can_map      = _has_metric and _has_state and not no_map

# Map or no-map layout
_state_active = bool(st.session_state.get("selected_state"))

if not _can_map:
    # No-map datasets (Coal, Total Energy, Petroleum PADD aggregate)
    if no_map:
        st.markdown(
            '<div class="no-map-box">🗺️ Choropleth map not available for this dataset — '
            'location dimension is not US state-level. See charts below.</div>',
            unsafe_allow_html=True,
        )
        st.markdown("")

    # Determine best dimension and value to chart.
    # Use state col if it exists, else breakdown_col.
    dim_col = (
        "state" if "state" in df_map.columns
        else meta.get("breakdown_col") or meta.get("state_col")
    )
    val_col = (
        selected_metric_col
        if selected_metric_col and selected_metric_col in df_map.columns
           and pd.api.types.is_numeric_dtype(df_map.get(selected_metric_col, pd.Series(dtype=float)))
        else (meta.get("value_col") or meta.get("breakdown_value"))
    )
    # Fallback: if dim_col not in df_map, try state_col raw name before rename
    if dim_col and dim_col not in df_map.columns:
        dim_col = meta.get("state_col")
    
    if dim_col and val_col and dim_col in df_map.columns and val_col in df_map.columns:
        top_n = (
            df_map.groupby(dim_col, as_index=False)[val_col]
            .sum()
            .nlargest(15, val_col)
        )
        fig_top = px.bar(
            top_n,
            x=val_col,
            y=dim_col,
            orientation="h",
            color=val_col,
            color_continuous_scale=color_scale,
            labels={val_col: f"{selected_metric_label} ({unit})", dim_col: ""},
            title=f"Top {dim_col.replace('_', ' ').title()} — {period_label}",
        )
        fig_top.update_layout(
            height=460,
            showlegend=False,
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
            **PLOTLY_LAYOUT,
        )
        st.plotly_chart(fig_top, use_container_width=True)
    else:
        st.info(f"No chart data available for '{selected_metric_label}' in {period_label}. "
                "Try selecting a different metric from the sidebar.")

else:
    # Choropleth map (state-level datasets):
    # agg_for_map aggregates to one numeric value per state for the choropleth.
    # df_map retains all columns for the detail panel / breakdown charts.
    df_map_agg = agg_for_map(df_map, selected_key, selected_metric_col)
    df_map_agg = add_abbrev_col(df_map_agg)
    df_map_states = df_map_agg[df_map_agg["state_abbrev"].notna()].copy()

    map_col, detail_col = st.columns([2, 1] if _state_active else [3, 1], gap="large")

    with map_col:
        if df_map_states.empty:
            st.warning("No state-level data to display on the map for this period.")
        else:
            fig_map = go.Figure(
                go.Choropleth(
                    locations=df_map_states["state_abbrev"],
                    z=df_map_states[selected_metric_col],
                    locationmode="USA-states",
                    colorscale=color_scale,
                    colorbar=dict(
                        title=dict(text=f"{selected_metric_label}<br>({unit})", side="right"),
                        tickfont=dict(color="#8b949e", size=11),
                        bgcolor="rgba(13,17,23,0.7)",
                        bordercolor="#30363d",
                        len=0.75,
                    ),
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        f"{selected_metric_label}: %{{z:,.1f}} {unit}"
                        "<extra></extra>"
                    ),
                    customdata=df_map_states[["state"]].values,
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
            selected_points = st.plotly_chart(
                fig_map,
                use_container_width=True,
                key="choropleth_map",
                on_select="rerun",
                selection_mode="points",
            )
            st.markdown(
                '<p style="color:#484f58;font-size:0.75rem;text-align:center;">'
                'Click any state to open the detail panel →</p>',
                unsafe_allow_html=True,
            )

    # Detail panel:
    with detail_col:
        clicked_abbrev: str | None = None
        try:
            if (
                selected_points
                and hasattr(selected_points, "selection")
                and selected_points.selection
                and selected_points.selection.get("points")
            ):
                pt = selected_points.selection["points"][0]
                clicked_abbrev = pt.get("location") or pt.get("customdata", [None])[0]
        except Exception:
            pass

        if "selected_state" not in st.session_state:
            st.session_state["selected_state"] = None
        clear_was_requested = st.session_state.pop("_clear_requested", False)

        if clicked_abbrev and not clear_was_requested:
            match = df_map_states[df_map_states["state_abbrev"] == clicked_abbrev]
            if not match.empty:
                new_state = match.iloc[0]["state"]
                if new_state != st.session_state.get("selected_state"):
                    st.session_state["selected_state"] = new_state
                    st.rerun()

        selected_state: str | None = st.session_state.get("selected_state")

        if not selected_state:
            st.markdown("### 📊 Top States")
            st.markdown(
                f'<p style="color:#8b949e;font-size:0.85rem;">Ranked by {selected_metric_label} ({period_label})</p>',
                unsafe_allow_html=True,
            )
            if not df_map_states.empty:
                top_n = df_map_states.nlargest(10, selected_metric_col)[["state", selected_metric_col]]
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
                    height=380, showlegend=False, coloraxis_showscale=False,
                    yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
                    xaxis=dict(tickfont=dict(size=10)),
                    **PLOTLY_LAYOUT,
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.markdown(f"### 📌 {selected_state}")
            st.markdown(f'<span class="badge {badge_cls}">{period_label}</span>', unsafe_allow_html=True)
            st.markdown("")

            state_row = df_map_states[df_map_states["state"] == selected_state]
            if not state_row.empty and selected_metric_col in state_row.columns:
                primary_val = state_row.iloc[0][selected_metric_col]
                cols_m = st.columns(2)
                render_metric(cols_m[0], selected_metric_label, f"{primary_val:,.1f}", unit)

                df_snap = df_full[df_full["year"] == selected_year].copy()
                if selected_month:
                    df_snap = df_snap[df_snap["month"] == selected_month]
                if selected_week:
                    df_snap = df_snap[df_snap["week"] == selected_week]
                if "state" in df_snap.columns:
                    df_snap = df_snap[df_snap["state"] == selected_state]

                extra_shown = 0
                is_seds_key = selected_key.startswith("seds")
                for col_key, col_label in meta["extra_cols"].items():
                    if col_key in df_snap.columns and not df_snap.empty:
                        if pd.api.types.is_numeric_dtype(df_snap[col_key]):
                            val = df_snap[col_key].sum()
                            disp_val = f"{val:,.1f}"
                        else:
                            val = df_snap[col_key].iloc[0]
                            disp_val = str(val)
                        # Use human-readable SEDS label when col_label is a raw code
                        if is_seds_key and len(col_label) <= 3 and col_label.isupper():
                            display_label = seds_col_label(col_label)
                        else:
                            display_label = col_label
                        label_short = display_label.split("(")[0].strip()
                        this_unit   = display_label.split("(")[-1].rstrip(")") if "(" in display_label else ""
                        render_metric(cols_m[(extra_shown + 1) % 2], label_short, disp_val, this_unit)
                        extra_shown += 1
            else:
                st.info(f"No data for {selected_state} in {period_label}.")

            st.markdown("")

            # Time series
            st.markdown(f"**{selected_metric_label} over time**")
            df_state_ts = df_full[df_full["state"] == selected_state].copy() if "state" in df_full.columns else pd.DataFrame()
            if not df_state_ts.empty and selected_metric_col in df_state_ts.columns:
                if meta["time_granularity"] == "monthly":
                    ts = df_state_ts.groupby("period", as_index=False)[selected_metric_col].sum().sort_values("period")
                    x_col = "period"
                elif meta["time_granularity"] == "weekly":
                    ts = df_state_ts.groupby("period", as_index=False)[selected_metric_col].sum().sort_values("period")
                    x_col = "period"
                else:
                    ts = df_state_ts.groupby("year", as_index=False)[selected_metric_col].sum().sort_values("year")
                    x_col = "year"
                fig_ts = px.line(
                    ts, x=x_col, y=selected_metric_col,
                    labels={selected_metric_col: f"{selected_metric_label} ({unit})", x_col: ""},
                    color_discrete_sequence=["#58a6ff"],
                )
                fig_ts.update_traces(line=dict(width=2.5), mode="lines+markers", marker=dict(size=4))
                fig_ts.update_layout(height=200, **PLOTLY_LAYOUT)
                st.plotly_chart(fig_ts, use_container_width=True)

            # Breakdown chart
            breakdown_col = meta.get("breakdown_col")
            breakdown_val = meta.get("breakdown_value")
            if breakdown_col and breakdown_val and "state" in df_full.columns:
                df_bd = df_full[df_full["state"] == selected_state].copy()
                df_bd = df_bd[df_bd["year"] == selected_year]
                if selected_month:
                    df_bd = df_bd[df_bd["month"] == selected_month]
                if selected_week:
                    df_bd = df_bd[df_bd["week"] == selected_week]

                if not df_bd.empty and breakdown_col in df_bd.columns and breakdown_val in df_bd.columns:
                    bd_agg = (
                        df_bd.groupby(breakdown_col, as_index=False)[breakdown_val]
                        .sum()
                        .sort_values(breakdown_val, ascending=False)
                        .head(10)
                    )
                    bd_title = BREAKDOWN_TITLES.get(breakdown_col, breakdown_col.replace("_", " ").title())
                    st.markdown(f"**{bd_title}**")
                    fig_bd = px.bar(
                        bd_agg, x=breakdown_val, y=breakdown_col, orientation="h",
                        color=breakdown_col,
                        color_discrete_sequence=px.colors.qualitative.Pastel,
                        labels={breakdown_val: unit, breakdown_col: ""},
                    )
                    fig_bd.update_layout(
                        height=260, showlegend=False,
                        yaxis=dict(autorange="reversed"),
                        **PLOTLY_LAYOUT,
                    )
                    st.plotly_chart(fig_bd, use_container_width=True)


# Bottom row: US trend + data table:
st.markdown("---")
trend_col, table_col = st.columns([3, 2], gap="large")

with trend_col:
    st.markdown("### 📈 US Total Over Time")
    # Use the currently selected metric so switching metrics updates the trend too.
    # Fall back to meta value_col if selected_metric_col is unavailable.
    trend_val_col = (
        selected_metric_col
        if selected_metric_col and selected_metric_col in df_full.columns
        else meta.get("value_col")
    )
    trend_label = selected_metric_label if trend_val_col == selected_metric_col else meta.get("value_label", "Value")

    if trend_val_col and trend_val_col in df_full.columns and pd.api.types.is_numeric_dtype(df_full[trend_val_col]):
        if meta["time_granularity"] in ("monthly", "weekly"):
            us_ts = df_full.groupby("period", as_index=False)[trend_val_col].sum().sort_values("period")
            x_col_us = "period"
        else:
            us_ts = df_full.groupby("year", as_index=False)[trend_val_col].sum().sort_values("year")
            x_col_us = "year"

        fig_us = px.area(
            us_ts, x=x_col_us, y=trend_val_col,
            labels={trend_val_col: f"{trend_label} ({unit})", x_col_us: ""},
            color_discrete_sequence=["#58a6ff"],
        )
        fig_us.update_traces(line=dict(width=2, color="#58a6ff"), fillcolor="rgba(88,166,255,0.12)")
        fig_us.update_layout(height=260, **PLOTLY_LAYOUT)
        st.plotly_chart(fig_us, use_container_width=True)
    else:
        st.info("No numeric value column available for US trend chart.")

with table_col:
    st.markdown("### 📋 Rankings")
    st.markdown(
        f'<p style="color:#8b949e;font-size:0.82rem;">All locations — {period_label}</p>',
        unsafe_allow_html=True,
    )
    # Determine best dimension col: prefer state, then breakdown_col, then state_col
    rank_col = (
        "state" if "state" in df_map.columns
        else (meta.get("breakdown_col") or meta.get("state_col"))
    )
    rank_val = (
        selected_metric_col
        if selected_metric_col and selected_metric_col in df_map.columns
           and pd.api.types.is_numeric_dtype(df_map[selected_metric_col])
        else (meta.get("value_col") or meta.get("breakdown_value"))
    )

    if rank_col and rank_val and rank_col in df_map.columns and rank_val in df_map.columns:
        rank_df = (
            df_map.groupby(rank_col, as_index=False)[rank_val].sum()
            .sort_values(rank_val, ascending=False)
            .reset_index(drop=True)
        )
        rank_df.index += 1
        rank_df.columns = ["Location", f"{selected_metric_label} ({unit})"]
        st.dataframe(rank_df, use_container_width=True, height=260)
    else:
        st.info("No ranking data available for current selection.")