"""Pipeline dashboard: a client of the FastAPI service and nothing more.

    uv run uvicorn app:app --app-dir services/api       # start the API first
    uv run streamlit run services/dashboard/app.py      # then this

Everything on the page arrives through the API over HTTP -- the dashboard never
opens a Parquet file -- so the API stays the one door to the data and the two
deploy as separate containers. Point it somewhere else with API_URL.
"""

import os

import altair as alt
import pandas as pd
import requests
import streamlit as st

API = os.environ.get("API_URL", "http://localhost:8000").rstrip("/")

# Categorical slots 1-3 of the dataviz reference palette, in its validated
# order, with each theme's own step: every check passes in both modes (worst
# colour-blind separation dE 9.2). Aqua sits under 3:1 on the light surface,
# so each chart has its data one click away as a table.
SERIES = {
    "light": {"actual": "#2a78d6", "forecast": "#eb6834", "persistence": "#1baf7a"},
    "dark": {"actual": "#3987e5", "forecast": "#d95926", "persistence": "#199e70"},
}


@st.cache_data(ttl=60, show_spinner=False)
def get(path: str, **params):
    r = requests.get(f"{API}{path}", params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def get_all(path: str, **params) -> pd.DataFrame:
    """Follow next_cursor to the last page."""
    rows, cursor = [], None
    while True:
        body = get(path, **params, **({"cursor": cursor} if cursor else {}))
        rows += body["data"]
        cursor = body["next_cursor"]
        if cursor is None:
            df = pd.DataFrame(rows)
            if "time" in df:
                df["time"] = pd.to_datetime(df["time"])
            return df


def hour(ts: str | None) -> str:
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "none yet"


def line_chart(df: pd.DataFrame, colors: dict[str, str], y_title: str, y_format: str) -> alt.LayerChart:
    """Lines on one y-axis, a crosshair and tooltip on hover, a legend when there is more than one."""
    names = list(colors)
    long = df.melt("time", value_vars=names, var_name="series", value_name="value")
    color = alt.Color("series:N", scale=alt.Scale(domain=names, range=list(colors.values())),
                      legend=alt.Legend(title=None, orient="top") if len(names) > 1 else None)
    base = alt.Chart(long).encode(
        x=alt.X("time:T", title=None),
        y=alt.Y("value:Q", title=y_title, axis=alt.Axis(format=y_format), scale=alt.Scale(zero=False)),
        color=color)
    hover = alt.selection_point(fields=["time"], nearest=True, on="pointerover", clear="pointerout", empty=False)
    tooltip = [alt.Tooltip("time:T", title="hour (UTC)", format="%Y-%m-%d %H:%M")] + [
        alt.Tooltip(f"{n}:Q", format=y_format) for n in names]
    return alt.layer(
        base.mark_line(strokeWidth=2),
        alt.Chart(df).mark_rule(strokeWidth=1, opacity=0.5).encode(x="time:T").transform_filter(hover),
        base.mark_point(size=64, filled=True).encode(opacity=alt.condition(hover, alt.value(1), alt.value(0))),
        # a transparent 14 px rule per hour: the hover target is wider than the line it tracks
        alt.Chart(df).mark_rule(strokeWidth=14, opacity=0).encode(x="time:T", tooltip=tooltip).add_params(hover),
    ).properties(height=320)


st.set_page_config(page_title="BTC-USD pipeline", layout="wide")
theme = getattr(getattr(st.context, "theme", None), "type", None)
colors = SERIES.get(theme or "light", SERIES["light"])

try:
    health = get("/health")
    symbols = [s["symbol"] for s in get("/symbols")]
except requests.RequestException:
    st.error(f"Can't reach the API at {API}.  \nStart it with `uv run uvicorn app:app --app-dir services/api`, "
             "or set API_URL to where it runs.")
    st.stop()

symbol = st.sidebar.selectbox("Symbol", symbols)
days = st.sidebar.slider("Days of history", min_value=3, max_value=60, value=14)
st.sidebar.caption(f"API: {API}")

st.title(f"{symbol} pipeline")
latest_tick = health.get("latest_tick")          # absent from an API older than this page
st.caption(f"Latest bar {hour(health['latest_bar'])} UTC · latest tick {hour(latest_tick)} UTC · "
           f"{health['tick_hours_published']} tick hours in the lake")

latest_bar = pd.Timestamp(health["latest_bar"])
if pd.Timestamp.now(tz="UTC") - latest_bar > pd.Timedelta(days=2):
    st.info(f"Bars end at {hour(health['latest_bar'])} UTC. Re-run `src/tracker/candles.py`, then "
            "`pipeline/build_warehouse.py` and `notebooks/volatility.py`, to bring them up to date.")

bars = get_all("/bars", symbol=symbol, start=(latest_bar - pd.Timedelta(days=days)).isoformat(), limit=1000)
fc = get("/forecast", symbol=symbol)
wf = fc["walk_forward"]

last, day_ago = bars["close"].iloc[-1], bars["close"].iloc[max(0, len(bars) - 25)]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Last close", f"${last:,.2f}", f"{last / day_ago - 1:+.2%} over 24h")
c2.metric("Volatility, last hour", f"{fc['vol_1h_last']:.3%}")
c3.metric(f"Forecast for {hour(fc['for_hour'])[11:]} UTC", f"{fc['vol_1h_forecast']:.3%}",
          f"{fc['vol_1h_forecast'] - fc['vol_1h_last']:+.3%} vs last hour", delta_color="off")
c4.metric("Model vs persistence", f"{wf['vs_persistence']:.1%} lower MAE",
          f"{wf['vs_mean_24h']:.1%} vs the 24h mean", delta_color="off")

st.subheader("Hourly close")
st.altair_chart(line_chart(bars[["time", "close"]], {"close": colors["actual"]}, "USD", "$,.0f"),
                width="stretch")
with st.expander("Bars as a table"):
    st.dataframe(bars.sort_values("time", ascending=False), hide_index=True, width="stretch")

st.subheader("Next-hour volatility: forecast against what happened")
st.caption("Out of sample: each week is predicted by a model trained only on the hours before it. "
           "Parkinson volatility, per hour.")
hist_end = pd.Timestamp(wf["test_end"])
hist = get_all("/forecast/history", symbol=symbol,
               start=(hist_end - pd.Timedelta(days=days)).isoformat(), limit=1000)
st.altair_chart(line_chart(hist[["time", "actual", "forecast", "persistence"]], colors, "per hour", ".2%"),
                width="stretch")
mae = {k: (hist[k] - hist["actual"]).abs().mean() for k in ("forecast", "persistence")}
st.caption(f"Over these {len(hist)} hours: model MAE {mae['forecast']:.4%}, persistence {mae['persistence']:.4%}. "
           f"Over all {wf['hours']:,} test hours: {wf['vs_persistence']:.1%} lower.")
with st.expander("Forecasts as a table"):
    st.dataframe(hist.sort_values("time", ascending=False), hide_index=True, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Data quality")
    q = get("/quality")
    if q["errors"]:
        st.error(f"{q['errors']} error finding(s) in {q['rows_checked']:,} bars")
    else:
        st.success(f"No errors in {q['rows_checked']:,} bars · {q['warnings']} warning(s)")
    if q["findings"]:
        st.dataframe(pd.DataFrame([{"check": f["check"], "kind": f["kind"], "rows": f["rows"],
                                    "what": f["detail"], "first at": ", ".join(t[:16] for t in f["first_times"][:3])}
                                   for f in q["findings"]]), hide_index=True, width="stretch")

with right:
    st.subheader("Latest ticks")
    if latest_tick:
        since = (pd.Timestamp(latest_tick) - pd.Timedelta(minutes=5)).isoformat()
        ticks = pd.DataFrame(get("/ticks", symbol=symbol, start=since, limit=1000)["data"])
        st.dataframe(ticks[["recv_time", "price", "last_size", "side", "best_bid", "best_ask"]].iloc[::-1].head(25),
                     hide_index=True, width="stretch")
    else:
        st.write("No tick hours in the lake yet. Run `pipeline/compact_ticks.py` after a capture.")
