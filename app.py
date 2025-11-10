# app.py
"""
Streamlit app for hackathon-aws-data-pipeline-lol-analytics
Features:
 - load local match / player CSV or parquet
 - OR fetch matches live using functions from riot.py (if available)
 - basic EDA: counts, winrate, time series, champion frequencies
 - per-match view with key stats and quick coaching suggestions
"""

from typing import Optional
import streamlit as st
import pandas as pd
import numpy as np
import importlib
import os
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="LoL Coach — Hackathon Demo", layout="wide")

# -------------------------
# Helpers
# -------------------------
@st.cache_data
def load_local_file(file: Path) -> pd.DataFrame:
    ext = file.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(file)
    if ext in [".parquet", ".pqt"]:
        return pd.read_parquet(file)
    # fallback attempt
    return pd.read_csv(file, low_memory=False)

def try_import_riot_module():
    try:
        import riot  # if your file is riot.py in repo, Python module name is riot
        return riot
    except Exception:
        try:
            # sometimes your file is named riot.py but package name differs
            mod = importlib.import_module("riot")
            return mod
        except Exception:
            return None

def summarize_matches(df: pd.DataFrame) -> dict:
    summary = {}
    # Make best-effort flexible column names
    col_win = None
    for c in df.columns:
        if c.lower() in ("win","is_win","victory","result"):
            col_win = c
            break
    if col_win is None:
        # try to infer from 'result' containing 'Win'/'Loss'
        summary['win_rate'] = None
    else:
        wins = df[col_win].map(lambda v: 1 if str(v).lower() in ("true","1","win","victory") else 0)
        summary['win_rate'] = wins.mean() if len(wins)>0 else None

    # Champion frequency
    champ_col = next((c for c in df.columns if "champ" in c.lower()), None)
    if champ_col:
        summary['champ_freq'] = df[champ_col].value_counts().head(10)
    else:
        summary['champ_freq'] = None

    # KDA if present
    k_cols = [c for c in df.columns if any(x in c.lower() for x in ("kill","assist","death"))]
    summary['has_kda'] = len(k_cols) >= 3
    return summary

# -------------------------
# Sidebar
# -------------------------
st.sidebar.title("LoL Coach — Controls")
data_source = st.sidebar.radio("Data source", ["Local file / dataset", "Fetch live via riot.py (PUUID)"])

df = None

if data_source == "Local file / dataset":
    uploaded = st.sidebar.file_uploader("Upload CSV or Parquet (or select sample)", type=["csv","parquet","pqt","zip"])
    use_sample = st.sidebar.checkbox("Use sample demo data (if no upload)", value=True)
    if uploaded:
        # save to temp and load
        tmp_path = Path("tmp_uploaded_data")
        tmp_path.mkdir(exist_ok=True)
        fpath = tmp_path / uploaded.name
        with open(fpath, "wb") as f:
            f.write(uploaded.getbuffer())
        df = load_local_file(fpath)
    elif use_sample:
        # small demo synthetic dataset if repo has none
        st.sidebar.write("Using generated demo dataset.")
        # make small synthetic dataframe
        np.random.seed(1)
        n = 200
        df = pd.DataFrame({
            "match_id": np.arange(n),
            "date": pd.date_range(end=pd.Timestamp.now(), periods=n).astype(str),
            "champion": np.random.choice(["Ahri","Yasuo","Lee Sin","Ezreal","Lux"], size=n),
            "kills": np.random.poisson(4, n),
            "deaths": np.random.poisson(3, n),
            "assists": np.random.poisson(5, n),
            "win": np.random.choice([True, False], n, p=[0.52,0.48]),
            "gold": np.random.normal(11000, 1500, n).astype(int)
        })

else:
    st.sidebar.write("Live fetch selected.")
    riot_mod = try_import_riot_module()
    if riot_mod is None:
        st.sidebar.error("Could not import riot.py from the repo. Make sure riot.py exists and defines functions to fetch matches.")
    else:
        st.sidebar.success("riot.py imported.")
    puuid = st.sidebar.text_input("Player PUUID (or leave blank to use saved)", value=os.getenv("PUUID",""))
    riot_key = st.sidebar.text_input("Riot API key (will not be saved by app)", type="password", value=os.getenv("RIOT_API_KEY",""))
    fetch_n = st.sidebar.slider("Num matches to fetch",  min_value=20, max_value=500, value=100, step=20)
    if st.sidebar.button("Fetch matches"):
        if riot_mod is None:
            st.sidebar.error("riot.py not found. Can't fetch.")
        elif not puuid or not riot_key:
            st.sidebar.error("Provide PUUID and API key.")
        else:
            # assume riot_mod has function fetch_matches(puuid, api_key, count)
            try:
                st.sidebar.info("Requesting matches... this may take a moment.")
                matches = riot_mod.fetch_matches(puuid, riot_key, count=fetch_n)
                # expect matches as list of dicts or DataFrame
                if isinstance(matches, pd.DataFrame):
                    df = matches
                else:
                    df = pd.DataFrame(matches)
                st.sidebar.success(f"Fetched {len(df)} matches.")
            except Exception as e:
                st.sidebar.error(f"Error fetching: {e}")

# -------------------------
# Main layout
# -------------------------
st.title("LoL Coach — Hackathon Demo app")
st.markdown("Quick demo UI for your hackathon analytics project. Use the sidebar to upload data or fetch live matches.")

if df is None:
    st.info("No dataset loaded yet. Upload a CSV/Parquet or use the demo data from the sidebar.")
else:
    st.subheader("Dataset preview")
    st.dataframe(df.head(50), use_container_width=True)

    # smart infer date column
    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    if date_col:
        try:
            df[date_col] = pd.to_datetime(df[date_col])
        except Exception:
            pass

    # Summary cards in three columns
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Matches", len(df))
    with col2:
        summary = summarize_matches(df)
        wr = summary.get("win_rate")
        st.metric("Win rate", f"{(wr*100):.1f}%" if wr is not None else "N/A")
    with col3:
        avg_kills = df["kills"].mean() if "kills" in df.columns else None
        st.metric("Avg kills", f"{avg_kills:.2f}" if avg_kills is not None else "N/A")

    # Champion frequency plot
    champ_col = next((c for c in df.columns if "champ" in c.lower()), None)
    if champ_col:
        champ_counts = (
        df[champ_col]
        .value_counts(dropna=False)
        .rename_axis("champion")     # name the index -> column after reset_index
        .reset_index(name="count")
    )
    fig = px.bar(champ_counts, x="champion", y="count", title="Top champions")
    st.plotly_chart(fig, use_container_width=True)
    # Win rate over time if date present
    if date_col:
        tmp = df.copy()
        # need a boolean win col
        win_col = next((c for c in df.columns if c.lower() in ("win","is_win","victory")), None)
        if win_col:
            tmp["win_flag"] = tmp[win_col].map(lambda v: 1 if str(v).lower() in ("true","1","win","victory") else 0)
            ts = tmp.set_index(date_col).resample("7D").win_flag.mean().reset_index()
            fig2 = px.line(ts, x=date_col, y="win_flag", title="Win rate (7-day rolling bins)")
            st.plotly_chart(fig2, use_container_width=True)

    # Simple KDA scatter if columns exist
    if all(x in df.columns for x in ("kills","deaths","assists")):
        df["kda"] = (df["kills"] + df["assists"]) / df["deaths"].replace(0,1)
        fig3 = px.scatter(df, x="kda", y="gold" if "gold" in df.columns else "kills",
                          hover_data=["match_id"] if "match_id" in df.columns else None,
                          title="KDA vs gold/kills")
        st.plotly_chart(fig3, use_container_width=True)

    # Match list + detail
    st.subheader("Match list")
    if "match_id" in df.columns:
        match_ids = df["match_id"].astype(str).tolist()
        sel = st.selectbox("Select a match", ["-- pick --"] + match_ids)
        if sel and sel != "-- pick --":
            m = df[df["match_id"].astype(str) == sel].iloc[0]
            st.markdown("### Match details")
            # pretty table of important stats
            stats = {k: m[k] for k in m.index if k not in ("raw_json",)}
            st.json(stats)

            # coaching suggestions (very simple rule-based)
            st.markdown("**Quick coaching hints**")
            hints = []
            if "deaths" in m and m["deaths"] >= 6:
                hints.append("High deaths — play safer in teamfights, ward more, avoid 1v1s when behind.")
            if "kills" in m and m["kills"] >= 8:
                hints.append("Good aggression — look to turn kills into objectives (towers, dragons).")
            if "gold" in m and "minions" in m.index and m["minions"] < 60:
                hints.append("CS is low — focus on last-hitting and wave management.")
            if not hints:
                hints.append("No automated tips for this match — add custom logic in riot.py or here.")
            for h in hints:
                st.write("- " + h)
    else:
        st.write("No `match_id` column found; add one to enable per-match view.")

    # Export cleaned dataset
    st.sidebar.subheader("Export / Save")
    if st.sidebar.button("Download cleaned CSV"):
        csv = df.to_csv(index=False)
        st.sidebar.download_button("Download CSV", data=csv, file_name="cleaned_matches.csv", mime="text/csv")

# Footer / notes
st.markdown("---")
st.markdown("**Notes:** This is a demo Streamlit UI. To integrate with your riot.py, ensure riot.py provides a `fetch_matches(puuid, api_key, count)` function that returns a list of dicts or a DataFrame. Keep API keys out of VCS (use .env or secrets).")
