#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


# In[9]:


# --- CONFIG ---
RIOT_API_KEY = "RGAPI-db8fafe7-e1dd-42fe-901b-3c35ac52538b"
PUUID = "ietVCTS7Tqi47nsRIhmJoIMtYhIT0rtlALufrc2o03sKfgyIvaWBIdKMS2YO17FqODtYSy010_-dxw"
S3_BUCKET = "hackathon-s3-rift-rewind-mohammad"
AWS_REGION = "us-east-1"
ROUTING = "asia"   # change if needed: americas, europe, asia, sea


# In[12]:


# --- S3 CLIENT ---
try:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    print("S3 client ready for bucket:", S3_BUCKET)
except ClientError as e:
    print("S3 client creation failed:", e)

def upload_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
    except ClientError as e:
        print("Upload failed:", e)
        raise SystemExit("Exiting: cannot continue without valid S3 client.")


# In[14]:


# --- TIME WINDOW (1 YEAR) ---
end_dt = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(days=365)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

print("Collecting matches from", start_dt.date(), "to", end_dt.date())


# In[16]:


# --- FETCH MATCH IDS (PAGING, NO BREAK) ---
headers = {"X-Riot-Token": RIOT_API_KEY}
all_ids = []
start = 0
count = 100
done = False

while not done:
    url = f"https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{PUUID}/ids"
    params = {"start": start, "count": count, "startTime": start_ts, "endTime": end_ts}
    r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code != 200:
        print("Error:", r.status_code, r.text[:100])
        done = True
        continue

    ids = r.json()
    if not ids:
        done = True
        continue

    all_ids.extend(ids)
    print(f"Fetched {len(ids)} IDs (total so far {len(all_ids)})")

    start += count
    time.sleep(0.3)

print("Total match IDs collected:", len(all_ids))


# In[17]:


# ====== SAVE & UPLOAD INDEX FILE ======
index_path = Path("data/raw/index/match_ids_year.json")
index_path.parent.mkdir(parents=True, exist_ok=True)
with open(index_path, "w") as f:
    json.dump(all_ids, f, indent=2)

upload_to_s3(str(index_path), "raw/index/match_ids_year.json")


# In[20]:


# ====== DOWNLOAD MATCHES ======
def fetch_match(matchId):
    url = f"https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/{matchId}"
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 429:
        # Simple backoff: wait then retry once
        print("Rate limit (429). Waiting 20s, then retry:", matchId)
        time.sleep(20)
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            return r.json()
        else:
            print("Still failing after backoff:", matchId, r.status_code)
            return None
    else:
        print("Match failed:", matchId, r.status_code)
        return None

total = len(all_ids)
print(f"Total match IDs to download: {total}")

success = 0
failed = 0

# define 4 progress checkpoints (25%, 50%, 75%, 100%)
checkpoints = {int(total * 0.25), int(total * 0.5), int(total * 0.75), total}

for i, matchId in enumerate(all_ids, start=1):
    #skip if file already exists locally ---
    local_path = Path(f"data/raw/matches/{matchId}.json")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        print(f"[{i}/{total}] Skipping (already downloaded): {matchId}")
        continue

    # --- Download if not skipped ---
    data = fetch_match(matchId)
    if not data:
        print(f"[{i}/{total}] Failed: {matchId}")
        failed += 1
        continue

    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    s3_key = f"raw/matches/{matchId}.json"
    upload_to_s3(str(local_path), s3_key)
    success += 1

    # Print only at key checkpoints
    if i in checkpoints:
        percent = round(i / total * 100)
        print(f"Progress: {percent}% ({i}/{total} matches done)")

    time.sleep(0.5)

print(f"All done! Uploaded {success} matches, failed {failed}.")


# In[22]:


# ====== CREATE MATCH-LEVEL TABLE ======
rows1 = []

# go through every raw match json
for f in Path("data/raw/matches").glob("*.json"):
    with open(f) as fp:
        match = json.load(fp)

    info = match["info"]
    meta = match["metadata"]
    teams = info["teams"]

    # some matches might not have both teams
    blue = teams[0] if len(teams) > 0 else {}
    red = teams[1] if len(teams) > 1 else {}

    rows1.append({
        "matchId": meta.get("matchId"),
        "gameCreation": info.get("gameCreation"),
        "gameStartTimestamp": info.get("gameStartTimestamp"),
        "gameEndTimestamp": info.get("gameEndTimestamp"),
        "gameDuration": info.get("gameDuration"),
        "endOfGameResult": info.get("endOfGameResult"),
        "gameMode": info.get("gameMode"),
        "gameType": info.get("gameType"),
        "gameVersion": info.get("gameVersion"),
        "queueId": info.get("queueId"),
        "mapId": info.get("mapId"),
        "platformId": info.get("platformId"),
        "tournamentCode": info.get("tournamentCode"),
        "blueWin": blue.get("win"),
        "redWin": red.get("win")
    })

df_matches = pd.DataFrame(rows1)
print("Total matches:", len(df_matches))

# save locally
out_path = Path("data/staged/matches.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)
df_matches.to_csv(out_path, index=False)
print("Saved:", out_path)

# upload to S3
s3_key = "staged/matches.csv"
upload_to_s3(str(out_path), s3_key)
print(f"Uploaded to S3 path: s3://{S3_BUCKET}/{s3_key}")


# In[24]:


try:
    matches_parquet = Path("data/staged/matches.parquet")
    df_matches.to_parquet(matches_parquet, index=False)
    upload_to_s3(str(matches_parquet), "staged/matches.parquet")
    print("Saved & uploaded:", matches_parquet)
except Exception as e:
    print("Parquet save skipped for matches:", e)


# In[48]:


rows = []
for f in Path("data/raw/matches").glob("*.json"):
    with open(f) as fp:
        m = json.load(fp)

    meta = m.get("metadata", {})
    info = m.get("info", {})
    participants = info.get("participants", [])

    for p in participants:
        rows.append({
            # context
            "matchId": meta.get("matchId"),
            "gameVersion": info.get("gameVersion"),
            "queueId": info.get("queueId"),
            "gameDuration": info.get("gameDuration"),  # seconds
            "championName": p.get("championName"),
            "teamId": p.get("teamId"),
            "win": p.get("win"),
            "winCount": 1 if p.get("win") else 0,
            "loseCount": 0 if p.get("win") else 1,

            # core stats
            "kills": p.get("kills"),
            "deaths": p.get("deaths"),
            "assists": p.get("assists"),
            "goldEarned": p.get("goldEarned"),
            "goldSpent": p.get("goldSpent"),
            "totalDamageDealtToChampions": p.get("totalDamageDealtToChampions"),
            "totalDamageTaken": p.get("totalDamageTaken"),
            "totalMinionsKilled": p.get("totalMinionsKilled"),
            "neutralMinionsKilled": p.get("neutralMinionsKilled"),
            "visionScore": p.get("visionScore"),
            "wardsPlaced": p.get("wardsPlaced"),
            "wardsKilled": p.get("wardsKilled"),
            "timeCCingOthers": p.get("timeCCingOthers"),
            "totalHeal": p.get("totalHeal"),
            "totalTimeSpentDead": p.get("totalTimeSpentDead"),
            "killingSprees": p.get("killingSprees"),
            "damageDealtToObjectives": p.get("damageDealtToObjectives"),
            "turretTakedowns": p.get("turretTakedowns"),
            "inhibitorTakedowns": p.get("inhibitorTakedowns"),
            "champExperience": p.get("champExperience"),
            "timePlayed": p.get("timePlayed")  # seconds
        })

df = pd.DataFrame(rows)
print("Rows:", len(df), "Cols:", len(df.columns))

# --- simple features (safe divisions) ---
denom_deaths = df["deaths"].replace(0, 1)
denom_time_min = (df["timePlayed"].replace(0, 1) / 60)

df["KDA_ratio"]      = (df["kills"] + df["assists"]) / denom_deaths
df["CS_per_min"]     = df["totalMinionsKilled"] / denom_time_min
df["Gold_efficiency"]= df["goldSpent"] / df["goldEarned"].replace(0, 1)
df["DMG_Gold_ratio"] = df["totalDamageDealtToChampions"] / df["goldEarned"].replace(0, 1)
df["Vision_per_min"] = df["visionScore"] / denom_time_min
df["DMG_per_min"]    = df["totalDamageDealtToChampions"] / denom_time_min
df["CS_per_game"] = df["totalMinionsKilled"] + df["neutralMinionsKilled"]

# --- choose order for the CSV (context → stats → features) ---
cols = [
    "matchId","gameVersion","queueId","gameDuration","championName","teamId","win","winCount", "loseCount",
    "kills","deaths","assists","goldEarned","goldSpent",
    "totalDamageDealtToChampions","totalDamageTaken",
    "totalMinionsKilled","neutralMinionsKilled",
    "visionScore","wardsPlaced","wardsKilled",
    "timeCCingOthers","totalHeal","totalTimeSpentDead","killingSprees",
    "damageDealtToObjectives","turretTakedowns","inhibitorTakedowns",
    "champExperience","timePlayed",
    "KDA_ratio","CS_per_min","CS_per_game","Gold_efficiency","DMG_Gold_ratio","Vision_per_min","DMG_per_min"
]
df_tidy = df[cols].copy()

# --- save locally + upload to analytics/ ---
out_path = Path("data/processed/tidy_participants.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)
df_tidy.to_csv(out_path, index=False)
print("Saved:", out_path)

s3_key = "analytics/tidy_participants.csv"
upload_to_s3(str(out_path), s3_key)
print(f"Uploaded to S3 path: s3://{S3_BUCKET}/{s3_key}")


# In[50]:


# ====== ENHANCED CHAMPION SUMMARY TABLE ======
champ_summary = (
    df_tidy.groupby("championName", as_index=False)
    .agg({
        "winCount": "sum",
        "loseCount": "sum",
        "kills": "mean",
        "deaths": "mean",
        "assists": "mean",
        "goldEarned": "mean",
        "goldSpent": "mean",
        "KDA_ratio": "mean",
        "CS_per_min": "mean",
        "DMG_per_min": "mean",
        "Vision_per_min": "mean",
        "Gold_efficiency": "mean",
        "DMG_Gold_ratio": "mean",
        "CS_per_game": "mean",
        "totalDamageDealtToChampions": "mean",
        "totalDamageTaken": "mean",
        "totalHeal": "mean",
        "timeCCingOthers": "mean"
    })
)

# basic totals
champ_summary["totalGames"] = champ_summary["winCount"] + champ_summary["loseCount"]
champ_summary["winRate"] = 100 * champ_summary["winCount"] / champ_summary["totalGames"]

# ---- new advanced columns ----
# Popularity (how often this champion appears compared to all)
total_games_all = champ_summary["totalGames"].sum()
champ_summary["popularity_%"] = 100 * champ_summary["totalGames"] / total_games_all

# KDA_performance = kills + assists - deaths (simple contribution indicator)
champ_summary["KDA_performance"] = (
    champ_summary["kills"] + champ_summary["assists"] - champ_summary["deaths"]
)

# Damage Efficiency = Damage Dealt per Death
champ_summary["DMG_per_death"] = (
    champ_summary["totalDamageDealtToChampions"] / champ_summary["deaths"].replace(0, 1)
)

# Tankiness = Damage Taken per Death
champ_summary["DMG_taken_per_death"] = (
    champ_summary["totalDamageTaken"] / champ_summary["deaths"].replace(0, 1)
)

# Healing per Minute
champ_summary["Heal_per_min"] = champ_summary["totalHeal"] / champ_summary["DMG_per_min"].replace(0, 1)

# Crowd Control per Minute (utility)
champ_summary["CC_per_min"] = champ_summary["timeCCingOthers"] / champ_summary["DMG_per_min"].replace(0, 1)

# ---- save table ----
out_path = Path("data/processed/champion_summary.csv")
champ_summary.to_csv(out_path, index=False)
print("Saved enhanced champion summary:", out_path)

upload_to_s3(str(out_path), "analytics/champion_summary.csv")
print(f"Uploaded to S3 path: s3://{S3_BUCKET}/analytics/champion_summary.csv")


# In[ ]:




