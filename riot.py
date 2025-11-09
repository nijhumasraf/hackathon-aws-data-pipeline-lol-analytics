#!/usr/bin/env python
# coding: utf-8

# In[107]:


import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


# In[109]:


# --- CONFIG ---
RIOT_API_KEY = "RGAPI-e0b5bf3f-4ef3-4f55-b791-1e0a9e6fc472"
PUUID = "ietVCTS7Tqi47nsRIhmJoIMtYhIT0rtlALufrc2o03sKfgyIvaWBIdKMS2YO17FqODtYSy010_-dxw"
S3_BUCKET = "hackathon-s3-rift-rewind-mohammad"
AWS_REGION = "us-east-1"
ROUTING = "asia"   # change if needed: americas, europe, asia, sea


# In[111]:


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


# In[113]:


# --- TIME WINDOW (1 YEAR) ---
end_dt = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(days=365)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

print("Collecting matches from", start_dt.date(), "to", end_dt.date())


# In[115]:


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


# In[117]:


# ====== SAVE & UPLOAD INDEX FILE ======
index_path = Path("data/raw/index/match_ids_year.json")
index_path.parent.mkdir(parents=True, exist_ok=True)
with open(index_path, "w") as f:
    json.dump(all_ids, f, indent=2)

upload_to_s3(str(index_path), "raw/index/match_ids_year.json")


# In[119]:


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


# In[121]:


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


# In[123]:


try:
    matches_parquet = Path("data/staged/matches.parquet")
    df_matches.to_parquet(matches_parquet, index=False)
    upload_to_s3(str(matches_parquet), "staged/matches.parquet")
    print("Saved & uploaded:", matches_parquet)
except Exception as e:
    print("Parquet save skipped for matches:", e)


# In[125]:


#Transform
raw_dir = Path("data/raw/matches")
files = list(raw_dir.glob("*.json"))

rows = []
for f in files:
    with open(f) as fp:
        match = json.load(fp)
    for p in match["info"]["participants"]:
        rows.append(p)

df = pd.DataFrame(rows)
print("Rows:", len(df), "Cols:", len(df.columns))


# In[127]:


df.columns.tolist()


# In[129]:


# Non-predictive columns (IDs, names, positions)
non_predictive = [
    "puuid", "summonerId", "summonerName", "riotIdGameName", "riotIdTagline",
    "participantId", "teamId", "championId", "championName",
    "individualPosition", "teamPosition", "role", "lane"
]

# Add new calculated (made-up) features
df["KDA_ratio"] = (df["kills"] + df["assists"]) / df["deaths"].replace(0, 1)
df["CS_per_min"] = df["totalMinionsKilled"] / (df["timePlayed"] / 60)
df["Gold_efficiency"] = df["goldSpent"] / df["goldEarned"]
df["DMG_Gold_ratio"] = df["totalDamageDealtToChampions"] / df["goldEarned"]
df["Vision_per_min"] = df["visionScore"] / (df["timePlayed"] / 60)

# Top 20 important metrics
top20 = [
    "kills","deaths","assists","goldEarned","totalDamageDealtToChampions",
    "totalDamageTaken","visionScore","wardsPlaced","wardsKilled",
    "champExperience","goldSpent","neutralMinionsKilled","totalMinionsKilled",
    "damageDealtToObjectives","turretTakedowns","inhibitorTakedowns",
    "timeCCingOthers","totalHeal","totalTimeSpentDead","killingSprees"
]

# Add made-up features list
madeup = ["KDA_ratio","CS_per_min","Gold_efficiency","DMG_Gold_ratio","Vision_per_min"]

# Human-readable: non-predictive + top20 + made-up
df_full = df[non_predictive + top20 + madeup].copy()

# Machine-readable: only top20 + made-up
df_tidy = df[top20 + madeup].copy()


# In[131]:


df_full.columns.tolist()


# In[133]:


df_tidy.columns.tolist()


# In[135]:


# === Save tidy data locally ===
out_path = Path("data/processed/tidy_participants.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)
df_tidy.to_csv(out_path, index=False)
print("Saved:", out_path)

# === Upload tidy data to S3 ===
s3_key = "analytics/tidy_participants.csv"  # analytics = final cleaned data zone
upload_to_s3(str(out_path), s3_key)
print(f"Uploaded to S3 path: s3://{S3_BUCKET}/{s3_key}")


# In[137]:


try:
    pqt = Path("data/staged/participants.parquet")
    pqt.parent.mkdir(parents=True, exist_ok=True)
    df_tidy.to_parquet(pqt, index=False)
    upload_to_s3(str(pqt), "staged/participants.parquet")
    print("Saved & uploaded:", pqt)
except Exception as e:
    print("Parquet save skipped for participants:", e)


# In[139]:


# === Save human-readable data (non_predictive + top20 + made-up features) ===
out_path_full = Path("data/processed/participants_human.csv")
out_path_full.parent.mkdir(parents=True, exist_ok=True)
df_full.to_csv(out_path_full, index=False)
print("Saved:", out_path_full)

# === Upload human-readable data to S3 ===
s3_key_full = "analytics/participants_human.csv"
upload_to_s3(str(out_path_full), s3_key_full)
print(f"Uploaded to S3 path: s3://{S3_BUCKET}/{s3_key_full}")


# In[ ]:




