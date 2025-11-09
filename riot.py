#!/usr/bin/env python
# coding: utf-8

# In[11]:


import requests
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


# In[21]:


# --- CONFIG ---
RIOT_API_KEY = "RGAPI-e0b5bf3f-4ef3-4f55-b791-1e0a9e6fc472"
PUUID = "ietVCTS7Tqi47nsRIhmJoIMtYhIT0rtlALufrc2o03sKfgyIvaWBIdKMS2YO17FqODtYSy010_-dxw"
S3_BUCKET = "hackathon-s3-rift-rewind-mohammad"
AWS_REGION = "us-east-1"
ROUTING = "asia"   # change if needed: americas, europe, asia, sea


# In[23]:


# --- S3 CLIENT ---
try:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    print("S3 client ready for bucket:", S3_BUCKET)
except ClientError as e:
    print("S3 client creation failed:", e)

def upload_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"Uploaded {local_path} -> s3://{S3_BUCKET}/{s3_key}")
    except ClientError as e:
        print("Upload failed:", e)


# In[25]:


# --- TIME WINDOW (1 YEAR) ---
end_dt = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(days=365)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

print("Collecting matches from", start_dt.date(), "to", end_dt.date())


# In[27]:


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


# In[29]:


# ====== SAVE & UPLOAD INDEX FILE ======
index_path = Path("data/raw/index/match_ids_year.json")
index_path.parent.mkdir(parents=True, exist_ok=True)
with open(index_path, "w") as f:
    json.dump(all_ids, f, indent=2)

upload_to_s3(str(index_path), "raw/index/match_ids_year.json")


# In[31]:


# ====== DOWNLOAD SAMPLE MATCHES ======
def fetch_match(matchId):
    url = f"https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/{matchId}"
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code == 200:
        return r.json()
    else:
        print("Match failed:", matchId, r.status_code)
        return None

sample_matches = all_ids[:5]  # only first few for testing

for matchId in sample_matches:
    data = fetch_match(matchId)
    if not data:
        continue

    local_path = Path(f"data/raw/matches/{matchId}.json")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    s3_key = f"raw/matches/{matchId}.json"
    upload_to_s3(str(local_path), s3_key)
    time.sleep(0.5)

print("Sample upload complete. Total sample files:", len(sample_matches))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




