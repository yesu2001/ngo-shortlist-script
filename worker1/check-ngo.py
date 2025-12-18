#!/usr/bin/env python3
"""
NGO shortlister (robust multi-source research + Gemini light fallback)

Features:
- Primary: Google Custom Search (limited free quota)
- Fallbacks: DuckDuckGo HTML search, NewsData API, Wikipedia lookup,
  Wayback Machine, direct website scraping
- Final fallback: Google Gemini (light research) to determine ACTIVE / INACTIVE
- SQLite caching to avoid re-checking processed NGOs
- Excel outputs for shortlisted & excluded
- Configurable START_INDEX and BATCH_SIZE for resuming batches

REQUIRED PACKAGES:
pip install requests pandas beautifulsoup4 google-genai openpyxl wikipedia
(If using newsdata API: no extra package needed)
"""

import os
import json
import time
import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from google import genai
import wikipedia  

# -----------------------------
# CONFIG - set your keys here
# -----------------------------
# The GEMINI_MODEL below is used as a suggestion; the script will try to auto-select a usable model too.


GEMINI_API_KEY = "AIzaSyDcC3ai6vSUvBd8eW8F9ZlbgDpP1bbhEiE"     # used by google-genai client
GOOGLE_API_KEY = "AIzaSyCO9aEjMNsPOlUHrcSUGenq0PZilcJvPug"
CUSTOM_SEARCH_ENGINE_ID = "5545960a53a594daa"
# GEMINI_MODEL = "gemini-1.5-pro"  
GEMINI_MODEL = "gemini-2.5-flash"

NEWSDATA_API_KEY = "pub_beb70c97d12c44118d0803a81f0c0b5b"  # Optional

# CONFIDENCE_THRESHOLD = 60

# DAILY_LIMIT = 50             # Max NGOs to process per run
# MAX_SEARCH_RESULTS = 6       # Preserve quota
# TOTAL_API_QUOTA = 100        # Max Google search requests per day (free tier)

# client = genai.Client(api_key="AIzaSyDcC3ai6vSUvBd8eW8F9ZlbgDpP1bbhEiE")

# Batch control: By default skip first 50 (already processed) and process next 1000
START_INDEX = 1100
BATCH_SIZE = 5000

# Local limits
TOTAL_GOOGLE_QUOTA = 100   # free Google CSE queries/day
MAX_SEARCH_RESULTS = 6
DAILY_PROCESS_LIMIT = None  # set to integer to limit processed NGOs in one run, or None

# Output files
SHORTLISTED_FILE = "Shortlisted_NGOs_Part2.xlsx"
EXCLUDED_FILE = "Excluded_NGOs_Part2.xlsx"

# -----------------------------
# Globals & clients
# -----------------------------
google_api_used = 0
client = genai.Client(api_key=GEMINI_API_KEY)

# SQLite cache
CACHE_DB = "ngo_cache.db"
conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS cache (
    name TEXT,
    location TEXT,
    result_json TEXT,
    PRIMARY KEY (name, location)
)
""")
conn.commit()

# -----------------------------
# Utility: Cache helpers
# -----------------------------
def cache_get(name, location):
    cur.execute("SELECT result_json FROM cache WHERE name=? AND location=?", (name, location))
    row = cur.fetchone()
    if row:
        try:
            return json.loads(row[0])
        except Exception:
            return row[0]
    return None

def cache_save(name, location, result):
    # result should be JSON-serializable or already a string
    payload = result if isinstance(result, str) else json.dumps(result)
    cur.execute("INSERT OR REPLACE INTO cache (name, location, result_json) VALUES (?, ?, ?)",
                (name, location, payload))
    conn.commit()

# -----------------------------
# Search & scrape helpers
# -----------------------------
def google_search(query):
    """Return dict (Google CSE JSON) or {} on failure or if quota exceeded."""
    global google_api_used
    if google_api_used >= TOTAL_GOOGLE_QUOTA:
        print("[WARN] Google API quota reached. Skipping Google searches.")
        return {}

    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": CUSTOM_SEARCH_ENGINE_ID,
        "q": query,
        "num": MAX_SEARCH_RESULTS,
    }
    try:
        r = requests.get(url, params=params, timeout=12)
        if r.status_code == 200:
            google_api_used += 1
            return r.json()
        else:
            print(f"[WARN] Google search returned status {r.status_code}")
            google_api_used += 1  # still count attempt to avoid excess calls
            return {}
    except Exception as e:
        print("[WARN] google_search error:", e)
        return {}

def duckduckgo_search(query, max_results=6):
    try:
        q = query.replace(" ", "+")
        url = f"https://duckduckgo.com/html/?q={q}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.select(".result__a")[:max_results]:
            href = a.get('href')
            if href:
                links.append(href)
        return links
    except Exception as e:
        return []

def newsdata_lookup(name, max_items=5):
    if not NEWSDATA_API_KEY:
        return []
    try:
        url = "https://newsdata.io/api/1/news"
        params = {"apikey": NEWSDATA_API_KEY, "q": name, "language": "en", "page": 1}
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        return data.get("results", [])[:max_items]
    except Exception:
        return []

def wikipedia_lookup(name):
    """Return page summary or empty string."""
    try:
        # try direct page
        page = wikipedia.page(name, auto_suggest=True)
        return {"title": page.title, "summary": wikipedia.summary(name, sentences=3), "url": page.url}
    except Exception:
        # try search and pick first match
        try:
            results = wikipedia.search(name, results=3)
            if results:
                title = results[0]
                return {"title": title, "summary": wikipedia.summary(title, sentences=3), "url": wikipedia.page(title).url}
        except Exception:
            return {}
    return {}

def wayback_lookup(url):
    try:
        api = f"http://archive.org/wayback/available?url={url}"
        r = requests.get(api, timeout=8)
        return r.json()
    except Exception:
        return {}

def scrape_website(url, max_chars=4000):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        return text[:max_chars]
    except Exception:
        return ""

# -----------------------------
# Gemini model selection helper
# -----------------------------
def choose_gemini_model(preferred_model=None):
    """
    Try preferred_model first; otherwise list available models and pick a model
    that supports generate_content. Returns model name string.
    """
    if preferred_model:
        try:
            # test a dry-run call with empty prompt - better to just return preferred and rely on API errors caught elsewhere
            return preferred_model
        except Exception:
            pass
    # try to list models and pick one
    try:
        models = client.models.list()
        for m in models:
            # m.supported_methods might not exist depending on SDK; use name as heuristic
            name = getattr(m, "name", str(m))
            # prefer gemini models
            if "gemini" in name.lower():
                return name
        # fallback to first model name
        first = getattr(models[0], "name", str(models[0]))
        return first
    except Exception:
        # as last resort, return the provided preferred or common name
        return preferred_model or "gemini-1.5-pro"

GEMINI_MODEL = choose_gemini_model(GEMINI_MODEL)

# -----------------------------
# Gemini processors
# -----------------------------
def process_gemini_full(name, location, dataset):
    """
    Full dataset processing: ask Gemini to synthesize structured JSON using scraped dataset.
    (Used when Google/DDG scraped data is available.)
    """
    prompt = f"""
You are an expert NGO verifier. Using ONLY the enclosed dataset, produce a strict JSON object with fields:

verifiedName, city, state, country, fullAddress, website, emails, phones,
linkedin, instagram, facebook, twitter, youtube, registrationNumbers, yearFounded,
sectors, shortMission, impactMetrics, activeStatus (ACTIVE/INACTIVE/UNVERIFIED),
confidenceScore (0-100), requiresHumanReview (true/false), topSourceURLs (list), notes.

DATASET:
{json.dumps(dataset, indent=2) }

Return ONLY valid JSON.
"""
    try:
        resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        return resp.text
    except Exception as e:
        print("[WARN] Gemini full call failed:", e)
        return ""

def process_gemini_light_status(name, location, context_text=""):
    """
    Light research: ask Gemini to decide active status and return minimal JSON.
    context_text: short concatenated texts from scraping/news if available.
    Returns JSON text.
    """
    prompt = f"""
You are an assistant that must determine whether an NGO is currently ACTIVE or INACTIVE.

NGO Name: {name}
Location: {location}

Context (if any):
{context_text}

Task:
Return ONLY a JSON object with fields:
- activeStatus: "ACTIVE" or "INACTIVE" or "UNVERIFIED"
- confidenceScore: integer 0-100
- reasons: short array/list of 1-3 strings explaining evidence (website updated, social activity, news, etc.)
- topSourceURLs: list (if any)

Rules:
- Treat social activity within last 6 months as strong evidence of activity.
- Treat website updates within last 12 months as evidence.
- If no public info, return UNVERIFIED with low confidence.

Return EXACT JSON only.
"""
    try:
        resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        return resp.text
    except Exception as e:
        print("[WARN] Gemini light call failed:", e)
        return ""

# -----------------------------
# Robust JSON extractor
# -----------------------------
def extract_json_from_text(raw_text):
    """Attempt to extract JSON object from LLM or messy text."""
    if raw_text is None:
        return {}
    text = raw_text.strip()
    # remove code fences
    if text.startswith("```"):
        text = text.strip("`")
    # sometimes LLMs prefix with text - attempt to find first { and last }
    try:
        return json.loads(text)
    except Exception:
        # try to find JSON substring
        try:
            start = text.index("{")
            end = text.rindex("}") + 1
            snippet = text[start:end]
            return json.loads(snippet)
        except Exception:
            return {}

# -----------------------------
# High-level research flow
# -----------------------------
def research_ngo_multisource(name, location, ngo_link=None):
    """
    Attempt multi-source research in priority order:
    1) Google CSE (if quota available)
    2) DuckDuckGo HTML search
    3) Scrape top candidate URLs
    4) NewsData / Wikipedia / Wayback
    5) If still insufficient, use Gemini light to determine status
    Returns a dict (structured) or raw LLM JSON.
    """

    # 1) Try Google CSE
    query = f'{name} NGO {location}'
    google_resp = google_search(query)
    dataset = {}
    if google_resp and google_resp.get("items"):
        # Good: collect urls and snippets
        items = google_resp.get("items", [])[:MAX_SEARCH_RESULTS]
        dataset["google_results"] = items
        urls = [it.get("link") for it in items if it.get("link")]
    else:
        dataset["google_results"] = []
        urls = []

    # 2) DuckDuckGo if no google urls
    if not urls:
        ddg_links = duckduckgo_search(query, max_results=8)
        dataset["duckduckgo_links"] = ddg_links
        urls.extend(ddg_links)

    # 3) If NGO link provided in sheet, prefer it
    if ngo_link and ngo_link not in urls:
        urls.insert(0, ngo_link)

    # 4) Scrape top URLs (text)
    scraped_texts = []
    for u in urls[:6]:
        text = scrape_website(u)
        scraped_texts.append({"url": u, "text": text})
    dataset["scraped_texts"] = scraped_texts

    # 5) News, Wikipedia, Wayback
    dataset["news"] = newsdata_lookup(name)
    dataset["wikipedia"] = wikipedia_lookup(name)
    ways = [wayback_lookup(u) for u in urls[:4]]
    dataset["wayback"] = ways

    # Evaluate whether we have enough evidence (website or socials or news)
    has_website = any((u for u in urls if u and not u.lower().startswith("http://duckduckgo.com/")))
    has_scraped_nonempty = any(s.get("text") for s in scraped_texts)
    has_news = bool(dataset["news"])
    has_wiki = bool(dataset["wikipedia"])

    # If there's decent scraped data or news/wikipedia, call full Gemini if you want more fields
    if has_scraped_nonempty or has_news or has_wiki:
        # use the full dataset; Gemini will synthesize structured output
        print("[INFO] Sufficient web data found; calling Gemini full synthesis.")
        gemini_raw = process_gemini_full(name, location, dataset)
        parsed = extract_json_from_text(gemini_raw)
        if parsed:
            # Save cached parsed JSON
            parsed.setdefault("topSourceURLs", [s.get("url") for s in scraped_texts if s.get("url")][:6])
            parsed.setdefault("eligibility", "eligible" if parsed.get("confidenceScore",0) >= 60 else "ineligible")
            return parsed
        # If Gemini full returned nothing valid, fallthrough to light path

    # If Google quota exhausted or no good web data, attempt lightweight research:
    print("[INFO] Falling back to lightweight research (news/wiki/scrape + Gemini light).")
    # prepare a short context: combine scraped snippets + wiki + news
    context_parts = []
    for s in scraped_texts:
        if s.get("text"):
            context_parts.append((s.get("text")[:800]))
    if dataset.get("wikipedia"):
        context_parts.append(dataset["wikipedia"].get("summary",""))
    # include news titles
    for n in dataset.get("news", []):
        title = n.get("title") or n.get("title_no_format") or n.get("link")
        if title:
            context_parts.append(title)
    context_text = "\n\n".join(context_parts[:6])

    # 6) If no web signals but Google quota reached, ask Gemini light
    gemini_light_raw = process_gemini_light_status(name, location, context_text=context_text)
    parsed_light = extract_json_from_text(gemini_light_raw)
    if parsed_light:
        # Make sure fields present
        parsed_light.setdefault("topSourceURLs", [s.get("url") for s in scraped_texts if s.get("url")][:6])
        parsed_light.setdefault("eligibility", "eligible" if parsed_light.get("confidenceScore",0) >= 60 and parsed_light.get("activeStatus","UNVERIFIED") == "ACTIVE" else "ineligible")
        return parsed_light

    # final fallback: return minimal record marking UNVERIFIED
    return {
        "verifiedName": name,
        "location": location,
        "activeStatus": "UNVERIFIED",
        "confidenceScore": 10,
        "topSourceURLs": [s.get("url") for s in scraped_texts if s.get("url")][:6],
        "notes": "No reliable web data found; final fallback used."
    }

# -----------------------------
# Main runner
# -----------------------------
def run_pipeline(input_excel, start_row=0, end_row=None):
    print(f"Loading data from {input_excel} ...")

    df = pd.read_excel(input_excel)

    required = ["NGO Name", "NGO Link", "Phone", "Email", "Location", "Sectors"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    total_rows = len(df)

    if end_row is None or end_row > total_rows:
        end_row = total_rows

    print(f"Total NGOs in sheet: {total_rows}")
    print(f"Processing NGOs from row {start_row} → {end_row - 1}\n")

    shortlisted = []
    excluded = []
    processed = 0

    for i in range(start_row, end_row):
        row = df.iloc[i]

        name = str(row["NGO Name"]).strip()
        location = str(row["Location"]).strip() if not pd.isna(row["Location"]) else ""
        ngo_link = str(row["NGO Link"]).strip() if not pd.isna(row["NGO Link"]) else None

        if not name:
            print(f"[SKIP] Row {i} has empty NGO name.")
            continue

        print(f"\n=== {i+1}/{end_row}: Processing NGO → {name} | {location}")

        cached = cache_get(name, location)
        if cached:
            print("[CACHE] Found previous result.")
            record = cached if isinstance(cached, dict) else extract_json_from_text(cached)
        else:
            try:
                record = research_ngo_multisource(name, location, ngo_link=ngo_link)
                cache_save(name, location, record)
            except Exception as e:
                print("[ERROR] Research failed:", e)
                record = {
                    "verifiedName": name,
                    "location": location,
                    "activeStatus": "UNVERIFIED",
                    "confidenceScore": 0,
                    "notes": str(e)
                }

        status = str(record.get("activeStatus", "UNVERIFIED")).upper()
        confidence = int(record.get("confidenceScore", 0))

        if status == "ACTIVE" and confidence >= 60:
            shortlisted.append(record)
            print(f"[SHORTLIST] {name} ✔ (Confidence {confidence})")
        else:
            excluded.append(record)
            print(f"[EXCLUDE] {name} ✖ (Status {status}, Confidence {confidence})")

        processed += 1
        time.sleep(0.7)

    print("\n=== COMPLETED CHUNK ===")
    print(f"Total NGOs processed in this run: {processed}")

    out_suffix = f"{start_row}_{end_row}"

    pd.DataFrame(shortlisted).to_excel(f"Shortlisted_{out_suffix}.xlsx", index=False)
    pd.DataFrame(excluded).to_excel(f"Excluded_{out_suffix}.xlsx", index=False)

    print("Generated:")
    print(f" - Shortlisted_{out_suffix}.xlsx")
    print(f" - Excluded_{out_suffix}.xlsx")

    return shortlisted, excluded


# -----------------------------
# CLI entry
# -----------------------------
if __name__ == "__main__":
    # Example usage: python check_ngo.py
    input_file = "Cleaned_NGO_Data.xlsx"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    run_pipeline(input_file,1999,5000)

