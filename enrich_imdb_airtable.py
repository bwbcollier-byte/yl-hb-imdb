#!/usr/bin/env python3
"""
enrich_imdb_airtable.py

Enriches Airtable records with data from IMDb via RapidAPI and TMDb.
Identifies the IMDb ID from 'Soc IMDb Id' or 'Soc IMDb' URL.

Features:
- IMDb Profile Enrichment (RapidAPI)
- IMDb Cast Titles (Media Array) (RapidAPI)
- TMDb Identification & Metadata (TMDb API)
- Automatic Search Fallbacks (By Name)

Usage:
    python3 enrich_imdb_airtable.py --limit 10
    python3 enrich_imdb_airtable.py --all
"""

import os
import sys
import argparse
import requests
import json
import time
import re
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID          = "appqfxEDZEN5ASSBB"
TABLE_ID         = "tbl1Sk8qXGbtFS7ZO"
VIEW_NAME        = "To Process"

# IMDb RapidAPI
RAPIDAPI_KEY     = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST    = "imdb236.p.rapidapi.com"
IMDB_BASE_URL    = "https://imdb236.p.rapidapi.com/api/imdb"

# TMDb Read Access Token (v4)
TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

IMDB_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
    "Content-Type": "application/json"
}

TMDB_HEADERS = {
    "Authorization": f"Bearer {TMDB_READ_TOKEN}",
    "accept": "application/json"
}


# ── Helper Functions ──────────────────────────────────────────────────────────

def extract_id(id_string, prefix="nm"):
    """Extract numeric/prefixed ID from URL or string."""
    if not id_string:
        return None
    # nm0000001 or 12345
    if prefix:
        match = re.search(f"{prefix}\\d+", str(id_string))
        if match:
            return match.group(0)
    else:
        match = re.search(r"\\d+", str(id_string))
        if match:
            return match.group(0)
    return None

def fetch_imdb_person_details(imdb_id):
    """Fetch profile from RapidAPI."""
    url = f"{IMDB_BASE_URL}/name/{imdb_id}"
    try:
        resp = requests.get(url, headers=IMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"    [ERROR] IMDb API request failed: {e}")
    return None

def fetch_imdb_cast_titles(imdb_id):
    """Fetch cast titles from RapidAPI."""
    url = f"https://imdb236.p.rapidapi.com/api/imdb/cast/{imdb_id}/titles"
    try:
        resp = requests.get(url, headers=IMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"    [ERROR] IMDb Cast Titles request failed: {e}")
    return None

def find_tmdb_info_by_imdb(imdb_id):
    """Use TMDb /find endpoint to get TMDb details via IMDb ID."""
    if not imdb_id:
        return None
    url = f"https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id&language=en-US"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"    [ERROR] TMDb Find request failed: {e}")
    return None

def search_imdb_by_name(name):
    """Search for a person on IMDb via RapidAPI."""
    if not name or name == "Unknown":
        return None
    url = f"{IMDB_BASE_URL}/search/name"
    params = {"name": name}
    try:
        resp = requests.get(url, headers=IMDB_HEADERS, params=params, timeout=15)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                # Return the ID of the first result
                return results[0].get("id")
    except Exception as e:
        print(f"    [ERROR] IMDb search failed: {e}")
    return None

def find_imdb_id_via_tmdb(tmdb_id):
    """Use TMDb person external IDs to get IMDb ID."""
    if not tmdb_id:
        return None
    url = f"https://api.themoviedb.org/3/person/{tmdb_id}/external_ids"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("imdb_id")
    except Exception as e:
        print(f"    [ERROR] TMDb lookup failed: {e}")
    return None

def find_person_by_name_tmdb(name):
    """Search TMDb for a person by name."""
    if not name or name == "Unknown":
        return None
    url = f"https://api.themoviedb.org/3/search/person?query={requests.utils.quote(name)}"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0].get("id")
    except Exception as e:
        print(f"    [ERROR] TMDb name search failed: {e}")
    return None

def update_records_bulk(records_batch: list):
    """PATCH up to 10 records at once in Airtable."""
    if not records_batch:
        return True, {}
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    try:
        r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=20)
        return r.status_code == 200, r.json()
    except Exception as e:
        return False, str(e)

def update_run_details(existing_details, status, message):
    """Maintain historical log in Airtable."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_entry = f"[{timestamp}] {status}: {message}"
    
    if not existing_details:
        return [new_entry]
    
    # If it's a string (old format), convert to list
    if isinstance(existing_details, str):
        try:
            history = json.loads(existing_details)
        except:
            history = [existing_details]
    else:
        history = list(existing_details)
        
    history.append(new_entry)
    return history[-10:] # Keep last 10 entries

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich IMDb profiles in Airtable.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N records")
    parser.add_argument("--all",   action="store_true",   help="Process all records in the view")
    args = parser.parse_args()

    if not args.all and args.limit is None:
        args.limit = 10

    print(f"🎬 Starting IMDb enrichment (limit={args.limit or 'ALL'})...")

    ok_count    = 0
    err_count   = 0
    skip_count  = 0
    processed   = 0
    batch_queue = []
    today_str   = datetime.now().strftime("%Y-%m-%d")
    
    airtable_url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    
    # Fields to fetch from Airtable
    fields_to_read = [
        "Name", "Soc IMDb Id", "Soc IMDb", "Soc TMDb", "Soc TMDb Id", "Run Details",
        "Description", "Profile Image", "Media Array", "TMDb Information"
    ]
    
    params = {
        "pageSize": 100, 
        "view": VIEW_NAME,
        "fields[]": fields_to_read
    }

    while True:
        try:
            r = requests.get(airtable_url, headers=AIRTABLE_HEADERS, params=params, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"[ERROR] Airtable fetch failed: {e}")
            break

        if "error" in data:
            print(f"[ERROR] Airtable API: {data}")
            break

        page_records = data.get("records", [])
        if not page_records:
            break

        print(f"\n--- Processing batch of {len(page_records)} records ---")

        for record in page_records:
            if args.limit and processed >= args.limit:
                break

            rec_id = record["id"]
            fields = record.get("fields", {})
            name   = fields.get("Name", "Unknown")
            
            processed += 1
            print(f"[{processed}] {name}")

            # 1. Determine IMDb ID
            imdb_id = extract_id(fields.get("Soc IMDb Id"), "nm") or extract_id(fields.get("Soc IMDb"), "nm")

            if not imdb_id:
                # Fallback A: Try finding via Soc TMDb
                tmdb_id = extract_id(fields.get("Soc TMDb Id"), "") or extract_id(fields.get("Soc TMDb"), "")
                if tmdb_id:
                    print(f"  🔍 Finding IMDb ID via TMDb ID: {tmdb_id}")
                    imdb_id = find_imdb_id_via_tmdb(tmdb_id)
            
            if not imdb_id:
                # Fallback B: Search IMDb by name
                print(f"  🔍 Searching IMDb by name: {name}")
                imdb_id = search_imdb_by_name(name)
                
            if not imdb_id:
                # Fallback C: Search TMDb by name then get external ID
                print(f"  🔍 Searching TMDb by name: {name}")
                tmdb_id = find_person_by_name_tmdb(name)
                if tmdb_id:
                    print(f"  🔍 Found TMDb ID {tmdb_id}, checking external IDs...")
                    imdb_id = find_imdb_id_via_tmdb(tmdb_id)

            if not imdb_id:
                print("  ⏭  Skipping — No IMDb ID found even after fallbacks")
                update_fields = {
                    "Run Status": "Processed",
                    "Run Details": "\n".join(update_run_details(fields.get("Run Details"), "Skipped", "No IMDb ID found"))
                }
                batch_queue.append({"id": rec_id, "fields": update_fields})
                skip_count += 1
                continue

            # 2. Fetch Enrichment Data
            print(f"  📥 Fetching Details for {imdb_id}...")
            imdb_data    = fetch_imdb_person_details(imdb_id)
            cast_titles  = fetch_imdb_cast_titles(imdb_id)
            tmdb_find    = find_tmdb_info_by_imdb(imdb_id)
            
            if not imdb_data:
                print("  ❌ Failed to retrieve IMDb profile")
                update_fields = {
                    "Run Status": "Error",
                    "Run Details": "\n".join(update_run_details(fields.get("Run Details"), "Error", "IMDb fetch failed"))
                }
                batch_queue.append({"id": rec_id, "fields": update_fields})
                err_count += 1
            else:
                # 3. Map data to Airtable fields
                update_fields = {
                    "Soc IMDb Id": imdb_id,
                    "Soc IMDb": f"https://www.imdb.com/name/{imdb_id}/",
                    "IMDb Name": imdb_data.get("name"),
                    "IMDb Birth Name": imdb_data.get("birthName"),
                    "IMDb Profile Image": imdb_data.get("primaryImage"),
                    "IMDb Birth Date": imdb_data.get("birthDate"),
                    "IMDb Death Date": imdb_data.get("deathDate"),
                    "IMDb Birth Location": imdb_data.get("birthLocation"),
                    "IMDb Death Location": imdb_data.get("deathLocation"),
                    "IMDb Height": imdb_data.get("height"),
                    "IMDb Check": today_str,
                    "Run Status": "Processed"
                }
                
                # Media Array (JSON)
                if cast_titles:
                    media_json = json.dumps(cast_titles)
                    if len(media_json) > 95000:
                        media_json = media_json[:95000] + "..."
                    update_fields["Media Array"] = media_json
                
                # TMDb Information (JSON and IDs)
                if tmdb_find:
                    tmdb_json = json.dumps(tmdb_find)
                    if len(tmdb_json) > 95000:
                        tmdb_json = tmdb_json[:95000] + "..."
                    update_fields["TMDb Information"] = tmdb_json
                    
                    # Extract ID from person_results
                    p_results = tmdb_find.get("person_results", [])
                    if p_results:
                        p_res_obj = p_results[0]
                        p_id = p_res_obj.get("id")
                        update_fields["Soc TMDb Id"] = str(p_id)
                        update_fields["Soc TMDb"] = f"https://www.themoviedb.org/person/{p_id}"
                        
                        # Fallback Gender/Professions from TMDb if possible
                        if p_res_obj.get("gender"):
                            g_map = {1: "Female", 2: "Male", 3: "Non-binary"}
                            update_fields["Gender"] = g_map.get(p_res_obj["gender"], "Other")
                        
                        if not update_fields.get("IMDb Professions") and p_res_obj.get("known_for_department"):
                            update_fields["IMDb Professions"] = p_res_obj["known_for_department"]

                # Professions string
                professions = imdb_data.get("primaryProfessions", [])
                if professions and isinstance(professions, list):
                    update_fields["IMDb Professions"] = ", ".join(professions)
                elif professions:
                    update_fields["IMDb Professions"] = str(professions)
                    
                # Known For titles string
                known_for = imdb_data.get("knownForTitles", [])
                if known_for and isinstance(known_for, list):
                    update_fields["IMDb Known For"] = ", ".join(known_for)
                elif known_for:
                    update_fields["IMDb Known For"] = str(known_for)
                    
                # Core Descriptions
                if not fields.get("Description") and imdb_data.get("biography"):
                    update_fields["Description"] = imdb_data["biography"]
                
                if not fields.get("Profile Image") and imdb_data.get("primaryImage"):
                    update_fields["Profile Image"] = imdb_data["primaryImage"]
                    
                # Store History
                update_fields["Run Details"] = "\n".join(update_run_details(
                    fields.get("Run Details"), 
                    "Success", 
                    f"Full enrichment for {imdb_id} (IMDb + TMDb)"
                ))
                
                batch_queue.append({"id": rec_id, "fields": update_fields})

            # Check for batch flush
            if len(batch_queue) >= 10:
                print(f"  📤 Sending bulk update ({len(batch_queue)} records)...")
                success, resp = update_records_bulk(batch_queue)
                if success:
                    print("  ✅ Batch saved")
                    ok_count += len(batch_queue)
                else:
                    print(f"  ❌ Batch update failed: {resp}")
                    # If batch fails, try one-by-one to isolate culprit
                    print("  🔍 Retrying individually to isolate field error...")
                    for q in batch_queue:
                        s, r = update_records_bulk([q])
                        if s: 
                            ok_count += 1
                        else: 
                            err_count += 1
                            print(f"    ❌ Failed Individual Record: {q.get('fields', {}).get('Name')} - {r}")
                batch_queue.clear()
                time.sleep(0.5)

            # Rate Limit Protection
            time.sleep(0.55)

        # Final Batch Flush
        if batch_queue:
            print(f"  📤 Sending final batch update ({len(batch_queue)} records)...")
            success, resp = update_records_bulk(batch_queue)
            if success:
                print("  ✅ Batch saved")
                ok_count += len(batch_queue)
            else:
                print(f"  ❌ Batch update failed: {resp}")
                print("  🔍 Retrying individually to isolate field error...")
                for q in batch_queue:
                    s, r = update_records_bulk([q])
                    if s: 
                        ok_count += 1
                    else: 
                        err_count += 1
                        print(f"    ❌ Failed Individual Record: {q.get('fields', {}).get('Name')} - {r}")
            batch_queue.clear()

        if args.limit and processed >= args.limit:
            break
            
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    print(f"\n{'='*55}")
    print(f"🎬 IMDb Enrichment Complete!")
    print(f"   ✅ Updated : {ok_count}")
    print(f"   ⏭  Skipped : {skip_count}")
    print(f"   ❌ Errors  : {err_count}")
    print(f"{'='*55}")

if __name__ == "__main__":
    main()
