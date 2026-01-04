import os
import requests
import msal
import logging
from urllib.parse import quote
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ==========================================
# CONFIGURATION
# ==========================================
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN") 
GLEAN_API_TOKEN = os.getenv("GLEAN_API_TOKEN")
GLEAN_URL = os.getenv("GLEAN_URL")
DATASOURCE = "powerbiconductor" 

# TARGET WORKSPACE NAME (The one from your screenshot)
TARGET_WORKSPACE_NAME = "Superstore"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Connector")

app = Flask(__name__)

def get_access_token():
    if not REFRESH_TOKEN:
        logger.error("❌ REFRESH_TOKEN is missing!")
        return None
    
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    client = msal.PublicClientApplication(CLIENT_ID, authority=authority)
    
    # UPDATED SCOPES: Added Group.Read.All to access Shared Workspaces
    result = client.acquire_token_by_refresh_token(
        REFRESH_TOKEN, 
        scopes=[
            "https://analysis.windows.net/powerbi/api/Report.Read.All", 
            "https://analysis.windows.net/powerbi/api/Dataset.Read.All",
            "https://analysis.windows.net/powerbi/api/Group.Read.All" 
        ]
    )
    
    if "access_token" in result:
        return result["access_token"]
    else:
        logger.error(f"❌ Auth Failed: {result.get('error_description')}")
        return None

def sync_powerbi_to_glean():
    logger.info("⏳ Starting Global Sync Job...")
    
    if not GLEAN_URL:
        return "Config Error: Missing GLEAN_URL"

    token = get_access_token()
    if not token: return "Auth Failed"

    headers = {"Authorization": f"Bearer {token}"}
    
    # ---------------------------------------------------------
    # STEP 1: Find the 'Superstore' Workspace
    # ---------------------------------------------------------
    logger.info(f"   🔎 Searching for Workspace: '{TARGET_WORKSPACE_NAME}'...")
    groups_url = "https://api.powerbi.com/v1.0/myorg/groups"
    g_res = requests.get(groups_url, headers=headers)
    
    if g_res.status_code != 200:
        logger.error(f"❌ Failed to list workspaces: {g_res.text}")
        return "Workspace Error"

    groups = g_res.json().get("value", [])
    target_group = next((g for g in groups if g["name"] == TARGET_WORKSPACE_NAME), None)

    if not target_group:
        logger.error(f"❌ Workspace '{TARGET_WORKSPACE_NAME}' NOT FOUND. Check spelling or permissions.")
        # Log available groups to help debug
        all_groups = [g["name"] for g in groups]
        logger.info(f"   (Available Workspaces: {all_groups})")
        return "Workspace Not Found"

    ws_id = target_group["id"]
    logger.info(f"   ✅ Found Workspace ID: {ws_id}")

    # ---------------------------------------------------------
    # STEP 2: Find 'Acme_Corp_Reports' inside Superstore
    # ---------------------------------------------------------
    logger.info(f"   📂 Scanning contents of '{TARGET_WORKSPACE_NAME}'...")
    reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports"
    r_res = requests.get(reports_url, headers=headers)
    reports = r_res.json().get("value", [])

    # We specifically look for Acme first because we know its Table Structure
    target_report = next((r for r in reports if r["name"] == "Acme_Corp_Reports"), None)

    if not target_report:
        logger.error("❌ 'Acme_Corp_Reports' not found in Superstore workspace.")
        return "Report Not Found"

    logger.info(f"   ✅ Found Report: {target_report['name']}")
    dataset_id = target_report["datasetId"]

    # ---------------------------------------------------------
    # STEP 3: Query Data (Existing Logic)
    # ---------------------------------------------------------
    query_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    dax = {"queries": [{"query": "EVALUATE Reports"}]}
    
    q_res = requests.post(query_url, headers=headers, json=dax)
    
    if q_res.status_code != 200:
        logger.error(f"❌ Query Failed: {q_res.text}")
        return "Query Failed"
    
    try:
        rows = q_res.json()["results"][0]["tables"][0]["rows"]
        logger.info(f"   ✅ Extracted {len(rows)} rows.")
    except:
        logger.error("❌ Failed to parse data rows.")
        return "Parse Error"

    # ---------------------------------------------------------
    # STEP 4: Push to Glean
    # ---------------------------------------------------------
    success_count = 0
    for row in rows:
        r_id = row.get("Reports[Column1]") or row.get("Column1")
        r_title = row.get("Reports[Column2]") or row.get("Column2")
        r_content = row.get("Reports[Column3]") or row.get("Column3")
        r_access = row.get("Reports[Column5]") or row.get("Column5")

        if not r_id: continue

        # Safe URL Encode
        raw_filter = f"Reports/Column1 eq '{r_id}'"
        encoded_filter = quote(raw_filter) 
        final_url = f"{target_report['webUrl']}?filter={encoded_filter}"

        payload = {
            "document": {
                "datasource": DATASOURCE,
                "id": r_id,
                "title": r_title,
                "viewURL": final_url,
                "body": {
                    "mimeType": "text/plain",
                    "textContent": f"Content: {r_content}"
                },
                "permissions": {"allowAnonymousAccess": True}
            }
        }
        
        res = requests.post(
            f"{GLEAN_URL}/api/index/v1/indexdocument", 
            headers={"Authorization": f"Bearer {GLEAN_API_TOKEN}"}, 
            json=payload
        )
        if res.status_code == 200:
            success_count += 1

    logger.info(f"📊 Sync Complete. {success_count} items pushed from Superstore.")
    return "Done"

scheduler = BackgroundScheduler()
scheduler.add_job(sync_powerbi_to_glean, 'interval', minutes=30)
scheduler.start()

@app.route('/')
def home(): return "Glean Superstore Connector Running"

@app.route('/sync')
def manual_sync():
    sync_powerbi_to_glean()
    return jsonify({"status": "Check Logs"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
