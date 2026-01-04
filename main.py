import os
import requests
import msal
import logging
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ==========================================
# CONFIGURATION
# ==========================================
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN") 
GLEAN_API_TOKEN = os.getenv("GLEAN_API_TOKEN")

GLEAN_URL = "https://app.glean.com"
DATASOURCE = "powerbiconductor" 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Connector")

app = Flask(__name__)

def get_access_token():
    if not REFRESH_TOKEN:
        logger.error("❌ REFRESH_TOKEN is missing!")
        return None
    
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    client = msal.PublicClientApplication(CLIENT_ID, authority=authority)
    
    result = client.acquire_token_by_refresh_token(
        REFRESH_TOKEN, 
        scopes=["https://analysis.windows.net/powerbi/api/Report.Read.All", 
                "https://analysis.windows.net/powerbi/api/Dataset.Read.All"]
    )
    
    if "access_token" in result:
        return result["access_token"]
    else:
        logger.error(f"❌ Auth Failed: {result.get('error_description')}")
        return None

def sync_powerbi_to_glean():
    logger.info("⏳ Starting Sync Job...")
    
    token = get_access_token()
    if not token: return "Auth Failed"

    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Find the Report
    logger.info("   Looking for 'Acme_Corp_Reports'...")
    reports_url = "https://api.powerbi.com/v1.0/myorg/reports"
    r = requests.get(reports_url, headers=headers)
    reports = r.json().get("value", [])
    
    target = next((item for item in reports if item["name"] == "Acme_Corp_Reports"), None)
    if not target:
        logger.error("❌ Report not found.")
        return "Report Not Found"

    dataset_id = target["datasetId"]
    
    # 2. Query Data
    logger.info(f"   Querying Dataset: {dataset_id}")
    query_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    dax = {"queries": [{"query": "EVALUATE Reports"}]}
    
    q_res = requests.post(query_url, headers=headers, json=dax)
    if q_res.status_code != 200:
        logger.error(f"❌ Query Error: {q_res.text}")
        return "Query Failed"
        
    try:
        rows = q_res.json()["results"][0]["tables"][0]["rows"]
        logger.info(f"✅ Found {len(rows)} rows. Sending to Glean...")
    except:
        logger.error("❌ Parse Error")
        return "Parse Error"

    # 3. Push to Glean (With Error Checking)
    success_count = 0
    error_count = 0

    for row in rows:
        r_id = row.get("Reports[Column1]") or row.get("Column1")
        r_title = row.get("Reports[Column2]") or row.get("Column2")
        r_content = row.get("Reports[Column3]") or row.get("Column3")
        r_author = row.get("Reports[Column4]") or row.get("Column4")
        r_access = row.get("Reports[Column5]") or row.get("Column5")

        if r_id == "id" or not r_id: continue

        payload = {
            "document": {
                "datasource": DATASOURCE,
                "id": r_id,
                "title": r_title,
                "viewURL": f"{target['webUrl']}?filter=Reports/Column1 eq '{r_id}'",
                "body": {
                    "mimeType": "text/plain",
                    "textContent": f"Content: {r_content}\n\nAccess Level: {r_access}"
                },
                "author": {"email": r_author},
                "permissions": {"allowAnonymousAccess": True}
            }
        }
        
        # --- NEW: Check the response! ---
        res = requests.post(
            f"{GLEAN_URL}/api/index/v1/indexdocument", 
            headers={"Authorization": f"Bearer {GLEAN_API_TOKEN}"}, 
            json=payload
        )
        
        if res.status_code != 200:
            error_count += 1
            # Only print the first error to avoid flooding logs
            if error_count == 1:
                logger.error(f"❌ GLEAN REJECTED DATA! Status: {res.status_code}")
                logger.error(f"❌ REASON: {res.text}")
        else:
            success_count += 1
            
    logger.info(f"📊 Summary: {success_count} Succeeded, {error_count} Failed.")
    return "Done"

scheduler = BackgroundScheduler()
scheduler.add_job(sync_powerbi_to_glean, 'interval', minutes=30)
scheduler.start()

@app.route('/')
def home(): return "Glean Connector Running"

@app.route('/sync')
def manual_sync():
    sync_powerbi_to_glean()
    return jsonify({"status": "Check Logs"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))