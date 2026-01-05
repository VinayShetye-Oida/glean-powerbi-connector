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
CLIENT_SECRET = os.getenv("CLIENT_SECRET") 
TENANT_ID = os.getenv("TENANT_ID")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN") 
GLEAN_API_TOKEN = os.getenv("GLEAN_API_TOKEN")
GLEAN_URL = os.getenv("GLEAN_URL")
DATASOURCE = "powerbiconductor" 

# The workspace to scan
TARGET_WORKSPACE_NAME = "Superstore"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Connector")

app = Flask(__name__)

def get_access_token():
    if not REFRESH_TOKEN or not CLIENT_SECRET:
        logger.error("❌ Credentials missing! Check Environment Variables.")
        return None
    
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    client = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    
    # Use .default scope for Service Principal
    result = client.acquire_token_by_refresh_token(
        REFRESH_TOKEN, 
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )
    return result.get("access_token")

def sync_powerbi_to_glean():
    logger.info("🤖 Starting Smart Sync Job...")
    token = get_access_token()
    if not token: return "Auth Failed"
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Find Workspace (To discover reports)
    groups_url = "https://api.powerbi.com/v1.0/myorg/groups"
    groups = requests.get(groups_url, headers=headers).json().get("value", [])
    target_group = next((g for g in groups if g["name"] == TARGET_WORKSPACE_NAME), None)

    if not target_group:
        logger.error(f"❌ Workspace '{TARGET_WORKSPACE_NAME}' not found.")
        return "Workspace Missing"

    ws_id = target_group["id"]
    logger.info(f"   📂 Scanning Workspace: {TARGET_WORKSPACE_NAME} ({ws_id})")

    # 2. Get ALL Reports in Workspace
    reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports"
    reports = requests.get(reports_url, headers=headers).json().get("value", [])
    logger.info(f"   🔎 Found {len(reports)} reports. Beginning analysis...")

    total_indexed = 0

    for report in reports:
        report_name = report["name"]
        dataset_id = report["datasetId"]
        web_url = report["webUrl"]
        logger.info(f"      👉 Processing Report: {report_name}")

        # 3. DISCOVERY: Get Tables (Reverted to global endpoint)
        # Note: We removed 'groups/{ws_id}' here because it caused 404s
        tables_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/tables"
        t_res = requests.get(tables_url, headers=headers)
        
        if t_res.status_code != 200:
            logger.warning(f"         ⚠️ Could not fetch metadata. Status: {t_res.status_code}. Skipping.")
            continue
            
        tables = t_res.json().get("value", [])
        
        for table in tables:
            table_name = table["name"]
            if table_name.startswith("DateTableTemplate"): continue 

            logger.info(f"         📊 Found Table: '{table_name}'. Querying...")

            # 4. DYNAMIC QUERY (Reverted to global endpoint)
            query_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
            
            dax = {"queries": [{"query": f"EVALUATE TOPN(50, '{table_name}')"}]}
            
            q_res = requests.post(query_url, headers=headers, json=dax)
            
            if q_res.status_code != 200:
                logger.warning(f"         ⚠️ Query failed for table '{table_name}'.")
                continue

            try:
                rows = q_res.json()["results"][0]["tables"][0]["rows"]
            except:
                continue

            # 5. DYNAMIC INDEXING
            count = 0
            for row in rows:
                values = list(row.values())
                if not values: continue
                
                # Dynamic Mapping
                r_id = str(values[0])
                r_title = str(values[1]) if len(values) > 1 else f"{report_name} - {table_name}"
                r_content = " | ".join([str(v) for v in values])

                # URL Filter Logic
                col_key_name = list(row.keys())[0] 
                clean_col_name = col_key_name.replace("[", "/").replace("]", "")
                
                raw_filter = f"{clean_col_name} eq '{r_id}'"
                final_url = f"{web_url}?filter={quote(raw_filter)}"

                payload = {
                    "document": {
                        "datasource": DATASOURCE,
                        "id": f"{report_name}_{table_name}_{r_id}",
                        "title": r_title,
                        "viewURL": final_url,
                        "body": {
                            "mimeType": "text/plain",
                            "textContent": f"Source: {report_name} / {table_name}\nData: {r_content}"
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
                    count += 1
            
            total_indexed += count
            logger.info(f"         ✅ Indexed {count} rows from '{table_name}'")

    logger.info(f"🚀 Sync Complete. Total items indexed: {total_indexed}")
    return "Done"

scheduler = BackgroundScheduler()
scheduler.add_job(sync_powerbi_to_glean, 'interval', minutes=30)
scheduler.start()

@app.route('/')
def home(): return "Glean Smart Connector Running"

@app.route('/sync')
def manual_sync():
    sync_powerbi_to_glean()
    return jsonify({"status": "Check Logs"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
