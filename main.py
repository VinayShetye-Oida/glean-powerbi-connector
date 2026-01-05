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

TARGET_WORKSPACE_NAME = "Superstore"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Connector")

app = Flask(__name__)

def get_access_token():
    if not REFRESH_TOKEN or not CLIENT_SECRET:
        logger.error("❌ Credentials missing!")
        return None
    
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    client = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = client.acquire_token_by_refresh_token(
        REFRESH_TOKEN, 
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )
    return result.get("access_token")

def sync_powerbi_to_glean():
    logger.info("🤖 Starting Smart DAX Sync...")
    token = get_access_token()
    if not token: return "Auth Failed"
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Find Workspace
    groups = requests.get("https://api.powerbi.com/v1.0/myorg/groups", headers=headers).json().get("value", [])
    target_group = next((g for g in groups if g["name"] == TARGET_WORKSPACE_NAME), None)

    if not target_group:
        logger.error(f"❌ Workspace '{TARGET_WORKSPACE_NAME}' not found.")
        return "Workspace Missing"

    ws_id = target_group["id"]
    logger.info(f"   📂 Scanning Workspace: {TARGET_WORKSPACE_NAME} ({ws_id})")

    # 2. Get Reports
    reports = requests.get(f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/reports", headers=headers).json().get("value", [])
    logger.info(f"   🔎 Found {len(reports)} reports.")

    total_indexed = 0

    for report in reports:
        report_name = report["name"]
        dataset_id = report["datasetId"]
        web_url = report["webUrl"]
        
        # Check if dataset is in a different workspace (Shared Dataset)
        # If 'datasetWorkspaceId' is present, use it. Otherwise use current ws_id.
        ds_ws_id = report.get("datasetWorkspaceId", ws_id)
        
        logger.info(f"      👉 Report: {report_name} (Dataset: {dataset_id})")

        # 3. SMART DISCOVERY VIA DAX (The "Side Door")
        # We query the DMV (Dynamic Management View) to list tables.
        discovery_query = "EVALUATE SELECTCOLUMNS(INFO.TABLES(), \"Table\", [Name])"
        
        query_url = f"https://api.powerbi.com/v1.0/myorg/groups/{ds_ws_id}/datasets/{dataset_id}/executeQueries"
        
        dax_payload = {"queries": [{"query": discovery_query}]}
        meta_res = requests.post(query_url, headers=headers, json=dax_payload)
        
        if meta_res.status_code != 200:
            logger.warning(f"         ⚠️ Could not discover tables (Error {meta_res.status_code}). Skipping.")
            continue
            
        try:
            # Parse the table names from the DAX result
            found_tables = [row["[Table]"] for row in meta_res.json()["results"][0]["tables"][0]["rows"]]
        except:
            logger.warning("         ⚠️ Failed to parse table list.")
            continue

        for table_name in found_tables:
            # Skip hidden system tables
            if table_name.startswith("Date") or table_name.startswith("LocalDate"): continue

            logger.info(f"         📊 Discovered Table: '{table_name}'. Indexing...")

            # 4. FETCH DATA (Dynamic TOPN)
            data_query = f"EVALUATE TOPN(50, '{table_name}')"
            q_res = requests.post(query_url, headers=headers, json={"queries": [{"query": data_query}]})
            
            if q_res.status_code != 200: continue

            try:
                rows = q_res.json()["results"][0]["tables"][0]["rows"]
            except: continue

            # 5. INDEX TO GLEAN
            count = 0
            for row in rows:
                values = list(row.values())
                if not values: continue
                
                # Heuristic: Col 0 = ID, Col 1 = Title, Rest = Body
                r_id = str(values[0])
                r_title = str(values[1]) if len(values) > 1 else f"{report_name} - {table_name}"
                r_content = " | ".join([str(v) for v in values])

                # Construct URL Filter
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
                if res.status_code == 200: count += 1
            
            total_indexed += count
            logger.info(f"         ✅ Indexed {count} rows.")

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
