import os
import requests
import msal
import logging
import time
import json
import threading
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ==========================================
# 🔐 CONFIGURATION (Env Vars for Render)
# ==========================================
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET") 
TENANT_ID = os.getenv("TENANT_ID")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN") 
GLEAN_API_TOKEN = os.getenv("GLEAN_API_TOKEN")
GLEAN_URL = os.getenv("GLEAN_URL", "https://oida-be.glean.com")
DATASOURCE = "powerbiconductor" 

# 🔥 REMOVED TARGET_WORKSPACES: We now dynamically discover ALL workspaces
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Connector")

app = Flask(__name__)

def get_access_token():
    if not REFRESH_TOKEN:
        logger.error("❌ REFRESH_TOKEN is missing from Environment Variables.")
        return None

    # Use PublicClientApplication to match Azure AD settings
    client = msal.PublicClientApplication(
        CLIENT_ID, 
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    
    result = client.acquire_token_by_refresh_token(
        REFRESH_TOKEN, 
        scopes=["https://analysis.windows.net/powerbi/api/Tenant.Read.All", 
                "https://analysis.windows.net/powerbi/api/Report.Read.All", 
                "https://analysis.windows.net/powerbi/api/Group.Read.All"]
    )
    
    if "access_token" in result: return result["access_token"]
    logger.error(f"❌ Auth Failed: {result.get('error_description')}")
    return None

def run_sync_job():
    logger.info("🤖 Starting ADMIN SCANNER Sync Job (Discovery Mode)...")
    token = get_access_token()
    if not token: return
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. DISCOVER ALL WORKSPACES
    logger.info("   🔎 Discovering ALL accessible Workspaces...")
    groups = requests.get("https://api.powerbi.com/v1.0/myorg/groups", headers=headers).json().get("value", [])
    
    # Discover all workspaces, but IGNORE the system "Admin monitoring" workspace
    ws_ids = [g["id"] for g in groups if g["name"] != "Admin monitoring"]
    
    if not ws_ids:
        logger.error("❌ No workspaces found for this account.")
        return
        
    logger.info(f"   📂 Discovered {len(ws_ids)} Workspaces.")

    total_indexed = 0
    chunk_size = 100 # Power BI API limit is 100 workspaces per scan request

    # Process in chunks to prevent API crashes if the org has 100+ workspaces
    for i in range(0, len(ws_ids), chunk_size):
        chunk = ws_ids[i:i + chunk_size]

        # 2. INITIATE BULK ADMIN SCAN FOR CHUNK
        scan_url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True"
        payload = {"workspaces": chunk}
        
        logger.info(f"   🛰️ Initiating Metadata Scan for chunk of {len(chunk)} workspaces...")
        scan_res = requests.post(scan_url, headers=headers, json=payload)
        
        if scan_res.status_code != 202:
            logger.error(f"   ❌ Scan Initiation Failed: {scan_res.status_code} - {scan_res.text}")
            continue

        scan_id = scan_res.json()["id"]
        logger.info(f"   ⏳ Scan ID: {scan_id}. Waiting for results...")

        # 3. POLL FOR RESULTS
        while True:
            status_res = requests.get(f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}", headers=headers)
            status = status_res.json().get("status")
            
            if status == "Succeeded": break
            if status == "Failed":
                logger.error("   ❌ Scan Failed for this chunk.")
                break
            time.sleep(2)

        # 4. PROCESS RESULTS
        if status == "Succeeded":
            result_res = requests.get(f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}", headers=headers)
            scan_data = result_res.json()
            
            # Loop through ALL returned workspaces in this chunk
            for workspace_data in scan_data.get("workspaces", []):
                ws_name = workspace_data.get("name")
                ws_id = workspace_data.get("id")
                logger.info(f"   ▶️ Processing Workspace: {ws_name}")
                
                for dataset in workspace_data.get("datasets", []):
                    ds_name = dataset.get("name")
                    ds_id = dataset.get("id")
                    
                    # Create valid View URL for Glean
                    valid_view_url = f"https://app.powerbi.com/groups/{ws_id}/datasets/{ds_id}"

                    if "tables" in dataset:
                        for table in dataset["tables"]:
                            table_name = table["name"]
                            if table_name.startswith("Date") or table_name.startswith("LocalDate") or table_name.startswith("RowNumber"): continue
                            
                            # Get Data
                            query_url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets/{ds_id}/executeQueries"
                            dax = {"queries": [{"query": f"EVALUATE TOPN(50, '{table_name}')"}]}
                            
                            try:
                                res = requests.post(query_url, headers=headers, json=dax)
                                if res.status_code == 200:
                                    rows = res.json()["results"][0]["tables"][0]["rows"]
                                    if rows:
                                        logger.info(f"      ✅ Extracted '{table_name}': {len(rows)} rows.")
                                        
                                        count = 0
                                        for row in rows:
                                            vals = list(row.values())
                                            if not vals: continue
                                            r_id = str(vals[0])
                                            r_title = f"[{ws_name}] {ds_name} - {table_name}"
                                            r_content = " | ".join([str(v) for v in vals])
                                            
                                            payload = {
                                                "document": {
                                                    "datasource": DATASOURCE,
                                                    "id": f"{ds_name}_{table_name}_{r_id}",
                                                    "title": r_title,
                                                    "viewURL": valid_view_url,
                                                    "body": {"mimeType": "text/plain", "textContent": r_content},
                                                    "permissions": {"allowAnonymousAccess": True}
                                                }
                                            }
                                            # Push to Glean
                                            g_res = requests.post(f"{GLEAN_URL}/api/index/v1/indexdocument", headers={"Authorization": f"Bearer {GLEAN_API_TOKEN}"}, json=payload)
                                            if g_res.status_code == 200: count += 1
                                        
                                        total_indexed += count
                            except Exception as e:
                                logger.error(f"      ⚠️ Error processing table {table_name}: {e}")

    logger.info(f"🚀 SYNC COMPLETE. Total indexed: {total_indexed}")

# Schedule the job every 60 minutes
scheduler = BackgroundScheduler()
scheduler.add_job(run_sync_job, 'interval', minutes=60)
scheduler.start()

@app.route('/')
def home():
    return "Glean PowerBI Connector is RUNNING (Discovery Mode Active)"

@app.route('/sync')
def manual_sync():
    # 🔥 FIX: Run in Background Thread so Render doesn't timeout!
    thread = threading.Thread(target=run_sync_job)
    thread.start()
    return jsonify({"status": "Sync Job Triggered in Background. Watch the Render Logs!"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
