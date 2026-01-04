import msal
import os

# ==========================================
# CONFIGURATION (SECRETS REMOVED)
# ==========================================
# If running locally, you must set these Env Vars or temporarily paste them, 
# but DO NOT save/commit them to GitHub.
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID") 

if not CLIENT_ID or not TENANT_ID:
    print("⚠️  MISSING CONFIG: Please set CLIENT_ID and TENANT_ID environment variables.")
    # For local testing ONLY, you can uncomment and paste below, but undo before pushing:
    # CLIENT_ID = "paste_your_id_here"
    # TENANT_ID = "paste_your_tenant_here"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"

# We removed "offline_access" from scopes to avoid the "Duplicate" error
SCOPES = ["https://analysis.windows.net/powerbi/api/Report.Read.All", 
          "https://analysis.windows.net/powerbi/api/Dataset.Read.All"]

app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

# Start the login flow
flow = app.initiate_device_flow(scopes=SCOPES)
if "user_code" not in flow:
    print("❌ Failed to create flow. Check your Client ID / Tenant ID.")
    exit(1)

print(f"\n******************************************************")
print(f"ACTION REQUIRED:")
print(f"1. Click: {flow['verification_uri']}")
print(f"2. Enter Code: {flow['user_code']}")
print(f"******************************************************\n")

# Wait for you to login
result = app.acquire_token_by_device_flow(flow)

if "refresh_token" in result:
    print("\n✅ SUCCESS! COPY THE TOKEN BELOW (IT IS VERY LONG):")
    print("---------------------------------------------------")
    print(result["refresh_token"])
    print("---------------------------------------------------")
else:
    print("❌ Failed.")
    print(result.get("error_description"))