import msal
import os

# ==========================================
# 🔐 CONFIGURATION (Env Vars)
# ==========================================
# Run this locally by exporting these variables first, 
# or temporarily hardcoding them for a one-time run.
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")

# Requesting Admin Scanner permissions explicitly
SCOPES = [
    "https://analysis.windows.net/powerbi/api/Tenant.Read.All",
    "https://analysis.windows.net/powerbi/api/Report.Read.All",
    "https://analysis.windows.net/powerbi/api/Group.Read.All",
    "https://analysis.windows.net/powerbi/api/Dataset.Read.All"
]

def get_new_refresh_token():
    if not CLIENT_ID or not TENANT_ID:
        print("❌ Error: Please set CLIENT_ID and TENANT_ID environment variables.")
        return

    # Use PublicClient for Device Flow (No Client Secret needed)
    app = msal.PublicClientApplication(
        CLIENT_ID, 
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )

    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        print("❌ Failed to create device flow.")
        return

    print(f"\n⚠️ ACTION REQUIRED:")
    print(f"1. Go to: {flow['verification_uri']}")
    print(f"2. Enter Code: {flow['user_code']}")
    print("3. Sign in with your Admin account\n")

    result = app.acquire_token_by_device_flow(flow)

    if "refresh_token" in result:
        print("\n✅ NEW REFRESH TOKEN GENERATED:")
        print("---------------------------------------------------")
        print(result["refresh_token"])
        print("---------------------------------------------------")
        print("👉 Copy this token into your Render Environment Variables as 'REFRESH_TOKEN'")
    else:
        print(f"❌ Error: {result.get('error_description')}")

if __name__ == "__main__":
    get_new_refresh_token()
