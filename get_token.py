import msal
import os

# ==========================================
# CONFIGURATION (SAFE FOR GITHUB)
# ==========================================
# When running locally, REPLACE these values temporarily.
# DO NOT COMMIT REAL SECRETS TO GIT.
CLIENT_ID = os.getenv("CLIENT_ID") 
CLIENT_SECRET = os.getenv("CLIENT_SECRET") 
TENANT_ID = os.getenv("TENANT_ID") 

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = [
    "https://analysis.windows.net/powerbi/api/Report.Read.All",
    "https://analysis.windows.net/powerbi/api/Dataset.Read.All",
    "https://analysis.windows.net/powerbi/api/Group.Read.All" 
]

app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)

# 1. Generate Login URL
auth_url = app.get_authorization_request_url(SCOPES)
print("\n1. CLICK THIS URL to Log In and Authorize the NEW permissions:")
print(auth_url)

# 2. Get Code
code = input("\n2. Paste the 'code' from the URL here: ")

# 3. Exchange for Token
try:
    result = app.acquire_token_by_authorization_code(code, scopes=SCOPES)
    if "refresh_token" in result:
        print("\n✅ NEW REFRESH TOKEN (Copy this):")
        print(result["refresh_token"])
    else:
        print("\n❌ Error:", result.get("error_description"))
except Exception as e:
    print(f"\n❌ Exception: {e}")
