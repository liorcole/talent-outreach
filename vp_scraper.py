import pandas as pd
import requests
import re
from github import Github
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- CONFIG ---
GITHUB_TOKEN = "YOUR_GITHUB_TOKEN"
HUNTER_API_KEY = "YOUR_HUNTER_API_KEY"
GOOGLE_SHEET_NAME = "VP Eng Leads"
GOOGLE_CREDS_FILE = "credentials.json"  # Download from Google Cloud Console
PHANTOMBUSTER_CSV = "phantombuster_output.csv"

# --- SETUP GOOGLE SHEETS ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# --- CUSTOM MESSAGE GENERATOR ---
def generate_message(name):
    return f"""{name}! Really inspiring background leading+recruiting top eng talent—I'd love to learn from your experience. I'm a first-time founder (building an agentic fashion discovery platform backed by Bloomberg Beta, 1517 VC, etc) + hiring a VP Eng + scaling. Open to a quick chat?"""

# --- GITHUB EMAIL DISCOVERY ---
def get_email_from_github(github_username):
    g = Github(GITHUB_TOKEN)
    try:
        user = g.get_user(github_username)
        email = user.email
        if not email:
            for repo in user.get_repos():
                try:
                    commits = repo.get_commits()
                    for commit in commits.reversed:
                        sha = commit.sha
                        patch_url = f"https://github.com/{user.login}/{repo.name}/commit/{sha}.patch"
                        r = requests.get(patch_url)
                        match = re.search(r'From:.*<(.+?)>', r.text)
                        if match:
                            return match.group(1)
                except: continue
    except:
        return None
    return email

# --- EMAIL VIA HUNTER ---
def hunter_lookup(domain):
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
    try:
        r = requests.get(url)
        data = r.json()
        return data['data']['emails'][0]['value']
    except:
        return None

# --- MAIN SCRIPT ---
df = pd.read_csv(PHANTOMBUSTER_CSV)
for _, row in df.iterrows():
    name = row.get('name') or row.get('Name')
    linkedin = row.get('linkedin') or row.get('LinkedIn')
    title = row.get('title') or row.get('Title')
    company = row.get('companyName') or row.get('Company')
    domain = f"{company.lower().replace(' ', '')}.com"
    message = generate_message(name)

    email = hunter_lookup(domain)
    if not email and row.get('github'):
        email = get_email_from_github(row['github'])

    sheet.append_row([name, linkedin, title, company, message, email or "Not found"])

print("✅ Done: Data added to Google Sheet")
