import pandas as pd
import requests
import re
import os
from github import Github
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import numpy as np

# --- CONFIG ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GOOGLE_SHEET_NAME = "VP Eng Leads"
GOOGLE_CREDS_FILE = "credentials.json"
PHANTOMBUSTER_CSV = "phantombuster_output.csv"

# --- SETUP GOOGLE SHEETS ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# --- BULLETPROOF VALUE EXTRACTION ---
def extract_clean_value(row, column_names):
    """
    Absolutely bulletproof value extraction that handles ANY pandas weirdness
    Returns clean string or None - NEVER returns anything that will cause AttributeError
    """
    for col_name in column_names:
        if col_name in row.index:
            raw_value = row[col_name]
            
            # Handle ALL possible problematic cases
            if raw_value is None:
                continue
            if pd.isna(raw_value):
                continue
            if isinstance(raw_value, float) and np.isnan(raw_value):
                continue
                
            # Force to string and clean
            try:
                clean_value = str(raw_value).strip()
                if clean_value and clean_value.lower() not in ['nan', 'none', 'null', '']:
                    return clean_value
            except:
                continue
    
    return None

# --- TITLE FILTERING ---
def is_target_title(title):
    """Check if title matches our target engineering leadership roles"""
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Exact matches and common variations
    target_titles = [
        'vp of engineering',
        'vp engineering', 
        'vice president of engineering',
        'engineering manager',
        'cto',
        'director of engineering'
    ]
    
    # Check exact matches
    for target in target_titles:
        if target in title_lower:
            return True
    
    # Check for any title containing "vp"
    if 'vp' in title_lower:
        return True
        
    return False

# --- MESSAGE GENERATOR ---
def generate_message(name, companies):
    """Generate personalized message with first name and company list"""
    # Extract first name
    first_name = name.split()[0] if name else "there"
    
    # Format companies list
    if companies:
        companies_text = ", ".join(companies[:3])  # Take first 3 companies
    else:
        companies_text = "your background"
    
    return f"""{first_name}! Really inspiring background leading + recruiting top eng talent ({companies_text}). I'm a first-time founder hiring a VP Eng + scaling (building an AI fashion discovery platform $ by Bloomberg Beta, 1517, etc). Would love to learn from you—totally get if now's not a good time!"""

# --- GITHUB EMAIL DISCOVERY ---
def get_email_from_github(github_username):
    if not GITHUB_TOKEN:
        return None
    
    try:
        g = Github(GITHUB_TOKEN)
        user = g.get_user(github_username)
        return user.email
    except:
        return None

# --- MAIN SCRIPT ---
def main():
    try:
        # Read CSV and immediately replace ALL problematic values
        df = pd.read_csv(PHANTOMBUSTER_CSV)
        df = df.replace([np.nan, None, 'nan', 'NaN', 'None', 'null'], '')
        
        print(f"📊 Processing {len(df)} rows from CSV")
        print(f"📋 Available columns: {list(df.columns)}")
        
        successful_rows = 0
        
        for index in range(len(df)):
            print(f"\n🔄 Processing row {index + 1}/{len(df)}")
            
            # Get the row as a Series
            row = df.iloc[index]
            
            # Extract values with bulletproof method
            name = extract_clean_value(row, ['fullName', 'name', 'Name', 'full_name'])
            company = extract_clean_value(row, ['company', 'companyName', 'Company', 'employer'])
            title = extract_clean_value(row, ['jobTitle', 'title', 'Title', 'position'])
            linkedin = extract_clean_value(row, ['profileUrl', 'linkedin', 'LinkedIn', 'url'])
            
            # Try to extract work history/companies - ONLY from safe fields
            companies = []
            
            # Get main company (avoid URL fields)
            current_company = extract_clean_value(row, ['company'])
            if current_company and not current_company.startswith('http'):
                companies.append(current_company)
            
            # Get second company if it exists
            company2 = extract_clean_value(row, ['company2']) 
            if (company2 and 
                not company2.startswith('http') and 
                company2 not in companies):
                companies.append(company2)
            
            print(f"📝 Extracted - Name: '{name}', Company: '{company}', Companies: {companies}")
            
            # Skip if missing critical data
            if not name:
                print(f"❌ Skipping row {index + 1} - No valid name found")
                continue
                
            if not company:
                print(f"❌ Skipping row {index + 1} - No valid company found")
                continue
            
            # Filter by title - ONLY process target engineering roles
            if not is_target_title(title):
                print(f"❌ Skipping row {index + 1} - Title '{title}' is not a target engineering role")
                continue
            
            print(f"✅ Valid data found: {name} at {company}")
            
            message = generate_message(name, companies)
            
            # Add to Google Sheet
            try:
                sheet.append_row([
                    name,
                    linkedin or "",
                    title or "",
                    company,
                    message,
                    "Not found"  # Email placeholder
                ])
                successful_rows += 1
                print(f"✅ Added {name} to Google Sheet")
                
            except Exception as e:
                print(f"❌ Failed to add {name} to Google Sheet: {e}")
        
        print(f"\n🎉 Completed! Successfully processed {successful_rows} rows")
        
    except FileNotFoundError:
        print(f"❌ CSV file '{PHANTOMBUSTER_CSV}' not found")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
