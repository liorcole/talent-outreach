import pandas as pd
import requests
import re
import os
import json
from github import Github
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import numpy as np
import time

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

# --- EMAIL VALIDATION ---
def is_valid_email(email):
    """Validate email format"""
    if not email:
        return False
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email) is not None

# --- BULLETPROOF VALUE EXTRACTION ---
def extract_clean_value(row, column_names):
    """Absolutely bulletproof value extraction that handles ANY pandas weirdness"""
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

# --- MAIN SCRIPT ---
def main():
    try:
        # Read CSV and immediately replace ALL problematic values
        df = pd.read_csv(PHANTOMBUSTER_CSV)
        df = df.replace([np.nan, None, 'nan', 'NaN', 'None', 'null'], '')
        
        print(f"📊 Processing {len(df)} rows from CSV")
        print(f"📋 Available columns: {list(df.columns)}")
        
        # Collect all people data first
        people_data = []
        
        for index in range(len(df)):
            row = df.iloc[index]
            
            # Extract values with bulletproof method
            name = extract_clean_value(row, ['fullName', 'name', 'Name', 'full_name'])
            company = extract_clean_value(row, ['company', 'companyName', 'Company', 'employer'])
            title = extract_clean_value(row, ['jobTitle', 'title', 'Title', 'position'])
            linkedin = extract_clean_value(row, ['profileUrl', 'linkedin', 'LinkedIn', 'url'])
            
            # Try to extract work history/companies
            companies = []
            current_company = extract_clean_value(row, ['company'])
            if current_company and not current_company.startswith('http'):
                companies.append(current_company)
            
            company2 = extract_clean_value(row, ['company2']) 
            if (company2 and 
                not company2.startswith('http') and 
                company2 not in companies):
                companies.append(company2)
            
            # Skip if missing critical data or not target title
            if not name or not company or not is_target_title(title):
                continue
            
            people_data.append({
                'name': name,
                'company': company,
                'title': title,
                'linkedin_url': linkedin,
                'companies': companies,
                'row_index': index
            })
        
        print(f"📋 Found {len(people_data)} valid engineering leaders")
        
        # Add to Google Sheet
        print("\n🚀 Adding people to Google Sheets...")
        successful_rows = 0
        for person in people_data:
            try:
                message = generate_message(person['name'], person['companies'])
                
                sheet.append_row([
                    person['name'],
                    person.get('linkedin_url', ''),
                    person.get('title', ''),
                    person['company'],
                    message,
                    ''  # Empty email column for manual entry later
                ])
                successful_rows += 1
                print(f"✅ Added {person['name']} to Google Sheet")
                
            except Exception as e:
                print(f"❌ Failed to add {person['name']} to Google Sheet: {e}")
        
        # Summary
        print(f"\n📊 FINAL RESULTS:")
        print(f"   📄 Added to Google Sheet: {successful_rows}")
        print(f"   📝 Email column left blank for manual entry")
        
        # Save detailed results
        with open('processed_results.json', 'w') as f:
            json.dump(people_data, f, indent=2)
        
        print(f"   💾 Detailed results saved to 'processed_results.json'")
        
    except FileNotFoundError:
        print(f"❌ CSV file '{PHANTOMBUSTER_CSV}' not found")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
