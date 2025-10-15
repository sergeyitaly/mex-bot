import gspread
import json
import os
from google.oauth2.service_account import Credentials
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def comprehensive_diagnostic():
    """Comprehensive diagnostic for Google Sheets issues"""
    print("🔍 Comprehensive Google Sheets Diagnostic")
    print("=" * 60)
    
    # Check credentials
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        print("❌ GOOGLE_CREDENTIALS_JSON not found in .env")
        return False
    
    try:
        creds_dict = json.loads(creds_json)
        print(f"✅ Credentials parsed successfully")
        print(f"   👤 Service Account: {creds_dict.get('client_email')}")
        print(f"   🏢 Project: {creds_dict.get('project_id')}")
    except Exception as e:
        print(f"❌ Failed to parse credentials: {e}")
        return False
    
    # Test different scopes
    scopes_to_test = [
        ['https://spreadsheets.google.com/feeds'],
        ['https://www.googleapis.com/auth/spreadsheets'],
        ['https://www.googleapis.com/auth/drive.file'],
        ['https://www.googleapis.com/auth/drive']
    ]
    
    for scope in scopes_to_test:
        print(f"\n🧪 Testing scope: {scope[0]}")
        try:
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            client = gspread.authorize(creds)
            
            # Try to list spreadsheets
            try:
                spreadsheets = client.list_spreadsheet_files()
                print(f"   ✅ Can list spreadsheets: {len(spreadsheets)} found")
                
                # Try to create a test sheet
                try:
                    test_name = f"Test Scope {datetime.now().strftime('%H:%M:%S')}"
                    spreadsheet = client.create(test_name)
                    print(f"   ✅ Can CREATE sheets with this scope")
                    
                    # Try to write data
                    worksheet = spreadsheet.get_worksheet(0)
                    worksheet.update('A1', [['Test', 'Success']])
                    print(f"   ✅ Can WRITE data with this scope")
                    
                    # Clean up
                    client.del_spreadsheet(spreadsheet.id)
                    print(f"   ✅ Can DELETE sheets with this scope")
                    
                    print(f"   🎉 SCOPE WORKS PERFECTLY: {scope[0]}")
                    return True
                    
                except Exception as e:
                    error_msg = str(e)
                    if "quota" in error_msg.lower():
                        print(f"   ❌ STORAGE QUOTA EXCEEDED with this scope")
                    elif "permission" in error_msg.lower():
                        print(f"   ❌ PERMISSION DENIED with this scope")
                    else:
                        print(f"   ❌ Create failed: {e}")
                        
            except Exception as e:
                print(f"   ❌ Cannot list spreadsheets: {e}")
                
        except Exception as e:
            print(f"   ❌ Authorization failed: {e}")
    
    return False

def test_minimal_scope():
    """Test with minimal permissions that might work"""
    print("\n🛠️  Testing Minimal Scope Solution")
    print("=" * 40)
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        return False
    
    try:
        creds_dict = json.loads(creds_json)
        
        # Use only the most basic scope
        scope = ['https://www.googleapis.com/auth/drive.file']  # Only access files created by this app
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        print("✅ Authorized with drive.file scope")
        
        # Try to create with a very simple name
        try:
            spreadsheet = client.create("MEXC Bot Test")
            print("✅ SUCCESS! Created spreadsheet with minimal scope")
            print(f"🔗 URL: {spreadsheet.url}")
            
            # Share with your email
            share_email = os.getenv('GOOGLE_SHEET_EMAIL')
            if share_email:
                spreadsheet.share(share_email, perm_type='user', role='writer')
                print(f"✅ Shared with: {share_email}")
            
            return True
            
        except Exception as e:
            print(f"❌ Still cannot create: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Minimal scope failed: {e}")
        return False

def check_project_quotas():
    """Check if there are project-level quotas limiting us"""
    print("\n📊 Checking Project-Level Limitations")
    print("=" * 40)
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        return
    
    try:
        creds_dict = json.loads(creds_json)
        print(f"Project: {creds_dict.get('project_id')}")
        print(f"Service Account: {creds_dict.get('client_email')}")
        
        print("\n💡 Common quota issues:")
        print("   1. Project has exceeded daily create limits")
        print("   2. Service account disabled or deleted")
        print("   3. API not enabled for the project")
        print("   4. Service account lacks 'Editor' role")
        
    except Exception as e:
        print(f"Error: {e}")

def provide_solutions():
    """Provide actionable solutions"""
    print("\n🚀 ACTIONABLE SOLUTIONS")
    print("=" * 40)
    
    print("1. 🕐 WAIT 24 HOURS")
    print("   - Google might have daily creation limits")
    print("   - Try again tomorrow")
    
    print("\n2. 🔧 CREATE NEW SERVICE ACCOUNT")
    print("   - Go to: https://console.cloud.google.com/")
    print("   - IAM & Admin → Service Accounts")
    print("   - Create new service account with 'Editor' role")
    print("   - Download new JSON key")
    
    print("\n3. 📧 USE YOUR PERSONAL ACCOUNT")
    print("   - Go to: https://docs.google.com/spreadsheets")
    print("   - Manually create a sheet named 'MEXC Futures Auto-Update'")
    print("   - Share it with your service account email:")
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            service_email = creds_dict.get('client_email')
            print(f"   📧 Share with: {service_email}")
        except:
            pass
    
    print("\n4. 📊 USE EXCEL EXPORTS")
    print("   - Your bot can still export to Excel files")
    print("   - Use /export command in your bot")
    print("   - Choose '📊 Excel Export' option")

if __name__ == "__main__":
    print("Google Sheets Comprehensive Diagnostic")
    print("=" * 60)
    
    # Run diagnostics
    success = comprehensive_diagnostic()
    
    if not success:
        print("\n🔄 Trying minimal scope approach...")
        success = test_minimal_scope()
    
    if not success:
        check_project_quotas()
        provide_solutions()
    else:
        print("\n🎉 SUCCESS! Your Google Sheets should work now!")
        print("   Run your MEXC bot and use /autosheet command")