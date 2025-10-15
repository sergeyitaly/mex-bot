import gspread
import json
import os
from google.oauth2.service_account import Credentials
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def test_specific_spreadsheet():
    """Test access to the specific spreadsheet URL"""
    print("🔍 Testing Specific Spreadsheet Access")
    print("=" * 50)
    print("📊 Target Sheet: https://docs.google.com/spreadsheets/d/1Axc4-JmvDtYV-uWhVxNqDHPOaXbwo2rJqnBKgnxGJY0/edit?gid=0#gid=0")
    print()
    
    # Check credentials
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        print("❌ GOOGLE_CREDENTIALS_JSON not found in .env")
        return False
    
    try:
        creds_dict = json.loads(creds_json)
        service_email = creds_dict.get('client_email')
        print(f"✅ Service Account: {service_email}")
    except Exception as e:
        print(f"❌ Failed to parse credentials: {e}")
        return False
    
    # Test different scopes for accessing existing sheet
    scopes_to_test = [
        ['https://www.googleapis.com/auth/spreadsheets'],
        ['https://www.googleapis.com/auth/drive'],
        ['https://www.googleapis.com/auth/drive.file'],
        ['https://spreadsheets.google.com/feeds']
    ]
    
    spreadsheet_id = "1Axc4-JmvDtYV-uWhVxNqDHPOaXbwo2rJqnBKgnxGJY0"
    
    for scope in scopes_to_test:
        print(f"\n🧪 Testing scope: {scope[0]}")
        try:
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            client = gspread.authorize(creds)
            
            # Try to open the specific spreadsheet
            try:
                spreadsheet = client.open_by_key(spreadsheet_id)
                print(f"   ✅ Can ACCESS the spreadsheet")
                print(f"   📝 Title: {spreadsheet.title}")
                
                # Try to get worksheets
                try:
                    worksheets = spreadsheet.worksheets()
                    print(f"   📋 Worksheets: {len(worksheets)} found")
                    for ws in worksheets:
                        print(f"      - {ws.title} (id: {ws.id})")
                    
                    # Try to read data
                    try:
                        worksheet = spreadsheet.get_worksheet(0)  # First worksheet
                        data = worksheet.get_all_values()
                        print(f"   📊 Can READ data: {len(data)} rows found")
                        
                        # Try to write data
                        try:
                            test_cell = f"BotTest{datetime.now().strftime('%H%M%S')}"
                            worksheet.update('A1', [[test_cell]])
                            print(f"   ✏️  Can WRITE data - updated A1 with: {test_cell}")
                            
                            # Verify write worked
                            updated_value = worksheet.acell('A1').value
                            if updated_value == test_cell:
                                print(f"   ✅ Write verification PASSED")
                            else:
                                print(f"   ⚠️  Write verification: expected '{test_cell}', got '{updated_value}'")
                            
                            print(f"   🎉 FULL ACCESS WORKS with scope: {scope[0]}")
                            return True
                            
                        except Exception as e:
                            print(f"   ❌ Cannot WRITE data: {e}")
                            
                    except Exception as e:
                        print(f"   ❌ Cannot READ data: {e}")
                        
                except Exception as e:
                    print(f"   ❌ Cannot list worksheets: {e}")
                
            except Exception as e:
                error_msg = str(e)
                if "not found" in error_msg.lower():
                    print(f"   ❌ Spreadsheet NOT FOUND - check URL and sharing permissions")
                elif "permission" in error_msg.lower():
                    print(f"   ❌ PERMISSION DENIED - share with service account: {service_email}")
                else:
                    print(f"   ❌ Cannot access spreadsheet: {e}")
                
        except Exception as e:
            print(f"   ❌ Authorization failed: {e}")
    
    return False

def check_sharing_permissions():
    """Check what permissions are needed"""
    print("\n🔐 Sharing Permission Check")
    print("=" * 40)
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        return
    
    try:
        creds_dict = json.loads(creds_json)
        service_email = creds_dict.get('client_email')
        
        print(f"📧 Service Account Email: {service_email}")
        print(f"🔗 Spreadsheet ID: 1Axc4-JmvDtYV-uWhVxNqDHPOaXbwo2rJqnBKgnxGJY0")
        print()
        print("📋 To fix permission issues:")
        print("1. Open your Google Sheet")
        print("2. Click 'Share' button (top-right)")
        print("3. Add this email as EDITOR:")
        print(f"   👉 {service_email}")
        print("4. Click 'Send'")
        print()
        print("🔄 Then run this diagnostic again")
        
    except Exception as e:
        print(f"Error: {e}")

def test_spreadsheet_creation():
    """Test if we can create new spreadsheets as fallback"""
    print("\n🆕 Testing Spreadsheet Creation (Fallback)")
    print("=" * 45)
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        return False
    
    try:
        creds_dict = json.loads(creds_json)
        scope = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        try:
            test_name = f"MEXC Test {datetime.now().strftime('%H:%M:%S')}"
            spreadsheet = client.create(test_name)
            print(f"✅ Can CREATE new spreadsheets")
            print(f"🔗 New sheet URL: {spreadsheet.url}")
            
            # Clean up
            client.del_spreadsheet(spreadsheet.id)
            print("✅ Cleanup successful")
            return True
            
        except Exception as e:
            print(f"❌ Cannot create spreadsheets: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Creation test failed: {e}")
        return False

def provide_focused_solutions(has_sheet_access, can_create_sheets):
    """Provide solutions based on test results"""
    print("\n🎯 FOCUSED SOLUTIONS")
    print("=" * 30)
    
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    service_email = None
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            service_email = creds_dict.get('client_email')
        except:
            pass
    
    if has_sheet_access:
        print("✅ Your bot can access the existing spreadsheet!")
        print("   Use /autosheet command in your bot to start auto-updating")
        
    elif can_create_sheets:
        print("🔄 Your bot can create new sheets but cannot access the existing one")
        print("   Either:")
        print("   1. Share the existing sheet with your service account:")
        if service_email:
            print(f"      📧 {service_email}")
        print()
        print("   2. Or let the bot create a new auto-update sheet")
        print("      Use /autosheet command")
        
    else:
        print("❌ Your bot cannot access Google Sheets")
        print()
        print("🔧 Quick fixes:")
        print("1. Check if Google Sheets API is enabled")
        print("   - Go to: https://console.cloud.google.com/apis/library/sheets.googleapis.com")
        print("2. Check if Drive API is enabled") 
        print("   - Go to: https://console.cloud.google.com/apis/library/drive.googleapis.com")
        print("3. Verify service account has Editor role")
        print("4. Wait 24h if you hit API quotas")
        print()
        print("📊 Alternative: Use Excel exports with /export command")

if __name__ == "__main__":
    print("Google Sheets Specific Diagnostic")
    print("=" * 50)
    print("Testing your specific spreadsheet...")
    print()
    
    # Test access to existing spreadsheet
    has_sheet_access = test_specific_spreadsheet()
    
    # Test creation capability
    can_create_sheets = test_spreadsheet_creation()
    
    # Check sharing if we can't access but can create
    if not has_sheet_access and can_create_sheets:
        check_sharing_permissions()
    
    # Provide solutions
    provide_focused_solutions(has_sheet_access, can_create_sheets)
    
    print("\n" + "=" * 50)
    if has_sheet_access:
        print("🎉 SUCCESS! Your bot can work with the existing spreadsheet!")
    elif can_create_sheets:
        print("⚠️  Can create sheets but need to fix permissions for existing sheet")
    else:
        print("❌ Need to fix Google Sheets setup")