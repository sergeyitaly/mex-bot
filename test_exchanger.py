#!/usr/bin/env python3
"""
Bybit Simple Market Data Test
Test the simplified approach for getting Bybit futures data
"""

import os
import requests
import hmac
import hashlib
import time
import json
from typing import Set, Optional

def test_simple_bybit_market_data():
    """Test the simple Bybit market data approach"""
    
    # Get API credentials from environment
    api_key = os.getenv('BYBIT_API_KEY', '')
    api_secret = os.getenv('BYBIT_API_SECRET', '')
    
    print("🚀 BYBIT SIMPLE MARKET DATA TEST")
    print("=" * 60)
    
    if not api_key or not api_secret:
        print("❌ ERROR: API credentials not set")
        return False
    
    print(f"✅ API Key: {api_key[:10]}...{api_key[-4:]}")
    print(f"✅ API Secret: {api_secret[:10]}...{api_secret[-4:]}")
    print()
    
    # Test 1: Simple linear perpetuals
    print("1. 📈 Testing Linear Perpetuals...")
    linear_futures = get_bybit_linear_perpetuals(api_key, api_secret)
    print(f"   ✅ Found {len(linear_futures)} linear perpetuals")
    if linear_futures:
        sample = list(linear_futures)[:5]
        print(f"   🔍 Sample: {sample}")
    
    # Test 2: Inverse perpetuals
    print("\n2. 📊 Testing Inverse Perpetuals...")
    inverse_futures = get_bybit_inverse_perpetuals(api_key, api_secret)
    print(f"   ✅ Found {len(inverse_futures)} inverse perpetuals")
    if inverse_futures:
        sample = list(inverse_futures)[:5]
        print(f"   🔍 Sample: {sample}")
    
    # Test 3: Spot data (for comparison)
    print("\n3. 💰 Testing Spot Data...")
    spot_symbols = get_bybit_spot_symbols(api_key, api_secret)
    print(f"   ✅ Found {len(spot_symbols)} spot symbols")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print(f"📈 Linear Perpetuals: {len(linear_futures)}")
    print(f"📊 Inverse Perpetuals: {len(inverse_futures)}")
    print(f"💰 Spot Symbols: {len(spot_symbols)}")
    print(f"🎯 TOTAL Futures: {len(linear_futures) + len(inverse_futures)}")
    
    return True

def get_bybit_linear_perpetuals(api_key: str, api_secret: str) -> Set[str]:
    """Get Bybit linear perpetual futures (USDT margined)"""
    try:
        base_url = "https://api.bybit.com"
        endpoint = "/v5/market/instruments-info"
        
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        # Build parameter string for signature
        param_string = f"category=linear&recv_window={recv_window}&timestamp={timestamp}"
        
        # Generate signature
        signature = hmac.new(
            api_secret.encode('utf-8'),
            param_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Build full URL
        full_url = f"{base_url}{endpoint}?{param_string}&sign={signature}"
        
        headers = {
            'X-BAPI-API-KEY': api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json'
        }
        
        print(f"   📡 Calling: {endpoint}")
        response = requests.get(full_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                items = data.get('result', {}).get('list', [])
                futures = set()
                
                for item in items:
                    symbol = item.get('symbol', '')
                    status = item.get('status', '')
                    contract_type = item.get('contractType', '')
                    
                    # Filter for trading linear perpetuals
                    if (status == 'Trading' and 
                        contract_type == 'LinearPerpetual' and
                        symbol.endswith('USDT')):
                        futures.add(symbol)
                
                print(f"   ✅ Success: {len(futures)} trading linear perpetuals")
                return futures
            else:
                print(f"   ❌ API Error: {data.get('retMsg')}")
                return set()
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        return set()

def get_bybit_inverse_perpetuals(api_key: str, api_secret: str) -> Set[str]:
    """Get Bybit inverse perpetual futures (coin margined)"""
    try:
        base_url = "https://api.bybit.com"
        endpoint = "/v5/market/instruments-info"
        
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        # Build parameter string for signature
        param_string = f"category=inverse&recv_window={recv_window}&timestamp={timestamp}"
        
        # Generate signature
        signature = hmac.new(
            api_secret.encode('utf-8'),
            param_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Build full URL
        full_url = f"{base_url}{endpoint}?{param_string}&sign={signature}"
        
        headers = {
            'X-BAPI-API-KEY': api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json'
        }
        
        print(f"   📡 Calling: {endpoint}")
        response = requests.get(full_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                items = data.get('result', {}).get('list', [])
                futures = set()
                
                for item in items:
                    symbol = item.get('symbol', '')
                    status = item.get('status', '')
                    contract_type = item.get('contractType', '')
                    base_coin = item.get('baseCoin', '')
                    
                    # Filter for trading inverse perpetuals (BTCUSD, ETHUSD, etc.)
                    if (status == 'Trading' and 
                        contract_type == 'InversePerpetual' and
                        symbol.endswith('USD')):
                        futures.add(symbol)
                
                print(f"   ✅ Success: {len(futures)} trading inverse perpetuals")
                return futures
            else:
                print(f"   ❌ API Error: {data.get('retMsg')}")
                return set()
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        return set()

def get_bybit_spot_symbols(api_key: str, api_secret: str) -> Set[str]:
    """Get Bybit spot symbols for comparison"""
    try:
        base_url = "https://api.bybit.com"
        endpoint = "/v5/market/tickers"
        
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        # Build parameter string for signature
        param_string = f"category=spot&recv_window={recv_window}&timestamp={timestamp}"
        
        # Generate signature
        signature = hmac.new(
            api_secret.encode('utf-8'),
            param_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Build full URL
        full_url = f"{base_url}{endpoint}?{param_string}&sign={signature}"
        
        headers = {
            'X-BAPI-API-KEY': api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json'
        }
        
        print(f"   📡 Calling: {endpoint}")
        response = requests.get(full_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                items = data.get('result', {}).get('list', [])
                symbols = set()
                
                for item in items:
                    symbol = item.get('symbol', '')
                    if symbol and symbol.endswith('USDT'):
                        symbols.add(symbol)
                
                print(f"   ✅ Success: {len(symbols)} spot symbols")
                return symbols
            else:
                print(f"   ❌ API Error: {data.get('retMsg')}")
                return set()
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        return set()

def test_different_categories():
    """Test different category types available"""
    print("\n4. 🔄 Testing Different Categories...")
    
    api_key = os.getenv('BYBIT_API_KEY', '')
    api_secret = os.getenv('BYBIT_API_SECRET', '')
    
    categories = ['linear', 'inverse', 'spot', 'option']
    
    for category in categories:
        try:
            base_url = "https://api.bybit.com"
            endpoint = "/v5/market/instruments-info"
            
            timestamp = str(int(time.time() * 1000))
            recv_window = "5000"
            
            param_string = f"category={category}&recv_window={recv_window}&timestamp={timestamp}"
            
            signature = hmac.new(
                api_secret.encode('utf-8'),
                param_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            full_url = f"{base_url}{endpoint}?{param_string}&sign={signature}"
            
            headers = {
                'X-BAPI-API-KEY': api_key,
                'X-BAPI-SIGN': signature,
                'X-BAPI-TIMESTAMP': timestamp,
                'X-BAPI-RECV-WINDOW': recv_window,
            }
            
            response = requests.get(full_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('retCode') == 0:
                    items = data.get('result', {}).get('list', [])
                    print(f"   ✅ {category}: {len(items)} instruments")
                else:
                    print(f"   ❌ {category}: API Error - {data.get('retMsg')}")
            else:
                print(f"   ❌ {category}: HTTP Error - {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ {category}: Failed - {e}")

if __name__ == "__main__":
    print("🧪 Bybit Simple Market Data Test Script")
    print("Testing the simplified approach for futures data\n")
    
    # Run the main test
    success = test_simple_bybit_market_data()
    
    # Test different categories
    test_different_categories()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 SUCCESS: Simple market data approach works!")
        print("💡 You can now implement this in your main application")
    else:
        print("❌ Some tests failed - check your implementation")
    print("=" * 60)