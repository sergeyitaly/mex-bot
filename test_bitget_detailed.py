#!/usr/bin/env python3
"""
BitGet Detailed Tester
"""

import requests
import json

def test_bitget_detailed():
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    print("🎯 DETAILED BITGET TEST")
    print("-" * 40)
    
    # Test USDT-FUTURES
    url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
    response = session.get(url, timeout=10)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Response code: {data.get('code')}")
        print(f"✅ Response message: {data.get('msg')}")
        
        if data.get('code') == '00000':
            items = data.get('data', [])
            print(f"📊 Total items: {len(items)}")
            
            # Show first 3 items in detail
            for i, item in enumerate(items[:3]):
                print(f"\n📋 Item {i+1}:")
                for key, value in item.items():
                    print(f"   {key}: {value}")
            
            # Count by status
            status_count = {}
            for item in items:
                status = item.get('status')
                status_count[status] = status_count.get(status, 0) + 1
            
            print(f"\n📊 Status distribution: {status_count}")
            
            # Get all symbols with status 'normal'
            normal_symbols = [item.get('symbol') for item in items if item.get('status') == 'normal']
            print(f"✅ Normal status symbols: {len(normal_symbols)}")
            print(f"📝 Sample: {normal_symbols[:10]}")
            
            return len(normal_symbols)
    
    return 0

if __name__ == "__main__":
    test_bitget_detailed()