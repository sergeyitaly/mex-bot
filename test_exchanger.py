#!/usr/bin/env python3
"""
Exchange API Debug Script
Detailed debugging for each exchange API response
"""

import requests
import json
import time
from datetime import datetime

class ExchangeAPIDebugger:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
        self.timeout = 30

    def debug_binance(self):
        """Debug Binance API in detail"""
        print("🔍 DEBUGGING BINANCE API")
        print("=" * 60)
        
        try:
            # USDⓈ-M Futures
            url1 = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            print(f"📡 URL: {url1}")
            response1 = self.session.get(url1, timeout=self.timeout)
            print(f"📊 Status Code: {response1.status_code}")
            
            if response1.status_code == 200:
                data = response1.json()
                symbols = data.get('symbols', [])
                print(f"📈 Total symbols: {len(symbols)}")
                
                # Analyze first 5 symbols in detail
                print("\n🔬 ANALYZING FIRST 5 SYMBOLS:")
                for i, symbol in enumerate(symbols[:5]):
                    print(f"\n--- Symbol {i+1} ---")
                    for key, value in symbol.items():
                        print(f"  {key}: {value}")
                
                # Count all contract types
                contract_types = {}
                status_types = {}
                for symbol in symbols:
                    contract_type = symbol.get('contractType')
                    status = symbol.get('status')
                    contract_types[contract_type] = contract_types.get(contract_type, 0) + 1
                    status_types[status] = status_types.get(status, 0) + 1
                
                print(f"\n📊 ALL CONTRACT TYPES: {contract_types}")
                print(f"📊 ALL STATUS TYPES: {status_types}")
                
                # Find all perpetuals
                perpetuals = []
                for symbol in symbols:
                    if symbol.get('contractType') == 'PERPETUAL':
                        perpetuals.append({
                            'symbol': symbol.get('symbol'),
                            'status': symbol.get('status'),
                            'contractType': symbol.get('contractType')
                        })
                
                print(f"\n🎯 FOUND {len(perpetuals)} PERPETUALS:")
                for p in perpetuals[:10]:  # Show first 10
                    print(f"  {p}")
                    
                trading_perpetuals = [p for p in perpetuals if p['status'] == 'TRADING']
                print(f"\n✅ TRADING PERPETUALS: {len(trading_perpetuals)}")
                
            else:
                print(f"❌ HTTP Error: {response1.status_code}")
                print(f"Response text: {response1.text}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
            import traceback
            traceback.print_exc()

    def debug_bybit(self):
        """Debug Bybit API in detail"""
        print("\n🔍 DEBUGGING BYBIT API")
        print("=" * 60)
        
        try:
            # Linear Futures
            url1 = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            print(f"📡 URL: {url1}")
            response1 = self.session.get(url1, timeout=self.timeout)
            print(f"📊 Status Code: {response1.status_code}")
            
            if response1.status_code == 200:
                data = response1.json()
                print(f"📊 API Code: {data.get('retCode')}")
                print(f"📊 API Message: {data.get('retMsg')}")
                
                if data.get('retCode') == 0:
                    items = data.get('result', {}).get('list', [])
                    print(f"📈 Total items: {len(items)}")
                    
                    # Analyze first 5 items in detail
                    print("\n🔬 ANALYZING FIRST 5 ITEMS:")
                    for i, item in enumerate(items[:5]):
                        print(f"\n--- Item {i+1} ---")
                        for key, value in item.items():
                            print(f"  {key}: {value}")
                    
                    # Count all contract types and statuses
                    contract_types = {}
                    status_types = {}
                    symbol_types = {}
                    
                    for item in items:
                        contract_type = item.get('contractType')
                        status = item.get('status')
                        symbol_type = item.get('symbolType', 'N/A')
                        
                        contract_types[contract_type] = contract_types.get(contract_type, 0) + 1
                        status_types[status] = status_types.get(status, 0) + 1
                        symbol_types[symbol_type] = symbol_types.get(symbol_type, 0) + 1
                    
                    print(f"\n📊 ALL CONTRACT TYPES: {contract_types}")
                    print(f"📊 ALL STATUS TYPES: {status_types}")
                    print(f"📊 ALL SYMBOL TYPES: {symbol_types}")
                    
                    # Find all perpetual-like contracts
                    perpetual_candidates = []
                    for item in items:
                        if item.get('status') == 'Trading':
                            perpetual_candidates.append({
                                'symbol': item.get('symbol'),
                                'status': item.get('status'),
                                'contractType': item.get('contractType'),
                                'symbolType': item.get('symbolType', 'N/A')
                            })
                    
                    print(f"\n🎯 FOUND {len(perpetual_candidates)} TRADING ITEMS:")
                    for p in perpetual_candidates[:10]:
                        print(f"  {p}")
                        
                else:
                    print(f"❌ API Error: {data.get('retMsg')}")
            else:
                print(f"❌ HTTP Error: {response1.status_code}")
                print(f"Response text: {response1.text}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
            import traceback
            traceback.print_exc()

    def debug_bitget(self):
        """Debug Bitget API in detail"""
        print("\n🔍 DEBUGGING BITGET API")
        print("=" * 60)
        
        try:
            # USDT Futures
            url1 = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            print(f"📡 URL: {url1}")
            response1 = self.session.get(url1, timeout=self.timeout)
            print(f"📊 Status Code: {response1.status_code}")
            
            if response1.status_code == 200:
                data = response1.json()
                print(f"📊 API Code: {data.get('code')}")
                print(f"📊 API Message: {data.get('msg')}")
                
                if data.get('code') == '00000':
                    items = data.get('data', [])
                    print(f"📈 Total items: {len(items)}")
                    
                    # Analyze first 5 items in detail
                    print("\n🔬 ANALYZING FIRST 5 ITEMS:")
                    for i, item in enumerate(items[:5]):
                        print(f"\n--- Item {i+1} ---")
                        for key, value in item.items():
                            print(f"  {key}: {value}")
                    
                    # Count all symbol types and statuses
                    symbol_types = {}
                    status_types = {}
                    product_types = {}
                    
                    for item in items:
                        symbol_type = item.get('symbolType')
                        status = item.get('status')
                        product_type = item.get('productType', 'N/A')
                        
                        symbol_types[symbol_type] = symbol_types.get(symbol_type, 0) + 1
                        status_types[status] = status_types.get(status, 0) + 1
                        product_types[product_type] = product_types.get(product_type, 0) + 1
                    
                    print(f"\n📊 ALL SYMBOL TYPES: {symbol_types}")
                    print(f"📊 ALL STATUS TYPES: {status_types}")
                    print(f"📊 ALL PRODUCT TYPES: {product_types}")
                    
                    # Find all perpetual-like contracts
                    perpetual_candidates = []
                    for item in items:
                        if item.get('status') == 'normal':
                            perpetual_candidates.append({
                                'symbol': item.get('symbol'),
                                'status': item.get('status'),
                                'symbolType': item.get('symbolType'),
                                'productType': item.get('productType', 'N/A')
                            })
                    
                    print(f"\n🎯 FOUND {len(perpetual_candidates)} NORMAL STATUS ITEMS:")
                    for p in perpetual_candidates[:10]:
                        print(f"  {p}")
                        
                else:
                    print(f"❌ API Error: {data.get('msg')}")
            else:
                print(f"❌ HTTP Error: {response1.status_code}")
                print(f"Response text: {response1.text}")
                
        except Exception as e:
            print(f"💥 Error: {e}")
            import traceback
            traceback.print_exc()

    def test_network_connectivity(self):
        """Test basic network connectivity to exchanges"""
        print("\n🌐 TESTING NETWORK CONNECTIVITY")
        print("=" * 60)
        
        endpoints = {
            'Binance': 'https://fapi.binance.com/fapi/v1/ping',
            'Bybit': 'https://api.bybit.com/v5/market/time',
            'BitGet': 'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES'
        }
        
        for name, url in endpoints.items():
            try:
                start_time = time.time()
                response = self.session.get(url.split('?')[0] if '?' in url else url, timeout=10)
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    print(f"✅ {name}: Connected ({response_time:.2f}s)")
                else:
                    print(f"⚠️  {name}: HTTP {response.status_code} ({response_time:.2f}s)")
                    
            except Exception as e:
                print(f"❌ {name}: Connection failed - {e}")

    def run_complete_debug(self):
        """Run complete debugging session"""
        print("🚀 STARTING COMPLETE EXCHANGE API DEBUG")
        print("=" * 80)
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Test network connectivity first
        self.test_network_connectivity()
        
        # Debug each exchange
        self.debug_binance()
        self.debug_bybit()
        self.debug_bitget()
        
        print("\n" + "=" * 80)
        print("🎯 DEBUGGING COMPLETE")
        print(f"🕐 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main function"""
    debugger = ExchangeAPIDebugger()
    
    try:
        debugger.run_complete_debug()
        
        # Save raw responses for further analysis
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        print(f"\n💾 Debug session completed. Check output above for issues.")
        print(f"📝 If problems persist, save this output to: debug_output_{timestamp}.txt")
        
    except KeyboardInterrupt:
        print("\n⏹️  Debug interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()