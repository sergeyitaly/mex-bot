#!/usr/bin/env python3
"""
Exchange API Parser Tester
Tests the actual data parsing logic for each exchange
"""

import requests
import json
import time
from datetime import datetime

class ExchangeParserTester:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
        self.timeout = 10

    def test_binance_parsing(self):
        """Test Binance data parsing"""
        print("🎯 TESTING BINANCE PARSING")
        print("-" * 40)
        
        try:
            # USDⓈ-M Futures
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                symbols = data.get('symbols', [])
                print(f"📊 Total symbols: {len(symbols)}")
                
                # Count by contract type
                contract_types = {}
                for symbol in symbols:
                    contract_type = symbol.get('contractType')
                    contract_types[contract_type] = contract_types.get(contract_type, 0) + 1
                
                print(f"📋 Contract types: {contract_types}")
                
                # Filter perpetuals
                perpetuals = []
                for symbol in symbols:
                    if (symbol.get('contractType') == 'PERPETUAL' and 
                        symbol.get('status') == 'TRADING'):
                        perpetuals.append(symbol.get('symbol'))
                
                print(f"✅ PERPETUAL symbols: {len(perpetuals)}")
                print(f"📝 Sample perpetuals: {perpetuals[:5]}")
                
                return len(perpetuals)
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return 0
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return 0

    def test_bybit_parsing(self):
        """Test Bybit data parsing"""
        print("\n🎯 TESTING BYBIT PARSING")
        print("-" * 40)
        
        try:
            # Linear Futures
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                print(f"📊 Response code: {data.get('retCode')}")
                print(f"📊 Response message: {data.get('retMsg')}")
                
                if data.get('retCode') == 0:
                    items = data.get('result', {}).get('list', [])
                    print(f"📊 Total items: {len(items)}")
                    
                    # Count by contract type
                    contract_types = {}
                    status_types = {}
                    for item in items:
                        contract_type = item.get('contractType')
                        status = item.get('status')
                        contract_types[contract_type] = contract_types.get(contract_type, 0) + 1
                        status_types[status] = status_types.get(status, 0) + 1
                    
                    print(f"📋 Contract types: {contract_types}")
                    print(f"📋 Status types: {status_types}")
                    
                    # Filter perpetuals
                    perpetuals = []
                    for item in items:
                        if (item.get('status') == 'Trading' and 
                            item.get('contractType') == 'LinearPerpetual'):
                            perpetuals.append(item.get('symbol'))
                    
                    print(f"✅ LinearPerpetual symbols: {len(perpetuals)}")
                    print(f"📝 Sample: {perpetuals[:5]}")
                    
                    return len(perpetuals)
                else:
                    print(f"❌ API Error: {data.get('retMsg')}")
                    return 0
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return 0
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return 0

    def test_bitget_parsing(self):
        """Test Bitget data parsing"""
        print("\n🎯 TESTING BITGET PARSING")
        print("-" * 40)
        
        try:
            # USDT Futures
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                print(f"📊 Response code: {data.get('code')}")
                print(f"📊 Response message: {data.get('msg')}")
                
                if data.get('code') == '00000':
                    items = data.get('data', [])
                    print(f"📊 Total items: {len(items)}")
                    
                    # Count by symbol type and status
                    symbol_types = {}
                    status_types = {}
                    for item in items:
                        symbol_type = item.get('symbolType')
                        status = item.get('status')
                        symbol_types[symbol_type] = symbol_types.get(symbol_type, 0) + 1
                        status_types[status] = status_types.get(status, 0) + 1
                    
                    print(f"📋 Symbol types: {symbol_types}")
                    print(f"📋 Status types: {status_types}")
                    
                    # Filter perpetuals
                    perpetuals = []
                    for item in items:
                        if (item.get('status') == 'normal' and 
                            item.get('symbolType') == 'perpetual'):
                            perpetuals.append(item.get('symbol'))
                    
                    print(f"✅ Perpetual symbols: {len(perpetuals)}")
                    print(f"📝 Sample: {perpetuals[:5]}")
                    
                    return len(perpetuals)
                else:
                    print(f"❌ API Error: {data.get('msg')}")
                    return 0
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return 0
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return 0

    def test_all_parsers(self):
        """Test all exchange parsers"""
        print("🚀 Starting Exchange Parser Tests")
        print("=" * 60)
        
        results = {}
        
        # Test Binance
        results['Binance'] = self.test_binance_parsing()
        
        # Test Bybit
        results['Bybit'] = self.test_bybit_parsing()
        
        # Test Bitget
        results['BitGet'] = self.test_bitget_parsing()
        
        # Generate summary
        print("\n" + "=" * 60)
        print("📊 PARSER TEST SUMMARY")
        print("=" * 60)
        
        for exchange, count in results.items():
            status = "✅ WORKING" if count > 0 else "❌ FAILED"
            print(f"{status} {exchange}: {count} perpetual futures")
        
        print(f"\n🎯 TOTAL PERPETUAL FUTURES FOUND: {sum(results.values())}")
        
        return results

    def debug_specific_symbols(self):
        """Debug specific symbols to understand data structure"""
        print("\n🔍 DEBUGGING SPECIFIC SYMBOLS")
        print("-" * 40)
        
        # Test Binance BTCUSDT
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                btc_symbol = None
                for symbol in data.get('symbols', []):
                    if symbol.get('symbol') == 'BTCUSDT':
                        btc_symbol = symbol
                        break
                
                if btc_symbol:
                    print("📋 Binance BTCUSDT structure:")
                    for key, value in btc_symbol.items():
                        print(f"   {key}: {value}")
        except Exception as e:
            print(f"❌ Binance debug error: {e}")
        
        # Test Bybit BTCUSDT
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol=BTCUSDT"
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if data.get('retCode') == 0:
                    items = data.get('result', {}).get('list', [])
                    if items:
                        print("\n📋 Bybit BTCUSDT structure:")
                        for key, value in items[0].items():
                            print(f"   {key}: {value}")
        except Exception as e:
            print(f"❌ Bybit debug error: {e}")
        
        # Test Bitget BTCUSDT
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == '00000':
                    btc_item = None
                    for item in data.get('data', []):
                        if item.get('symbol') == 'BTCUSDT':
                            btc_item = item
                            break
                    
                    if btc_item:
                        print("\n📋 Bitget BTCUSDT structure:")
                        for key, value in btc_item.items():
                            print(f"   {key}: {value}")
        except Exception as e:
            print(f"❌ Bitget debug error: {e}")

def main():
    """Main function"""
    tester = ExchangeParserTester()
    
    print(f"🕐 Parser test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Run main parser tests
        results = tester.test_all_parsers()
        
        # Run detailed debugging if any failed
        if any(count == 0 for count in results.values()):
            tester.debug_specific_symbols()
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"parser_test_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to: {filename}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()