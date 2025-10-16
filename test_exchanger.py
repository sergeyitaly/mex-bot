#!/usr/bin/env python3
"""
Exchange API Endpoint Tester
Tests connectivity and response for all cryptocurrency exchange APIs
"""

import requests
import json
import time
from datetime import datetime
import sys

class ExchangeTester:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
        })
        self.timeout = 10

    def test_endpoint(self, name, url, method='GET', params=None, data_key=None, check_value=None):
        """Test a single endpoint"""
        print(f"🔍 Testing {name}...")
        print(f"   URL: {url}")
        
        try:
            start_time = time.time()
            
            if method == 'GET':
                response = self.session.get(url, timeout=self.timeout, params=params)
            else:
                response = self.session.post(url, timeout=self.timeout, json=params)
            
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Check if we have the expected data structure
                    if data_key:
                        if data_key in data:
                            result_count = len(data[data_key]) if isinstance(data[data_key], list) else "Exists"
                            print(f"   ✅ SUCCESS - Status: {response.status_code}, Time: {elapsed:.2f}s")
                            print(f"   📊 Data: {data_key} found with {result_count} items")
                            
                            # Show first few items if it's a list
                            if isinstance(data[data_key], list) and data[data_key]:
                                print(f"   📋 Sample: {data[data_key][:2]}")
                        else:
                            print(f"   ⚠️  WARNING - Status: {response.status_code}, but {data_key} not found")
                            print(f"   📊 Response keys: {list(data.keys())}")
                    else:
                        print(f"   ✅ SUCCESS - Status: {response.status_code}, Time: {elapsed:.2f}s")
                        print(f"   📊 Response: {str(data)[:200]}...")
                    
                    return {
                        'status': 'SUCCESS',
                        'response_time': elapsed,
                        'data': data,
                        'size': len(response.content)
                    }
                    
                except json.JSONDecodeError:
                    print(f"   ❌ FAILED - Invalid JSON response")
                    return {
                        'status': 'INVALID_JSON',
                        'response_time': elapsed,
                        'error': 'Invalid JSON'
                    }
            else:
                print(f"   ❌ FAILED - HTTP {response.status_code}, Time: {elapsed:.2f}s")
                return {
                    'status': f'HTTP_{response.status_code}',
                    'response_time': elapsed,
                    'error': f'HTTP {response.status_code}'
                }
                
        except requests.exceptions.Timeout:
            print(f"   ❌ FAILED - Timeout after {self.timeout}s")
            return {
                'status': 'TIMEOUT',
                'response_time': self.timeout,
                'error': 'Request timeout'
            }
        except requests.exceptions.ConnectionError:
            print(f"   ❌ FAILED - Connection error")
            return {
                'status': 'CONNECTION_ERROR',
                'response_time': 0,
                'error': 'Connection failed'
            }
        except Exception as e:
            print(f"   ❌ FAILED - {str(e)}")
            return {
                'status': 'ERROR',
                'response_time': 0,
                'error': str(e)
            }
        
        print()  # Empty line for readability

    def test_all_exchanges(self):
        """Test all exchange endpoints"""
        print("🚀 Starting Comprehensive Exchange API Test")
        print("=" * 60)
        
        results = {}
        
        # MEXC
        print("\n🎯 TESTING MEXC")
        print("-" * 40)
        results['MEXC'] = self.test_endpoint(
            "MEXC Futures Details",
            "https://contract.mexc.com/api/v1/contract/detail",
            data_key="data"
        )
        
        # Binance USDⓈ-M Futures
        print("\n🎯 TESTING BINANCE")
        print("-" * 40)
        results['Binance_USDT-M'] = self.test_endpoint(
            "Binance USDⓈ-M Futures",
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            data_key="symbols"
        )
        
        # Binance COIN-M Futures
        results['Binance_COIN-M'] = self.test_endpoint(
            "Binance COIN-M Futures", 
            "https://dapi.binance.com/dapi/v1/exchangeInfo",
            data_key="symbols"
        )
        
        # Binance Ticker (fallback)
        results['Binance_Ticker'] = self.test_endpoint(
            "Binance Ticker Price",
            "https://fapi.binance.com/fapi/v1/ticker/price",
            data_key=None  # This returns a list directly
        )
        
        # Bybit
        print("\n🎯 TESTING BYBIT")
        print("-" * 40)
        results['Bybit_Linear'] = self.test_endpoint(
            "Bybit Linear Futures",
            "https://api.bybit.com/v5/market/instruments-info",
            params={'category': 'linear'},
            data_key="result"
        )
        
        results['Bybit_Inverse'] = self.test_endpoint(
            "Bybit Inverse Futures",
            "https://api.bybit.com/v5/market/instruments-info", 
            params={'category': 'inverse'},
            data_key="result"
        )
        
        # Bybit V2 (fallback)
        results['Bybit_V2'] = self.test_endpoint(
            "Bybit V2 Symbols",
            "https://api.bybit.com/v2/public/symbols",
            data_key="result"
        )
        
        # OKX
        print("\n🎯 TESTING OKX")
        print("-" * 40)
        results['OKX_Swap'] = self.test_endpoint(
            "OKX Swap Instruments",
            "https://www.okx.com/api/v5/public/instruments",
            params={'instType': 'SWAP'},
            data_key="data"
        )
        
        # Gate.io
        print("\n🎯 TESTING GATE.IO")
        print("-" * 40)
        results['Gate_USDT_Futures'] = self.test_endpoint(
            "Gate.io USDT Futures",
            "https://api.gateio.ws/api/v4/futures/usdt/contracts",
            data_key=None  # Returns list directly
        )
        
        # KuCoin
        print("\n🎯 TESTING KUCOIN")
        print("-" * 40)
        results['KuCoin_Active'] = self.test_endpoint(
            "KuCoin Active Contracts",
            "https://api-futures.kucoin.com/api/v1/contracts/active",
            data_key="data"
        )
        
        # BingX
        print("\n🎯 TESTING BINGX")
        print("-" * 40)
        results['BingX_Contracts'] = self.test_endpoint(
            "BingX Swap Contracts", 
            "https://open-api.bingx.com/openApi/swap/v2/quote/contracts",
            data_key="data"
        )
        
        # Bitget
        print("\n🎯 TESTING BITGET")
        print("-" * 40)
        results['Bitget_USDT_Futures'] = self.test_endpoint(
            "Bitget USDT Futures",
            "https://api.bitget.com/api/v2/mix/market/contracts",
            params={'productType': 'USDT-FUTURES'},
            data_key="data"
        )
        
        results['Bitget_COIN_Futures'] = self.test_endpoint(
            "Bitget COIN Futures",
            "https://api.bitget.com/api/v2/mix/market/contracts",
            params={'productType': 'COIN-FUTURES'},
            data_key="data"
        )
        
        # Bitget V1 (fallback)
        results['Bitget_V1'] = self.test_endpoint(
            "Bitget V1 Contracts",
            "https://api.bitget.com/api/mix/v1/market/contracts",
            params={'productType': 'umcbl'},
            data_key="data"
        )
        
        return results

    def generate_report(self, results):
        """Generate a summary report"""
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY REPORT")
        print("=" * 60)
        
        successful = 0
        failed = 0
        
        for endpoint, result in results.items():
            if result['status'] == 'SUCCESS':
                successful += 1
                print(f"✅ {endpoint}: SUCCESS ({result['response_time']:.2f}s)")
            else:
                failed += 1
                print(f"❌ {endpoint}: {result['status']}")
        
        print("\n" + "=" * 60)
        print(f"🎯 TOTAL: {successful + failed} endpoints")
        print(f"✅ SUCCESSFUL: {successful}")
        print(f"❌ FAILED: {failed}")
        print(f"📈 SUCCESS RATE: {(successful/(successful+failed))*100:.1f}%")
        
        # Show specific recommendations for failed endpoints
        if failed > 0:
            print("\n🔧 RECOMMENDATIONS:")
            for endpoint, result in results.items():
                if result['status'] != 'SUCCESS':
                    print(f"   • {endpoint}: {result.get('error', 'Unknown error')}")

def main():
    """Main function"""
    tester = ExchangeTester()
    
    print(f"🕐 Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        results = tester.test_all_exchanges()
        tester.generate_report(results)
        
        # Save detailed results to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"exchange_test_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            # Remove data from results to make file smaller
            clean_results = {}
            for endpoint, result in results.items():
                clean_results[endpoint] = {
                    'status': result['status'],
                    'response_time': result['response_time'],
                    'size': result.get('size', 0),
                    'error': result.get('error', '')
                }
            json.dump(clean_results, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {filename}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()