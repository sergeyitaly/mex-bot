import requests
import json
import logging
import os
import time
import schedule
from datetime import datetime, timedelta
from telegram import Bot, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
from telegram.error import TelegramError
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import fcntl
import threading
import atexit
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
import io
import random
import hmac
import hashlib
import re
from typing import Optional, List, Dict, Set, Any, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MEXCTracker:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.update_interval = int(os.getenv('UPDATE_INTERVAL', 60))
        self.price_check_interval = int(os.getenv('PRICE_CHECK_INTERVAL', 5))  # minutes
        
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        
        self.updater = Updater(token=self.bot_token, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.bot = Bot(token=self.bot_token)
        self.setup_handlers()
        self.init_data_file()
        self.last_unique_futures = set()
        self.bybit_api_key = os.getenv('BYBIT_API_KEY', '')
        self.bybit_api_secret = os.getenv('BYBIT_API_SECRET', '')
        
        # Price tracking
        self.price_history = {}  # symbol: {timestamp: price}
        self.last_price_check = None
        self.restart_count = 0
        self.last_restart = None
        # Google Sheets setup
        self.setup_google_sheets()
        self.session = self._create_session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
        self.proxies = self._get_proxies()

    def _create_session(self):
        """Create requests session with minimal headers"""
        session = requests.Session()
        
        # Simple retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def _get_proxies(self) -> List[dict]:
        return [{}]  # Empty dict means no proxy

    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if not creds_json:
                logger.info("Google Sheets not configured - no credentials found")
                self.gs_client = None
                self.spreadsheet = None
                return

            try:
                creds_dict = json.loads(creds_json)
                logger.info(f"Google Sheets credentials loaded for: {creds_dict.get('client_email')}")
            except Exception as e:
                logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
                self.gs_client = None
                self.spreadsheet = None
                return

            scope = ['https://www.googleapis.com/auth/spreadsheets']
            
            try:
                self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
                self.gs_client = gspread.authorize(self.creds)
                
                # Connect to existing spreadsheet
                spreadsheet_id = os.getenv('GOOGLE_SHEET_ID')
                if spreadsheet_id:
                    self.spreadsheet = self.gs_client.open_by_key(spreadsheet_id)
                    logger.info(f"✅ Connected to existing spreadsheet: {self.spreadsheet.title}")
                else:
                    self.spreadsheet = None
                    logger.info("No Google Sheet ID configured")
                
            except Exception as e:
                logger.error(f"Failed to connect to spreadsheet: {e}")
                self.gs_client = None
                self.spreadsheet = None

        except Exception as e:
            logger.error(f"Google Sheets setup error: {e}")
            self.gs_client = None
            self.spreadsheet = None

    # ==================== PRICE MONITORING ====================

    def get_all_mexc_prices(self):
        """Robust method to get price data for all MEXC futures"""
        try:
            symbols = self.get_mexc_futures()
            price_data = {}
            successful = 0
            failed = 0
            
            logger.info(f"💰 Getting price data for {len(symbols)} MEXC futures...")
            
            for symbol in list(symbols):
                try:
                    price_info = self.get_mexc_price_data(symbol)
                    if price_info and price_info.get('price'):
                        price_data[symbol] = price_info
                        successful += 1
                    else:
                        logger.debug(f"❌ No price data for {symbol}")
                        failed += 1
                    
                    # Rate limiting
                    time.sleep(0.05)  # 50ms between requests
                    
                except Exception as e:
                    logger.error(f"Error getting price for {symbol}: {e}")
                    failed += 1
                    continue
            
            # Store price history for tracking
            current_time = datetime.now()
            for symbol, data in price_data.items():
                if symbol not in self.price_history:
                    self.price_history[symbol] = {}
                self.price_history[symbol][current_time] = data['price']
            
            # Keep only last 24 hours of history
            cutoff_time = current_time - timedelta(hours=24)
            for symbol in self.price_history:
                self.price_history[symbol] = {
                    ts: price for ts, price in self.price_history[symbol].items() 
                    if ts > cutoff_time
                }
            
            logger.info(f"✅ Price data: {successful} successful, {failed} failed")
            
            # If we have some data but many failures, try batch method
            if successful > 0 and failed > len(symbols) * 0.5:  # If more than 50% failed
                logger.info("🔄 High failure rate, trying batch method...")
                batch_data = self.get_mexc_prices_batch()
                price_data.update(batch_data)
            
            return price_data
            
        except Exception as e:
            logger.error(f"Error getting all MEXC prices: {e}")
            return {}

    def get_mexc_prices_batch(self):
        """Get prices in batch using ticker endpoint"""
        try:
            url = "https://contract.mexc.com/api/v1/contract/ticker"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    tickers = data.get('data', [])
                    price_data = {}
                    
                    for ticker in tickers:
                        symbol = ticker.get('symbol')
                        price = ticker.get('lastPrice')
                        
                        if symbol and price:
                            # Add underscore for consistency
                            formatted_symbol = symbol
                            price_data[formatted_symbol] = {
                                'symbol': formatted_symbol,
                                'price': float(price),
                                'changes': {},  # No historical changes in batch
                                'timestamp': datetime.now(),
                                'source': 'batch_ticker'
                            }
                    
                    logger.info(f"✅ Batch prices: {len(price_data)} symbols")
                    return price_data
            
            return {}
            
        except Exception as e:
            logger.error(f"Batch price method error: {e}")
            return {}







    def get_all_mexc_prices(self):
        """Get price data for all MEXC futures"""
        try:
            symbols = self.get_mexc_futures()
            price_data = {}
            
            logger.info(f"🔄 Getting price data for {len(symbols)} MEXC futures...")
            
            for symbol in list(symbols)[:50]:  # Limit to first 50 to avoid rate limits
                try:
                    price_info = self.get_mexc_price_data(symbol)
                    if price_info:
                        price_data[symbol] = price_info
                    time.sleep(0.1)  # Rate limiting
                except Exception as e:
                    logger.error(f"Error getting price for {symbol}: {e}")
                    continue
            
            # Store price history
            current_time = datetime.now()
            for symbol, data in price_data.items():
                if symbol not in self.price_history:
                    self.price_history[symbol] = {}
                self.price_history[symbol][current_time] = data['price']
            
            # Keep only last 24 hours of history
            cutoff_time = current_time - timedelta(hours=24)
            for symbol in self.price_history:
                self.price_history[symbol] = {
                    ts: price for ts, price in self.price_history[symbol].items() 
                    if ts > cutoff_time
                }
            
            logger.info(f"✅ Got price data for {len(price_data)} symbols")
            return price_data
            
        except Exception as e:
            logger.error(f"Error getting all MEXC prices: {e}")
            return {}

    def analyze_price_movements(self, price_data):
        """Analyze price movements with fallback for missing data"""
        try:
            symbols_with_changes = []
            
            for symbol, data in price_data.items():
                changes = data.get('changes', {})
                price = data.get('price', 0)
                
                # If we have no historical changes, create minimal data
                if not changes:
                    # Try to calculate from price history if available
                    historical_changes = self.calculate_changes_from_history(symbol, price)
                    if historical_changes:
                        changes = historical_changes
                
                # Calculate overall score based on available timeframes
                score = 0
                weight_total = 0
                
                if '5m' in changes:
                    score += changes['5m'] * 2
                    weight_total += 2
                if '15m' in changes:
                    score += changes['15m'] * 1.5
                    weight_total += 1.5
                if '30m' in changes:
                    score += changes['30m'] * 1.2
                    weight_total += 1.2
                if '60m' in changes:
                    score += changes['60m'] * 1
                    weight_total += 1
                if '240m' in changes:
                    score += changes['240m'] * 0.5
                    weight_total += 0.5
                
                # Normalize score if we have weights
                if weight_total > 0:
                    score = score / weight_total
                
                symbols_with_changes.append({
                    'symbol': symbol,
                    'price': price,
                    'changes': changes,
                    'score': score,
                    'latest_change': changes.get('5m', changes.get('60m', 0))
                })
            
            # Sort by score (highest first)
            symbols_with_changes.sort(key=lambda x: x['score'], reverse=True)
            
            return symbols_with_changes
            
        except Exception as e:
            logger.error(f"Error analyzing price movements: {e}")
            return []

    def calculate_changes_from_history(self, symbol, current_price):
        """Calculate price changes from historical data if available"""
        try:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
                return {}
            
            history = self.price_history[symbol]
            timestamps = sorted(history.keys())
            current_time = datetime.now()
            
            changes = {}
            
            # Calculate 5m change
            five_min_ago = current_time - timedelta(minutes=5)
            price_5m = self.find_closest_price(history, five_min_ago)
            if price_5m:
                changes['5m'] = ((current_price - price_5m) / price_5m) * 100
            
            # Calculate 1h change
            one_hour_ago = current_time - timedelta(hours=1)
            price_1h = self.find_closest_price(history, one_hour_ago)
            if price_1h:
                changes['60m'] = ((current_price - price_1h) / price_1h) * 100
            
            # Calculate 4h change
            four_hours_ago = current_time - timedelta(hours=4)
            price_4h = self.find_closest_price(history, four_hours_ago)
            if price_4h:
                changes['240m'] = ((current_price - price_4h) / price_4h) * 100
            
            return changes
            
        except Exception as e:
            logger.error(f"Error calculating changes from history for {symbol}: {e}")
            return {}



    def find_closest_price(self, history, target_time):
        """Find closest price to target time in history"""
        try:
            closest_time = None
            min_diff = timedelta.max
            
            for timestamp in history.keys():
                diff = abs(timestamp - target_time)
                if diff < min_diff:
                    min_diff = diff
                    closest_time = timestamp
            
            # Only return if within reasonable time window
            if min_diff < timedelta(hours=1):
                return history[closest_time]
            return None
            
        except Exception as e:
            logger.error(f"Error finding closest price: {e}")
            return None
        

    # ==================== ENHANCED UNIQUE FUTURES MONITORING ====================

    def monitor_unique_futures_changes(self):
        """Monitor changes in unique futures and send notifications"""
        try:
            logger.info("🔍 Monitoring unique futures changes...")
            
            # Get current unique futures
            current_unique, exchange_stats = self.find_unique_futures_robust()
            current_unique_set = set(current_unique)
            
            # Load previous state
            data = self.load_data()
            previous_unique = set(data.get('unique_futures', []))
            
            # Find new unique futures
            new_futures = current_unique_set - previous_unique
            lost_futures = previous_unique - current_unique_set
            
            # Send notifications only if there are changes
            if new_futures:
                self.send_new_unique_notification(new_futures, current_unique_set)
            
            if lost_futures:
                self.send_lost_unique_notification(lost_futures, current_unique_set)
            
            # Update stored data
            data['unique_futures'] = list(current_unique_set)
            data['last_check'] = datetime.now().isoformat()
            data['exchange_stats'] = exchange_stats
            self.save_data(data)
            
            self.last_unique_futures = current_unique_set
            
            # ALWAYS return both values, even if empty
            return new_futures, lost_futures
            
        except Exception as e:
            logger.error(f"Error monitoring unique futures: {e}")
            # Return empty sets on error
            return set(), set()
    
    def send_new_unique_notification(self, new_futures, all_unique):
        """Send notification about new unique futures"""
        try:
            message = "🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
            
            # Get price data for new futures
            price_data = self.get_all_mexc_prices()
            
            for symbol in sorted(new_futures):
                price_info = price_data.get(symbol)
                if price_info:
                    changes = price_info.get('changes', {})
                    change_5m = changes.get('5m', 0)
                    change_1h = changes.get('60m', 0)
                    
                    message += f"✅ <b>{symbol}</b>\n"
                    message += f"   5m: {self.format_change(change_5m)}\n"
                    message += f"   1h: {self.format_change(change_1h)}\n\n"
                else:
                    message += f"✅ <b>{symbol}</b> (price data unavailable)\n\n"
            
            message += f"📊 Total unique: <b>{len(all_unique)}</b>"
            
            self.send_broadcast_message(message)
            
        except Exception as e:
            logger.error(f"Error sending new unique notification: {e}")

    def send_lost_unique_notification(self, lost_futures, remaining_unique):
        """Send notification about lost unique futures"""
        try:
            message = "📉 <b>FUTURES NO LONGER UNIQUE:</b>\n\n"
            
            for symbol in sorted(lost_futures):
                # Find which exchanges now have this symbol
                coverage = self.verify_symbol_coverage(symbol)
                other_exchanges = [e for e in coverage if e != 'MEXC']
                
                message += f"❌ <b>{symbol}</b>\n"
                message += f"   Now also on: {', '.join(other_exchanges)}\n\n"
            
            message += f"📊 Remaining unique: <b>{len(remaining_unique)}</b>"
            
            self.send_broadcast_message(message)
            
        except Exception as e:
            logger.error(f"Error sending lost unique notification: {e}")

    def format_change(self, change):
        """Format price change with color emoji"""
        if change > 0:
            return f"🟢 +{change:.2f}%"
        elif change < 0:
            return f"🔴 {change:.2f}%"
        else:
            return f"⚪ {change:.2f}%"

    # ==================== ENHANCED GOOGLE SHEETS ====================

    def update_google_sheet_with_prices(self):
        """Update Google Sheet with price data"""
        if not self.gs_client or not self.spreadsheet:
            return
        
        try:
            # Get unique futures and price data
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Update Unique Futures sheet with price data
            self.update_unique_futures_sheet_with_prices(unique_futures, analyzed_prices)
            
            # Update Price Analysis sheet
            self.update_price_analysis_sheet(analyzed_prices)
            
            logger.info("✅ Google Sheet updated with price data")
            
        except Exception as e:
            logger.error(f"Error updating Google Sheet with prices: {e}")

    def update_unique_futures_sheet_with_prices(self, unique_futures, analyzed_prices):
        """Update Unique Futures sheet with price information"""
        try:
            worksheet = self.spreadsheet.worksheet('Unique Futures')
            
            # Clear existing data
            if worksheet.row_count > 1:
                worksheet.clear()
            
            # Enhanced headers with price changes
            headers = [
                'Symbol', 'Current Price', '5m Change', '15m Change', 
                '30m Change', '1h Change', '4h Change', 'Score', 'Status', 'Last Updated'
            ]
            worksheet.update('A1', [headers])
            
            # Prepare data
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create mapping for quick price lookup
            price_map = {item['symbol']: item for item in analyzed_prices}
            
            for symbol in sorted(unique_futures):
                price_info = price_map.get(symbol, {})
                changes = price_info.get('changes', {})
                
                row = [
                    symbol,
                    price_info.get('price', 'N/A'),
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{price_info.get('score', 0):.2f}",
                    'UNIQUE',
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet in batches
            if sheet_data:
                batch_size = 100
                for i in range(0, len(sheet_data), batch_size):
                    batch = sheet_data[i:i + batch_size]
                    worksheet.update(f'A{i+2}', batch)
            
        except Exception as e:
            logger.error(f"Error updating Unique Futures sheet with prices: {e}")

    def update_price_analysis_sheet(self, analyzed_prices):
        """Update Price Analysis sheet with top performers"""
        try:
            # Get or create Price Analysis sheet
            try:
                worksheet = self.spreadsheet.worksheet('Price Analysis')
            except gspread.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(title='Price Analysis', rows=1000, cols=12)
            
            # Clear existing data
            worksheet.clear()
            
            # Headers
            headers = [
                'Rank', 'Symbol', 'Current Price', '5m %', '15m %', '30m %', 
                '1h %', '4h %', 'Score', 'Trend', 'Volume', 'Last Updated'
            ]
            worksheet.update('A1', [headers])
            
            # Prepare data - top 50 performers
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for i, item in enumerate(analyzed_prices[:50], 1):
                changes = item.get('changes', {})
                
                # Determine trend
                latest_change = item.get('latest_change', 0)
                if latest_change > 5:
                    trend = "🚀 STRONG UP"
                elif latest_change > 2:
                    trend = "🟢 UP"
                elif latest_change < -5:
                    trend = "🔻 STRONG DOWN"
                elif latest_change < -2:
                    trend = "🔴 DOWN"
                else:
                    trend = "⚪ FLAT"
                
                row = [
                    i,
                    item['symbol'],
                    item.get('price', 'N/A'),
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{item.get('score', 0):.2f}",
                    trend,
                    'N/A',  # Volume would require additional API call
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet
            if sheet_data:
                worksheet.update('A2', sheet_data)
            
        except Exception as e:
            logger.error(f"Error updating Price Analysis sheet: {e}")

    def format_change_for_sheet(self, change):
        """Format change for Google Sheets"""
        if change is None:
            return 'N/A'
        return f"{change:+.2f}%"

    # ==================== CORE UNIQUE FUTURES LOGIC ====================

    def normalize_symbol_for_comparison(self, symbol):
        """Simple and reliable symbol normalization for cross-exchange comparison"""
        if not symbol:
            return ""
        
        original = symbol.upper()
        normalized = original
        
        # Remove common futures suffixes
        patterns_to_remove = [
            r'[-_/]?PERP(ETUAL)?$',
            r'[-_/]?USDT$', 
            r'[-_/]?USD$',
            r'[-_/]?SWAP$',
            r'[-_/]?FUTURES?$',
            r'[-_/]?CONTRACT$',
        ]
        
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized)
        
        # Remove separators but keep the main symbol
        normalized = re.sub(r'[-_/]', '', normalized)
        
        # Remove numbers at the end (like BTC230930 for quarterly futures)
        normalized = re.sub(r'\d+$', '', normalized)
        
        return normalized.strip()

    def get_all_exchanges_futures(self):
        """Get futures from all exchanges except MEXC"""
        exchanges = {
            'Binance': self.get_binance_futures,
            'Bybit': self.get_bybit_futures,
            'OKX': self.get_okx_futures,
            'Gate.io': self.get_gate_futures,
            'KuCoin': self.get_kucoin_futures,
            'BingX': self.get_bingx_futures,
            'BitGet': self.get_bitget_futures
        }
        
        all_futures = set()
        exchange_stats = {}
        
        for name, method in exchanges.items():
            try:
                logger.info(f"🔍 Getting futures from {name}...")
                futures = method()
                if futures:
                    all_futures.update(futures)
                    exchange_stats[name] = len(futures)
                    logger.info(f"✅ {name}: {len(futures)} futures")
                else:
                    exchange_stats[name] = 0
                    logger.warning(f"❌ {name}: No futures found")
            except Exception as e:
                exchange_stats[name] = 0
                logger.error(f"🚨 Error getting {name} futures: {e}")
        
        logger.info(f"📊 Total futures from other exchanges: {len(all_futures)}")
        return all_futures, exchange_stats

    def find_unique_futures_robust(self):
        """Find truly unique MEXC futures with improved comparison"""
        try:
            # Get all futures from other exchanges
            all_futures, exchange_stats = self.get_all_exchanges_futures()
            
            # Get MEXC futures
            mexc_futures = self.get_mexc_futures()
            
            logger.info(f"📊 MEXC futures: {len(mexc_futures)}")
            logger.info(f"📊 Other exchanges total futures: {len(all_futures)}")
            
            # Create normalized mappings
            all_normalized = set()  # Just track normalized symbols from other exchanges
            mexc_normalized_map = {} # normalized -> original_mexc_symbol
            
            # Process other exchanges - just store normalized symbols
            for symbol in all_futures:
                normalized = self.normalize_symbol_for_comparison(symbol)
                all_normalized.add(normalized)
            
            # Process MEXC futures
            for symbol in mexc_futures:
                normalized = self.normalize_symbol_for_comparison(symbol)
                mexc_normalized_map[normalized] = symbol
            
            # Find unique futures (only on MEXC)
            unique_futures = set()
            
            for normalized, mexc_original in mexc_normalized_map.items():
                if normalized not in all_normalized:
                    unique_futures.add(mexc_original)
            
            # Additional verification step
            verified_unique = set()
            for symbol in unique_futures:
                coverage = self.verify_symbol_coverage(symbol)
                if coverage == ['MEXC']:
                    verified_unique.add(symbol)
                else:
                    logger.warning(f"False positive detected: {symbol} found on {coverage}")
            
            logger.info(f"🎯 Initial unique candidates: {len(unique_futures)}")
            logger.info(f"🎯 Verified unique: {len(verified_unique)}")
            
            return verified_unique, exchange_stats
            
        except Exception as e:
            logger.error(f"Error finding unique futures: {e}")
            return set(), {}

    def verify_symbol_coverage(self, symbol):
        """Check which exchanges have this specific symbol"""
        coverage = []
        
        exchange_methods = {
            'MEXC': self.get_mexc_futures,
            'Binance': self.get_binance_futures,
            'Bybit': self.get_bybit_futures,
            'OKX': self.get_okx_futures,
            'Gate.io': self.get_gate_futures,
            'KuCoin': self.get_kucoin_futures,
            'BingX': self.get_bingx_futures,
            'BitGet': self.get_bitget_futures
        }
        
        for exchange_name, method in exchange_methods.items():
            try:
                futures = method()
                found = False
                
                # Check normalized match
                normalized_target = self.normalize_symbol_for_comparison(symbol)
                
                for fut in futures:
                    normalized_fut = self.normalize_symbol_for_comparison(fut)
                    if normalized_target == normalized_fut:
                        found = True
                        break
                
                if found:
                    coverage.append(exchange_name)
                    
            except Exception as e:
                logger.error(f"Error checking {exchange_name} for {symbol}: {e}")
        
        return coverage

    # ==================== EXCHANGE API METHODS ====================

    def get_mexc_futures(self):
        """Get ALL futures from MEXC"""
        try:
            url = "https://contract.mexc.com/api/v1/contract/detail"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for contract in data.get('data', []):
                symbol = contract.get('symbol', '')
                if symbol:
                    futures.add(symbol)
            
            logger.info(f"MEXC: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"MEXC error: {e}")
            return set()

    def get_binance_futures(self):
        """Get Binance futures with proxy support"""
        try:
            logger.info("🔄 Fetching Binance futures...")
            
            futures = set()
            
            # USDⓈ-M Futures - try multiple endpoints
            endpoints = [
                "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"  # Fallback testnet
            ]
            
            for url in endpoints:
                logger.info(f"📡 Trying Binance URL: {url}")
                response = self._make_request_with_retry(url)
                
                if response and response.status_code == 200:
                    data = response.json()
                    symbols = data.get('symbols', [])
                    
                    usdt_futures = set()
                    for symbol in symbols:
                        if (symbol.get('contractType') == 'PERPETUAL' and 
                            symbol.get('status') == 'TRADING'):
                            usdt_futures.add(symbol.get('symbol'))
                    
                    futures.update(usdt_futures)
                    logger.info(f"✅ Binance USDⓈ-M perpetuals found: {len(usdt_futures)}")
                    break  # Success, no need to try other endpoints
                else:
                    logger.warning(f"❌ Failed to fetch from {url}")
            
            # If still no data, try alternative approach
            if not futures:
                logger.info("🔄 Trying alternative Binance endpoint...")
                alt_response = self._make_request_with_retry("https://api.binance.com/api/v3/exchangeInfo")
                if alt_response and alt_response.status_code == 200:
                    # This gives spot symbols, but we can use it as fallback
                    data = alt_response.json()
                    symbols = data.get('symbols', [])
                    spot_symbols = {s['symbol'] for s in symbols if s.get('status') == 'TRADING'}
                    # Filter for common futures symbols pattern
                    futures = {s for s in spot_symbols if s.endswith('USDT')}
                    logger.info(f"🔄 Using spot symbols as fallback: {len(futures)}")
            
            logger.info(f"🎯 Binance TOTAL: {len(futures)} futures")
            return futures
            
        except Exception as e:
            logger.error(f"❌ Binance error: {e}")
            return set()

    def get_bybit_futures(self):
        """Extremely simple Bybit implementation with caching to avoid 403 loops"""
        try:
            # Check cache first to avoid repeated failed requests
            cache_key = "bybit_futures_cache"
            cache_timeout = 300  # 5 minutes
            
            if hasattr(self, '_bybit_cache') and hasattr(self, '_bybit_cache_time'):
                if (datetime.now() - self._bybit_cache_time).seconds < cache_timeout:
                    logger.info("🔄 Using cached Bybit data")
                    return self._bybit_cache
            
            logger.info("🔄 Trying simplified Bybit request...")
            
            # Try the most basic endpoint with minimal headers
            url = "https://api.bybit.com/v5/market/tickers?category=linear"
            
            # Use minimal headers to avoid detection
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': '*/*',
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('retCode') == 0:
                        futures = set()
                        for item in data.get('result', {}).get('list', []):
                            symbol = item.get('symbol')
                            if symbol:
                                futures.add(symbol)
                        
                        # Cache successful result
                        self._bybit_cache = futures
                        self._bybit_cache_time = datetime.now()
                        
                        logger.info(f"✅ Bybit simple: {len(futures)} symbols")
                        return futures
                except:
                    pass
            
            # If we get here, the request failed
            logger.warning("⚠️ Bybit simple method failed, using empty set")
            
            # Cache empty result to avoid repeated attempts
            self._bybit_cache = set()
            self._bybit_cache_time = datetime.now()
            
            return set()
            
        except Exception as e:
            logger.error(f"Bybit simple error: {e}")
            # Cache empty result on error too
            self._bybit_cache = set()
            self._bybit_cache_time = datetime.now()
            return set()
        
    def get_okx_futures(self):
        """Get ALL futures from OKX"""
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('data', []):
                inst_id = item.get('instId', '')
                if inst_id and 'SWAP' in inst_id:
                    futures.add(inst_id)
            
            logger.info(f"OKX: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"OKX error: {e}")
            return set()

    def get_gate_futures(self):
        """Get ALL futures from Gate.io"""
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data:
                symbol = item.get('name', '')
                if symbol and item.get('in_delisting', False) is False:
                    futures.add(symbol)
            
            logger.info(f"Gate.io: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"Gate.io error: {e}")
            return set()

    def get_kucoin_futures(self):
        """Get ALL futures from KuCoin"""
        try:
            url = "https://api-futures.kucoin.com/api/v1/contracts/active"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                if symbol:
                    futures.add(symbol)
            
            logger.info(f"KuCoin: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"KuCoin error: {e}")
            return set()

    def get_bingx_futures(self):
        """Get ALL futures from BingX"""
        try:
            url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                if symbol:
                    futures.add(symbol)
            
            logger.info(f"BingX: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"BingX error: {e}")
            return set()

    def get_bitget_futures(self):
        """Get Bitget perpetual futures"""
        try:
            futures = set()
            
            # USDT-FUTURES
            url1 = "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures"
            response1 = requests.get(url1, timeout=10)
            
            if response1.status_code == 200:
                data = response1.json()
                if data.get('code') == '00000':
                    for item in data.get('data', []):
                        symbol_type = item.get('symbolType')
                        symbol_name = item.get('symbol')
                        if symbol_type == 'perpetual':
                            futures.add(symbol_name)
            
            # COIN-FUTURES
            url2 = "https://api.bitget.com/api/v2/mix/market/contracts?productType=coin-futures"
            response2 = requests.get(url2, timeout=10)
            
            if response2.status_code == 200:
                data = response2.json()
                if data.get('code') == '00000':
                    for item in data.get('data', []):
                        symbol_type = item.get('symbolType')
                        symbol_name = item.get('symbol')
                        if symbol_type == 'perpetual':
                            futures.add(symbol_name)
            
            logger.info(f"BitGet: {len(futures)} futures")
            return futures
            
        except Exception as e:
            logger.error(f"BitGet error: {e}")
            return set()

    # ==================== TELEGRAM COMMANDS ====================

    def setup_handlers(self):
        """Setup command handlers"""
        self.dispatcher.add_handler(CommandHandler("start", self.start_command))
        self.dispatcher.add_handler(CommandHandler("status", self.status_command))
        self.dispatcher.add_handler(CommandHandler("check", self.check_command))
        self.dispatcher.add_handler(CommandHandler("help", self.help_command))
        self.dispatcher.add_handler(CommandHandler("stats", self.stats_command))
        self.dispatcher.add_handler(CommandHandler("exchanges", self.exchanges_command))
        self.dispatcher.add_handler(CommandHandler("analysis", self.analysis_command))
        self.dispatcher.add_handler(CommandHandler("findunique", self.find_unique_command))
        self.dispatcher.add_handler(CommandHandler("checksymbol", self.check_symbol_command))
        self.dispatcher.add_handler(CommandHandler("prices", self.prices_command))
        self.dispatcher.add_handler(CommandHandler("toppers", self.top_performers_command))
        self.dispatcher.add_handler(CommandHandler("forceupdate", self.force_update_command))

    def update_google_sheet(self):
        """Update the Google Sheet with fresh data including price analysis"""
        if not self.gs_client or not self.spreadsheet:
            logger.warning("Google Sheets not available for update")
            return
        
        try:
            logger.info("🔄 Starting comprehensive Google Sheet update...")
            
            # Collect fresh data from all exchanges
            all_futures_data = []
            exchanges = {
                'MEXC': self.get_mexc_futures,
                'Binance': self.get_binance_futures,
                'Bybit': self.get_bybit_futures_robust,
                'OKX': self.get_okx_futures,
                'Gate.io': self.get_gate_futures,
                'KuCoin': self.get_kucoin_futures,
                'BingX': self.get_bingx_futures,
                'BitGet': self.get_bitget_futures
            }
            
            exchange_stats = {}
            symbol_coverage = {}
            current_time = datetime.now().astimezone().isoformat()
            
            # Get data from all exchanges
            for name, method in exchanges.items():
                try:
                    futures = method()
                    exchange_stats[name] = len(futures)
                    logger.info(f"{name}: {len(futures)} futures")
                    
                    for symbol in futures:
                        all_futures_data.append({
                            'symbol': symbol,
                            'exchange': name,
                            'timestamp': current_time
                        })
                        
                        # Track symbol coverage
                        normalized = self.normalize_symbol_for_comparison(symbol)
                        if normalized not in symbol_coverage:
                            symbol_coverage[normalized] = set()
                        symbol_coverage[normalized].add(name)
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Exchange {name} error during sheet update: {e}")
                    exchange_stats[name] = 0
            
            logger.info(f"Total futures collected: {len(all_futures_data)}")
            logger.info(f"Unique symbols: {len(symbol_coverage)}")
            
            # Get unique futures
            unique_futures, _ = self.find_unique_futures_robust()
            logger.info(f"Unique MEXC futures: {len(unique_futures)}")
            
            # Get price data for analysis
            logger.info("💰 Getting price data for analysis...")
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Update all sheets with fresh data
            self.update_unique_futures_sheet_with_prices(unique_futures, analyzed_prices)
            self.update_all_futures_sheet(self.spreadsheet, all_futures_data, symbol_coverage, current_time)
            self.update_mexc_analysis_sheet_with_prices(all_futures_data, symbol_coverage, analyzed_prices, current_time)
            self.update_price_analysis_sheet(analyzed_prices)
            self.update_exchange_stats_sheet(self.spreadsheet, exchange_stats, current_time)
            self.update_dashboard_with_comprehensive_stats(exchange_stats, len(symbol_coverage), len(unique_futures), analyzed_prices)
            
            logger.info("✅ Google Sheet update completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Google Sheet update error: {e}")

    def update_all_futures_sheet(self, spreadsheet, all_futures_data, symbol_coverage, timestamp):
        """Update All Futures sheet"""
        try:
            worksheet = spreadsheet.worksheet('All Futures')
            
            # Clear existing data (keep headers)
            if worksheet.row_count > 1:
                worksheet.clear()
                # Re-add headers
                worksheet.update('A1', [[
                    'Symbol', 'Exchange', 'Normalized', 'Available On', 
                    'Coverage', 'Timestamp', 'Unique', 'Current Price'
                ]])
            
            all_data = []
            for future in all_futures_data:
                normalized = self.normalize_symbol_for_comparison(future['symbol'])
                exchanges_list = symbol_coverage.get(normalized, set())
                available_on = ", ".join(sorted(exchanges_list))
                coverage = f"{len(exchanges_list)} exchanges"
                is_unique = "✅" if len(exchanges_list) == 1 else ""
                
                all_data.append([
                    future['symbol'],
                    future['exchange'],
                    normalized,
                    available_on,
                    coverage,
                    timestamp,
                    is_unique,
                    'N/A'  # Price would be added separately
                ])
            
            # Write in batches
            if all_data:
                batch_size = 100
                for i in range(0, len(all_data), batch_size):
                    batch = all_data[i:i + batch_size]
                    worksheet.update(f'A{i+2}', batch)
                
                logger.info(f"Updated All Futures with {len(all_data)} records")
            
        except Exception as e:
            logger.error(f"Error updating All Futures sheet: {e}")

    def update_exchange_stats_sheet(self, spreadsheet, exchange_stats, timestamp):
        """Update Exchange Stats sheet"""
        try:
            worksheet = spreadsheet.worksheet('Exchange Stats')
            
            # Clear existing data (keep headers)
            if worksheet.row_count > 1:
                worksheet.clear()
                # Re-add headers
                worksheet.update('A1', [[
                    'Exchange', 'Futures Count', 'Status', 'Last Updated', 
                    'Success Rate', 'Price Data Available'
                ]])
            
            stats_data = []
            for exchange, count in exchange_stats.items():
                status = "✅ WORKING" if count > 0 else "❌ FAILED"
                stats_data.append([
                    exchange,
                    count,
                    status,
                    timestamp,
                    "100%",  # Placeholder
                    "✅" if count > 0 else "❌"
                ])
            
            if stats_data:
                worksheet.update('A2', stats_data)
                logger.info(f"Updated Exchange Stats with {len(stats_data)} records")
            
        except Exception as e:
            logger.error(f"Error updating Exchange Stats sheet: {e}")

    def update_dashboard_stats(self, exchange_stats, unique_symbols_count, unique_futures_count, analyzed_prices):
        """Update dashboard statistics - simplified version"""
        if not self.spreadsheet:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet("Dashboard")
            
            # Count working exchanges
            working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
            total_exchanges = len(exchange_stats)
            
            # Get current time for next update
            next_update = (datetime.now() + timedelta(minutes=self.update_interval)).strftime('%H:%M:%S')
            
            # Update only the statistics section (rows 23-27)
            stats_update = [
                ["Next Data Update", next_update],
                ["Next Price Update", (datetime.now() + timedelta(minutes=self.price_check_interval)).strftime('%H:%M:%S')],
                ["Unique Futures Count", unique_futures_count],
                ["Working Exchanges", f"{working_exchanges}/{total_exchanges}"],
                ["Total Symbols", unique_symbols_count]
            ]
            
            # Update stats section (starting at row 23)
            for i, (label, value) in enumerate(stats_update):
                worksheet.update(f'A{23+i}:B{23+i}', [[label, value]])
                    
        except Exception as e:
            logger.error(f"Error updating dashboard stats: {e}")


    def update_unique_futures_sheet_with_prices(self, unique_futures, analyzed_prices):
        """Update Unique Futures sheet with price information"""
        try:
            worksheet = self.spreadsheet.worksheet('Unique Futures')
            
            # Clear existing data
            if worksheet.row_count > 1:
                worksheet.clear()
            
            # Enhanced headers with price changes
            headers = [
                'Symbol', 'Current Price', '5m Change %', '15m Change %', 
                '30m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated'
            ]
            worksheet.update('A1', [headers])
            
            # Prepare data
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create mapping for quick price lookup
            price_map = {item['symbol']: item for item in analyzed_prices}
            
            for symbol in sorted(unique_futures):
                price_info = price_map.get(symbol, {})
                changes = price_info.get('changes', {})
                
                row = [
                    symbol,
                    price_info.get('price', 'N/A'),
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{price_info.get('score', 0):.2f}",
                    'UNIQUE',
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet in batches
            if sheet_data:
                batch_size = 100
                for i in range(0, len(sheet_data), batch_size):
                    batch = sheet_data[i:i + batch_size]
                    worksheet.update(f'A{i+2}', batch)
                
                logger.info(f"✅ Updated Unique Futures with {len(sheet_data)} records")
            else:
                logger.warning("No unique futures data to update")
                
        except Exception as e:
            logger.error(f"Error updating Unique Futures sheet with prices: {e}")

    def update_mexc_analysis_sheet_with_prices(self, all_futures_data, symbol_coverage, analyzed_prices, timestamp):
        """Update MEXC Analysis sheet with price data"""
        try:
            worksheet = self.spreadsheet.worksheet('MEXC Analysis')
            
            # Get only MEXC futures
            mexc_futures = [f for f in all_futures_data if f['exchange'] == 'MEXC']
            logger.info(f"Updating MEXC Analysis with {len(mexc_futures)} futures")
            
            if not mexc_futures:
                logger.warning("No MEXC futures found to analyze")
                return
            
            # Clear the sheet completely and set up headers
            worksheet.clear()
            headers = [
                'MEXC Symbol', 'Normalized', 'Available On', 'Exchanges Count', 
                'Current Price', '5m Change %', '1h Change %', '4h Change %', 
                'Status', 'Unique', 'Timestamp'
            ]
            worksheet.update('A1', [headers])
            
            mexc_data = []
            
            # Create price mapping
            price_map = {item['symbol']: item for item in analyzed_prices}
            
            for future in mexc_futures:
                try:
                    symbol = future['symbol']
                    normalized = self.normalize_symbol_for_comparison(symbol)
                    exchanges_list = symbol_coverage.get(normalized, set())
                    available_on = ", ".join(sorted(exchanges_list)) if exchanges_list else "MEXC Only"
                    exchange_count = len(exchanges_list)
                    status = "Unique" if exchange_count == 1 else "Multi-exchange"
                    unique_flag = "✅" if exchange_count == 1 else "🔸"
                    
                    # Get price info
                    price_info = price_map.get(symbol, {})
                    changes = price_info.get('changes', {})
                    
                    row = [
                        symbol,
                        normalized,
                        available_on,
                        exchange_count,
                        price_info.get('price', 'N/A'),
                        self.format_change_for_sheet(changes.get('5m')),
                        self.format_change_for_sheet(changes.get('60m')),
                        self.format_change_for_sheet(changes.get('240m')),
                        status,
                        unique_flag,
                        timestamp
                    ]
                    mexc_data.append(row)
                    
                except Exception as e:
                    logger.error(f"Error processing MEXC future {future.get('symbol', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Processed {len(mexc_data)} MEXC futures for analysis")
            
            # Write data in batches
            if mexc_data:
                batch_size = 100
                for i in range(0, len(mexc_data), batch_size):
                    batch = mexc_data[i:i + batch_size]
                    start_row = i + 2  # +2 because row 1 is headers
                    range_str = f'A{start_row}'
                    worksheet.update(range_str, batch)
                
                logger.info(f"✅ Successfully updated MEXC Analysis with {len(mexc_data)} records")
            else:
                logger.warning("No MEXC data to write to analysis sheet")
            
        except Exception as e:
            logger.error(f"❌ Error updating MEXC Analysis sheet: {e}")

    def update_price_analysis_sheet(self, analyzed_prices):
        """Update Price Analysis sheet with top performers"""
        try:
            # Get or create Price Analysis sheet
            try:
                worksheet = self.spreadsheet.worksheet('Price Analysis')
            except gspread.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(title='Price Analysis', rows=1000, cols=12)
            
            # Clear existing data
            worksheet.clear()
            
            # Headers
            headers = [
                'Rank', 'Symbol', 'Current Price', '5m %', '15m %', '30m %', 
                '1h %', '4h %', 'Score', 'Trend', 'Volume', 'Last Updated'
            ]
            worksheet.update('A1', [headers])
            
            # Prepare data - top 50 performers
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for i, item in enumerate(analyzed_prices[:50], 1):
                changes = item.get('changes', {})
                
                # Determine trend
                latest_change = item.get('latest_change', 0)
                if latest_change > 5:
                    trend = "🚀 STRONG UP"
                elif latest_change > 2:
                    trend = "🟢 UP"
                elif latest_change < -5:
                    trend = "🔻 STRONG DOWN"
                elif latest_change < -2:
                    trend = "🔴 DOWN"
                else:
                    trend = "⚪ FLAT"
                
                row = [
                    i,
                    item['symbol'],
                    item.get('price', 'N/A'),
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{item.get('score', 0):.2f}",
                    trend,
                    'N/A',  # Volume would require additional API call
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet
            if sheet_data:
                worksheet.update('A2', sheet_data)
                logger.info(f"✅ Updated Price Analysis with {len(sheet_data)} top performers")
            else:
                logger.warning("No price data to update")
            
        except Exception as e:
            logger.error(f"Error updating Price Analysis sheet: {e}")

    def update_dashboard_with_comprehensive_stats(self, exchange_stats, unique_symbols_count, unique_futures_count, analyzed_prices):
        """Update the dashboard with comprehensive statistics including price analysis"""
        if not self.spreadsheet:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet("Dashboard")
            
            # Count working exchanges
            working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
            total_exchanges = len(exchange_stats)
            
            # Calculate price statistics
            top_performers = analyzed_prices[:10] if analyzed_prices else []
            strong_movers = [p for p in analyzed_prices if p.get('latest_change', 0) > 5]
            
            stats_update = [
                ["🤖 MEXC FUTURES AUTO-UPDATE DASHBOARD", ""],
                ["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Update Interval", f"{self.update_interval} minutes"],
                ["", ""],
                ["📊 EXCHANGE STATISTICS", ""],
                ["Working Exchanges", f"{working_exchanges}/{total_exchanges}"],
                ["Total Unique Symbols", unique_symbols_count],
                ["Unique MEXC Futures", unique_futures_count],
                ["", ""],
                ["💰 PRICE ANALYSIS", ""],
                ["Top Performers Tracked", len(top_performers)],
                ["Strong Movers (>5%)", len(strong_movers)],
                ["Best 5m Change", f"{top_performers[0].get('changes', {}).get('5m', 0):.2f}%" if top_performers else "N/A"],
                ["", ""],
                ["⚡ PERFORMANCE", ""],
                ["Next Auto-Update", (datetime.now() + timedelta(minutes=self.update_interval)).strftime('%H:%M:%S')],
                ["Status", "🟢 RUNNING"],
                ["", ""],
                ["🏆 TOP 5 PERFORMERS", ""],
            ]
            
            # Add top performers
            for i, performer in enumerate(top_performers[:5], 1):
                changes = performer.get('changes', {})
                stats_update.append([
                    f"{i}. {performer['symbol']}",
                    f"${performer.get('price', 0):.4f} ({changes.get('5m', 0):.2f}%)"
                ])
            
            # Update dashboard
            worksheet.clear()
            worksheet.update('A1', stats_update)
            
            logger.info("✅ Dashboard updated with comprehensive stats")
            
        except Exception as e:
            logger.error(f"Error updating dashboard stats: {e}")

    def format_change_for_sheet(self, change):
        """Format change for Google Sheets with color indicators"""
        if change is None:
            return 'N/A'
        
        # Add emoji based on change value
        if change > 10:
            return f"🚀 {change:+.2f}%"
        elif change > 5:
            return f"🟢 {change:+.2f}%"
        elif change > 2:
            return f"📈 {change:+.2f}%"
        elif change < -10:
            return f"💥 {change:+.2f}%"
        elif change < -5:
            return f"🔴 {change:+.2f}%"
        elif change < -2:
            return f"📉 {change:+.2f}%"
        else:
            return f"{change:+.2f}%"

    # Also update the forceupdate command to use the new method
    def ensure_sheets_initialized(self):
        """Ensure all required sheets exist and have proper headers with enough rows"""
        if not self.spreadsheet:
            return False
        
        try:
            # Get existing sheets
            existing_sheets = self.spreadsheet.worksheets()
            existing_sheet_names = [sheet.title for sheet in existing_sheets]
            
            logger.info(f"Existing sheets: {existing_sheet_names}")
            
            # Keep Dashboard if it exists, otherwise use first sheet
            main_sheet = None
            if 'Dashboard' in existing_sheet_names:
                main_sheet = self.spreadsheet.worksheet('Dashboard')
            elif existing_sheets:
                main_sheet = existing_sheets[0]
                main_sheet.update_title("Dashboard")
            else:
                # Create Dashboard if no sheets exist
                main_sheet = self.spreadsheet.add_worksheet(title="Dashboard", rows=50, cols=10)
            
            # Define sheets with enhanced headers for price data
            sheets_config = {
                'Unique Futures': {
                    'rows': 1000,
                    'cols': 11,  # Increased for price data
                    'headers': [
                        'Symbol', 'Current Price', '5m Change %', '15m Change %', 
                        '30m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated'
                    ]
                },
                'All Futures': {
                    'rows': 3000,
                    'cols': 8,  # Increased columns
                    'headers': [
                        'Symbol', 'Exchange', 'Normalized', 'Available On', 
                        'Coverage', 'Timestamp', 'Unique', 'Current Price'
                    ]
                },
                'MEXC Analysis': {
                    'rows': 1000,
                    'cols': 12,  # Increased for price analysis
                    'headers': [
                        'MEXC Symbol', 'Normalized', 'Available On', 'Exchanges Count', 
                        'Current Price', '5m Change %', '1h Change %', '4h Change %', 
                        'Status', 'Unique', 'Timestamp', 'Price Source'
                    ]
                },
                'Price Analysis': {
                    'rows': 1000,
                    'cols': 12,
                    'headers': [
                        'Rank', 'Symbol', 'Current Price', '5m %', '15m %', '30m %', 
                        '1h %', '4h %', 'Score', 'Trend', 'Volume', 'Last Updated'
                    ]
                },
                'Exchange Stats': {
                    'rows': 20,
                    'cols': 6,  # Added price data column
                    'headers': [
                        'Exchange', 'Futures Count', 'Status', 'Last Updated', 
                        'Success Rate', 'Price Data Available'
                    ]
                }
            }
            
            # Create or update sheets
            for sheet_name, config in sheets_config.items():
                try:
                    if sheet_name in existing_sheet_names:
                        # Sheet exists, just clear and update headers
                        worksheet = self.spreadsheet.worksheet(sheet_name)
                        # Clear existing data but keep formatting
                        worksheet.clear()
                        # Update headers
                        worksheet.update('A1', [config['headers']])
                        logger.info(f"✅ Updated existing sheet: {sheet_name}")
                    else:
                        # Create new sheet
                        worksheet = self.spreadsheet.add_worksheet(
                            title=sheet_name, 
                            rows=config['rows'],
                            cols=config['cols']
                        )
                        worksheet.update('A1', [config['headers']])
                        logger.info(f"✅ Created new sheet: {sheet_name} with {config['rows']} rows")
                    
                    # Apply basic formatting
                    try:
                        worksheet.format('A1:Z1', {
                            'textFormat': {'bold': True},
                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                        })
                        
                        # Auto-resize columns for better readability
                        if hasattr(worksheet, 'columns_auto_resize'):
                            worksheet.columns_auto_resize(0, min(config['cols'], 10))
                        
                        # Freeze header row
                        worksheet.freeze(rows=1)
                        
                    except Exception as format_error:
                        logger.warning(f"Could not format sheet {sheet_name}: {format_error}")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing sheet {sheet_name}: {e}")
                    # Continue with other sheets even if one fails
            
            # Delete any unexpected sheets (optional - be careful with this)
            self.cleanup_unexpected_sheets(existing_sheet_names, list(sheets_config.keys()) + ['Dashboard'])
            
            # Setup Dashboard with comprehensive data
            self.setup_dashboard_sheet(main_sheet)
            
            logger.info("✅ All sheets initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error ensuring sheets initialized: {e}")
            return False

    def cleanup_unexpected_sheets(self, existing_sheet_names, expected_sheets):
        """Remove sheets that are not in the expected list"""
        try:
            for sheet_name in existing_sheet_names:
                if sheet_name not in expected_sheets:
                    try:
                        worksheet = self.spreadsheet.worksheet(sheet_name)
                        self.spreadsheet.del_worksheet(worksheet)
                        logger.info(f"🗑️ Removed unexpected sheet: {sheet_name}")
                    except Exception as e:
                        logger.warning(f"Could not remove sheet {sheet_name}: {e}")
        except Exception as e:
            logger.error(f"Error cleaning up sheets: {e}")

    def setup_dashboard_sheet(self, worksheet):
        """Setup the dashboard sheet with comprehensive information"""
        try:
            # Clear existing data
            worksheet.clear()
            
            # Comprehensive dashboard data
            dashboard_data = [
                ["🤖 MEXC FUTURES AUTO-UPDATE DASHBOARD", ""],
                ["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Update Interval", f"{self.update_interval} minutes"],
                ["Price Check Interval", f"{self.price_check_interval} minutes"],
                ["", ""],
                ["📊 EXCHANGE MONITORING", ""],
                ["Total Exchanges Tracked", "8"],
                ["Primary Exchange", "MEXC"],
                ["Comparison Exchanges", "Binance, Bybit, OKX, Gate.io, KuCoin, BingX, BitGet"],
                ["", ""],
                ["🎯 UNIQUE FUTURES TRACKING", ""],
                ["Auto Unique Detection", "✅ ENABLED"],
                ["Price Monitoring", "✅ ENABLED"],
                ["Telegram Alerts", "✅ ENABLED"],
                ["Google Sheets Sync", "✅ ENABLED"],
                ["", ""],
                ["💰 PRICE ANALYSIS FEATURES", ""],
                ["Timeframes Tracked", "5m, 15m, 30m, 1h, 4h"],
                ["Top Performers", "Top 50 ranked by score"],
                ["Trend Analysis", "🚀🟢📈🔴📉 emoji indicators"],
                ["Volume Tracking", "⚡ Coming soon"],
                ["", ""],
                ["⚡ REAL-TIME STATS", ""],
                ["Next Data Update", "Will update automatically"],
                ["Next Price Update", "Will update automatically"],
                ["Unique Futures Count", "Will update automatically"],
                ["Top Performer", "Will update automatically"],
                ["", ""],
                ["🔧 SHEETS OVERVIEW", ""],
                ["Dashboard", "This overview and real-time stats"],
                ["Unique Futures", "Futures only on MEXC with prices"],
                ["All Futures", "All futures from all exchanges"],
                ["MEXC Analysis", "Detailed MEXC coverage with prices"],
                ["Price Analysis", "Top 50 performers with trends"],
                ["Exchange Stats", "Exchange performance metrics"],
                ["", ""],
                ["💡 QUICK START", ""],
                ["1. Check /status", "Current bot status"],
                ["2. Use /findunique", "Find unique MEXC futures"],
                ["3. Check /toppers", "Top performing futures"],
                ["4. Use /verifyunique", "Verify symbol uniqueness"],
                ["", ""],
                ["🆘 SUPPORT", ""],
                ["Use /help", "Complete command list"],
                ["Use /check", "Force immediate data update"],
                ["Check logs", "For detailed debugging info"]
            ]
            
            # Update the dashboard
            worksheet.update('A1', dashboard_data)
            
            # Apply formatting to dashboard
            try:
                # Format title row
                worksheet.format('A1:B1', {
                    'textFormat': {'bold': True, 'fontSize': 14},
                    'backgroundColor': {'red': 0.8, 'green': 0.9, 'blue': 1.0},
                    'horizontalAlignment': 'CENTER'
                })
                
                # Format section headers
                section_rows = [5, 11, 17, 23, 29, 35, 41]
                for row in section_rows:
                    worksheet.format(f'A{row}:B{row}', {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
                    })
                
                # Auto-resize columns
                worksheet.columns_auto_resize(0, 2)
                
            except Exception as format_error:
                logger.warning(f"Could not format dashboard: {format_error}")
            
            logger.info("✅ Dashboard setup completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Error setting up dashboard: {e}")

    def force_update_command(self, update: Update, context: CallbackContext):
        """Force immediate Google Sheet update with comprehensive data"""
        if not self.gs_client:
            update.message.reply_html("❌ Google Sheets not configured.")
            return
        
        try:
            update.message.reply_html("🔄 <b>Force updating Google Sheet with comprehensive data...</b>")
            
            # Ensure sheets are properly initialized with new structure
            if not self.ensure_sheets_initialized():
                update.message.reply_html("❌ Failed to initialize sheets.")
                return
            
            # Run the comprehensive update
            self.update_google_sheet()
            
            # Get the spreadsheet URL for the message
            data = self.load_data()
            sheet_url = data.get('google_sheet_url') or (self.spreadsheet.url if self.spreadsheet else 'N/A')
            
            update.message.reply_html(
                f"✅ <b>Google Sheet updated successfully!</b>\n\n"
                f"📊 <a href='{sheet_url}'>Open Your Sheet</a>\n\n"
                f"<b>Updated Sheets:</b>\n"
                f"• 📈 <b>Dashboard</b> - Overview and stats\n"
                f"• 🎯 <b>Unique Futures</b> - MEXC-only with price changes\n"
                f"• 📋 <b>All Futures</b> - All symbols from all exchanges\n"
                f"• 🔍 <b>MEXC Analysis</b> - Detailed coverage with prices\n"
                f"• 💰 <b>Price Analysis</b> - Top 50 performers\n"
                f"• 📊 <b>Exchange Stats</b> - Performance metrics\n\n"
                f"<b>Price Features:</b>\n"
                f"• 5m, 15m, 30m, 1h, 4h changes\n"
                f"• 🚀 Trend indicators\n"
                f"• 📈 Performance scoring\n"
                f"• 🔄 Auto-updates every {self.update_interval}min",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception as e:
            update.message.reply_html(f"❌ <b>Force update error:</b>\n{str(e)}")

    def _make_request_with_retry(self, url: str, timeout: int = 15, max_retries: int = 3) -> Optional[requests.Response]:
        """Make request with retry logic and proxy rotation"""
        for attempt in range(max_retries):
            try:
                proxy = random.choice(self.proxies) if self.proxies else {}
                response = self.session.get(url, timeout=timeout, proxies=proxy if proxy else None)
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 429]:
                    logger.warning(f"⚠️  Blocked on attempt {attempt + 1} for {url}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                else:
                    logger.error(f"❌ HTTP {response.status_code} for {url}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️  Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        return None



    def format_start_time(self, start_time):
        """Format start time for display"""
        if start_time:
            try:
                # Handle both string and datetime objects
                if isinstance(start_time, str):
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                else:
                    dt = start_time
                return dt.strftime("%Y-%m-%d %H:%M")
            except:
                pass
        return "Unknown"
    
    def get_uptime(self):
        """Calculate bot uptime"""
        data = self.load_data()
        start_time = data.get('statistics', {}).get('start_time')
        if start_time:
            try:
                if isinstance(start_time, str):
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                else:
                    start_dt = start_time
                uptime = datetime.now() - start_dt
                days = uptime.days
                hours = uptime.seconds // 3600
                minutes = (uptime.seconds % 3600) // 60
                return f"{days}d {hours}h {minutes}m"
            except:
                pass
        return "Unknown"            

    def start_command(self, update: Update, context: CallbackContext):
        """Send welcome message"""
        user = update.effective_user
        welcome_text = (
            f"🤖 Hello {user.mention_html()}!\n\n"
            "I'm <b>MEXC Unique Futures Tracker</b>\n\n"
            "<b>Features:</b>\n"
            "• Real-time monitoring of 8 exchanges\n"
            "• Unique futures detection\n"
            "• Price movement analysis\n"
            "• Automatic alerts\n"
            "• Google Sheets integration\n\n"
            "<b>Commands:</b>\n"
            "/start - Welcome message\n"
            "/status - Current status\n"
            "/check - Immediate check\n"
            "/analysis - Full analysis\n"
            "/exchanges - Exchange info\n"
            "/stats - Bot statistics\n"
            "/help - Help information\n"
            "/findunique - Find unique futures\n"
            "/forceupdate - Force update Google Sheet\n"
            "/checksymbol SYMBOL - Check specific symbol\n"
            "/prices - Check current prices\n"
            "/toppers - Top performing futures\n\n"
            "⚡ <i>Happy trading!</i>"
        )
        update.message.reply_html(welcome_text)

    def prices_command(self, update: Update, context: CallbackContext):
        """Get current price information for unique futures"""
        update.message.reply_html("📊 <b>Getting current prices...</b>")
        
        try:
            unique_futures, _ = self.find_unique_futures_robust()
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Filter to only unique futures
            unique_prices = [p for p in analyzed_prices if p['symbol'] in unique_futures]
            
            if not unique_prices:
                update.message.reply_html("❌ No price data available for unique futures")
                return
            
            message = "💰 <b>Unique Futures Prices</b>\n\n"
            
            for i, item in enumerate(unique_prices[:10], 1):  # Show top 10
                changes = item.get('changes', {})
                message += f"{i}. <b>{item['symbol']}</b>\n"
                message += f"   Price: ${item['price']:.4f}\n"
                
                if '5m' in changes:
                    message += f"   5m: {self.format_change(changes['5m'])}\n"
                if '1h' in changes:
                    message += f"   1h: {self.format_change(changes.get('60m', 0))}\n"
                if '4h' in changes:
                    message += f"   4h: {self.format_change(changes.get('240m', 0))}\n"
                
                message += "\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Error getting prices: {str(e)}")

    def top_performers_command(self, update: Update, context: CallbackContext):
        """Show top performing futures"""
        update.message.reply_html("🚀 <b>Analyzing top performers...</b>")
        
        try:
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            if not analyzed_prices:
                update.message.reply_html("❌ No price data available")
                return
            
            message = "🏆 <b>Top Performing Futures</b>\n\n"
            
            for i, item in enumerate(analyzed_prices[:15], 1):
                changes = item.get('changes', {})
                message += f"{i}. <b>{item['symbol']}</b>\n"
                message += f"   Price: ${item['price']:.4f}\n"
                
                change_5m = changes.get('5m', 0)
                change_1h = changes.get('60m', 0)
                change_4h = changes.get('240m', 0)
                
                message += f"   5m: {self.format_change(change_5m)}"
                message += f" | 1h: {self.format_change(change_1h)}"
                message += f" | 4h: {self.format_change(change_4h)}\n"
                
                # Add emoji for very strong performers
                if change_5m > 10 or change_1h > 20:
                    message += "   🚀 <b>STRONG UPTREND!</b>\n"
                elif change_5m > 5 or change_1h > 10:
                    message += "   📈 <b>Uptrend</b>\n"
                
                message += "\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Error analyzing performers: {str(e)}")

    def check_command(self, update: Update, context: CallbackContext):
        """Perform immediate check with colorful visual progress bar"""
        try:
            # Send initial message
            progress_message = update.message.reply_html(
                "🚀 <b>Starting Comprehensive Exchange Analysis</b>\n\n"
                "⚡ Initializing tracking systems...\n"
                "▰▱▱▱▱▱▱▱▱▱ 10%"
            )
            
            def update_progress(step, total_steps, status, current_exchange=None, count=None):
                """Update progress bar with colors"""
                try:
                    # Calculate progress
                    progress_percent = (step / total_steps) * 100
                    filled_blocks = int(progress_percent / 10)
                    empty_blocks = 10 - filled_blocks
                    
                    # Colorful progress bar based on completion
                    if progress_percent < 30:
                        progress_bar = "🟦" * filled_blocks + "⬜" * empty_blocks
                    elif progress_percent < 70:
                        progress_bar = "🟨" * filled_blocks + "⬜" * empty_blocks
                    else:
                        progress_bar = "🟩" * filled_blocks + "⬜" * empty_blocks
                    
                    # Build animated status
                    spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"][step % 10]
                    
                    message = f"🚀 <b>Comprehensive Exchange Analysis</b>\n\n"
                    message += f"{spinner} <b>Progress:</b> {progress_bar} {progress_percent:.0f}%\n"
                    message += f"📝 <b>Status:</b> {status}\n"
                    
                    if current_exchange and count is not None:
                        if count > 0:
                            message += f"✅ <b>{current_exchange}:</b> {count} futures found\n"
                        else:
                            message += f"❌ <b>{current_exchange}:</b> Failed\n"
                    
                    message += f"\n⏰ Step {step+1} of {total_steps}"
                    
                    # Update the message
                    context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=progress_message.message_id,
                        text=message,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.debug(f"Progress update failed: {e}")

            # Define check steps with more detail
            steps = [
                ("Initializing systems", "⚡ Starting tracking systems..."),
                ("Checking MEXC", "🔍 Scanning MEXC futures database..."),
                ("Checking Binance", "🌐 Connecting to Binance API..."),
                ("Checking Bybit", "🔄 Accessing Bybit perpetuals..."),
                ("Checking OKX", "📊 Analyzing OKX swap contracts..."),
                ("Checking Gate.io", "🔍 Scanning Gate.io futures..."),
                ("Checking KuCoin", "📈 Checking KuCoin derivatives..."),
                ("Checking BingX", "🔄 Accessing BingX futures..."),
                ("Checking BitGet", "🔍 Analyzing BitGet perpetuals..."),
                ("Finding unique symbols", "🎯 Calculating unique futures..."),
                ("Analyzing results", "📊 Compiling comprehensive report..."),
                ("Finalizing", "✅ Completing analysis...")
            ]

            exchange_results = {}
            data_before = self.load_data()
            unique_before = set(data_before.get('unique_futures', []))
            
            # Execute each step with progress updates
            for i, (step_name, status_text) in enumerate(steps):
                try:
                    current_count = 0
                    current_exchange = None
                    
                    if step_name.startswith("Checking "):
                        current_exchange = step_name.replace("Checking ", "")
                    
                    # Update progress
                    update_progress(i, len(steps), status_text, current_exchange, current_count)
                    time.sleep(0.8)  # Smooth animation
                    
                    # Execute the actual step
                    if step_name == "Checking MEXC":
                        mexc_futures = self.get_mexc_futures()
                        exchange_results['MEXC'] = len(mexc_futures)
                        current_count = len(mexc_futures)
                        
                    elif step_name == "Checking Binance":
                        binance_futures = self.get_binance_futures()
                        exchange_results['Binance'] = len(binance_futures)
                        current_count = len(binance_futures)
                        
                    elif step_name == "Checking Bybit":
                        bybit_futures = self.get_bybit_futures()
                        exchange_results['Bybit'] = len(bybit_futures)
                        current_count = len(bybit_futures)
                        
                    elif step_name == "Checking OKX":
                        okx_futures = self.get_okx_futures()
                        exchange_results['OKX'] = len(okx_futures)
                        current_count = len(okx_futures)
                        
                    elif step_name == "Checking Gate.io":
                        gate_futures = self.get_gate_futures()
                        exchange_results['Gate.io'] = len(gate_futures)
                        current_count = len(gate_futures)
                        
                    elif step_name == "Checking KuCoin":
                        kucoin_futures = self.get_kucoin_futures()
                        exchange_results['KuCoin'] = len(kucoin_futures)
                        current_count = len(kucoin_futures)
                        
                    elif step_name == "Checking BingX":
                        bingx_futures = self.get_bingx_futures()
                        exchange_results['BingX'] = len(bingx_futures)
                        current_count = len(bingx_futures)
                        
                    elif step_name == "Checking BitGet":
                        bitget_futures = self.get_bitget_futures()
                        exchange_results['BitGet'] = len(bitget_futures)
                        current_count = len(bitget_futures)
                        
                    elif step_name == "Finding unique symbols":
                        new_futures, lost_futures = self.monitor_unique_futures_changes()
                        
                    elif step_name == "Analyzing results":
                        data_after = self.load_data()
                        unique_after = set(data_after.get('unique_futures', []))
                        
                    # Update progress with results
                    update_progress(i, len(steps), status_text, current_exchange, current_count)
                        
                except Exception as e:
                    logger.error(f"Step {step_name} failed: {e}")
                    if step_name.startswith("Checking "):
                        exchange_name = step_name.replace("Checking ", "")
                        exchange_results[exchange_name] = 0
                        update_progress(i, len(steps), f"❌ {status_text}", exchange_name, 0)

            # Final progress update
            update_progress(len(steps), len(steps), "✅ Check complete!", exchange_results)
            time.sleep(1)

            # Build final results message
            working_exchanges = [name for name, count in exchange_results.items() if count > 0]
            total_futures = sum(exchange_results.values())
            
            # Get unique futures count
            data_after = self.load_data()
            unique_after = set(data_after.get('unique_futures', []))
            unique_before = set(data_before.get('unique_futures', []))
            
            new_futures = unique_after - unique_before
            lost_futures = unique_before - unique_after

            # Create final report
            final_message = "🎯 <b>COMPREHENSIVE CHECK COMPLETE</b>\n\n"
            
            # Exchange Statistics
            final_message += "📊 <b>EXCHANGE STATISTICS</b>\n"
            final_message += f"✅ Working: {len(working_exchanges)}/{len(exchange_results)} exchanges\n"
            final_message += f"📈 Total Futures: {total_futures}\n"
            final_message += f"🎯 MEXC Unique: {len(unique_after)}\n\n"
            
            # Detailed Exchange Results
            final_message += "🔍 <b>DETAILED RESULTS</b>\n"
            for exchange in ['MEXC', 'Binance', 'Bybit', 'OKX', 'Gate.io', 'KuCoin', 'BingX', 'BitGet']:
                count = exchange_results.get(exchange, 0)
                status = "✅" if count > 0 else "❌"
                final_message += f"{status} {exchange}: {count} futures\n"
            
            # Changes detected
            final_message += f"\n🔄 <b>CHANGES DETECTED</b>\n"
            if new_futures:
                final_message += f"🆕 New Unique: {len(new_futures)}\n"
                # Show first 3 new symbols
                for i, symbol in enumerate(list(new_futures)[:3], 1):
                    final_message += f"   {i}. {symbol}\n"
                if len(new_futures) > 3:
                    final_message += f"   ... and {len(new_futures) - 3} more\n"
            else:
                final_message += "🆕 New Unique: None\n"
                
            if lost_futures:
                final_message += f"📉 Lost Unique: {len(lost_futures)}\n"
            else:
                final_message += "📉 Lost Unique: None\n"
            
            # Performance summary
            final_message += f"\n⚡ <b>SUMMARY</b>\n"
            final_message += f"📊 MEXC Coverage: {len(unique_after)}/{exchange_results.get('MEXC', 0)} unique\n"
            final_message += f"🔄 Unique Ratio: {(len(unique_after)/exchange_results.get('MEXC', 1)*100):.1f}%\n"
            final_message += f"⏰ Next Auto-check: {self.update_interval} minutes\n\n"
            
            final_message += "✅ <i>Check completed successfully!</i>"

            # Send final message
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=progress_message.message_id,
                text=final_message,
                parse_mode='HTML'
            )

        except Exception as e:
            # Error handling
            error_message = (
                "❌ <b>CHECK FAILED</b>\n\n"
                f"<b>Error:</b> {str(e)}\n\n"
                "🔧 <i>The check encountered an unexpected error. "
                "This might be due to network issues or exchange API problems. "
                "Try again in a few moments.</i>"
            )
            
            try:
                context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=progress_message.message_id,
                    text=error_message,
                    parse_mode='HTML'
                )
            except:
                # If we can't edit, send new message
                update.message.reply_html(error_message)
            
            logger.error(f"Check command failed: {e}")

            
    def find_unique_command(self, update: Update, context: CallbackContext):
        """Find and display currently unique symbols with prices"""
        update.message.reply_html("🔍 Scanning for unique MEXC symbols with prices...")
        
        try:
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            price_data = self.get_all_mexc_prices()
            
            if not unique_futures:
                update.message.reply_html("❌ No unique symbols found on MEXC")
                return
            
            # Get price info for unique futures
            unique_with_prices = []
            for symbol in unique_futures:
                price_info = price_data.get(symbol)
                if price_info:
                    unique_with_prices.append({
                        'symbol': symbol,
                        'price': price_info['price'],
                        'changes': price_info.get('changes', {})
                    })
                else:
                    unique_with_prices.append({
                        'symbol': symbol,
                        'price': None,
                        'changes': {}
                    })
            
            # Sort by 5m change if available
            unique_with_prices.sort(key=lambda x: x['changes'].get('5m', 0), reverse=True)
            
            response = f"🎯 <b>Unique MEXC Symbols: {len(unique_futures)}</b>\n\n"
            
            for i, item in enumerate(unique_with_prices[:15], 1):
                response += f"{i}. <b>{item['symbol']}</b>"
                if item['price']:
                    response += f" - ${item['price']:.4f}"
                    if '5m' in item['changes']:
                        response += f" {self.format_change(item['changes']['5m'])}"
                response += "\n"
            
            if len(unique_with_prices) > 15:
                response += f"\n... and {len(unique_with_prices) - 15} more symbols"
            
            response += f"\n\n💡 Use /prices for detailed price info"
            
            update.message.reply_html(response)
            
        except Exception as e:
            update.message.reply_html(f"❌ Error finding unique symbols: {str(e)}")

    def check_symbol_command(self, update: Update, context: CallbackContext):
        """Check if a symbol is unique to MEXC"""
        if not context.args:
            update.message.reply_html("Usage: /checksymbol SYMBOL\nExample: /checksymbol BTC")
            return
        
        symbol = context.args[0].upper()
        update.message.reply_html(f"🔍 Checking symbol: {symbol}")
        
        try:
            coverage = self.verify_symbol_coverage(symbol)
            
            if not coverage:
                response = f"❌ Symbol not found on any exchange: {symbol}"
            elif len(coverage) == 1 and 'MEXC' in coverage:
                response = f"🎯 <b>UNIQUE TO MEXC!</b>\n\n{symbol} - Only available on: <b>MEXC</b>"
            elif 'MEXC' in coverage:
                other_exchanges = [e for e in coverage if e != 'MEXC']
                response = (f"📊 <b>{symbol} - Multi-Exchange</b>\n\n"
                        f"✅ Available on MEXC\n"
                        f"🔸 Also on: {', '.join(other_exchanges)}\n"
                        f"📈 Total exchanges: {len(coverage)}")
            else:
                response = f"📊 <b>{symbol}</b>\n\nNot on MEXC, available on:\n• " + "\n• ".join(coverage)
            
            update.message.reply_html(response)
            
        except Exception as e:
            update.message.reply_html(f"❌ Error checking symbol: {str(e)}")

    def status_command(self, update: Update, context: CallbackContext):
        """Send current status"""
        data = self.load_data()
        unique_count = len(data.get('unique_futures', []))
        last_check = data.get('last_check', 'Never')
        exchange_stats = data.get('exchange_stats', {})
        
        if last_check != 'Never':
            try:
                last_dt = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                last_check = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        status_text = (
            "📈 <b>Bot Status</b>\n\n"
            f"🎯 Current unique: <b>{unique_count}</b>\n"
            f"📅 Last check: {last_check}\n"
            f"⚡ Auto-check: {self.update_interval}min\n"
        )
        
        # Show exchange status
        working_exchanges = [name for name, count in exchange_stats.items() if count > 0]
        status_text += f"✅ Working exchanges: {len(working_exchanges)}/7\n"
        
        # Show unique futures if any
        if unique_count > 0:
            status_text += "\n<b>🎯 Unique futures:</b>\n"
            for symbol in sorted(data['unique_futures'])[:5]:
                status_text += f"• {symbol}\n"
            if unique_count > 5:
                status_text += f"• ... and {unique_count - 5} more"
        
        update.message.reply_html(status_text)

    def analysis_command(self, update: Update, context: CallbackContext):
        """Create comprehensive analysis"""
        update.message.reply_html("📈 <b>Creating comprehensive analysis...</b>")
        
        try:
            # Get fresh data
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            
            # Create analysis report
            report = self.create_analysis_report(unique_futures, exchange_stats)
            
            # Send as document
            file_obj = io.BytesIO(report.encode('utf-8'))
            file_obj.name = f'mexc_analysis_{datetime.now().strftime("%Y%m%d_%H%M")}.txt'
            
            update.message.reply_document(
                document=file_obj,
                caption=f"📊 <b>MEXC Analysis Complete</b>\n\n"
                       f"🎯 Unique futures: {len(unique_futures)}\n"
                       f"🏢 Exchanges: {len(exchange_stats) + 1}\n"
                       f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode='HTML'
            )
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Analysis error:</b>\n{str(e)}")

    def create_analysis_report(self, unique_futures, exchange_stats):
        """Create comprehensive analysis report"""
        report = []
        report.append("=" * 60)
        report.append("🎯 MEXC UNIQUE FUTURES ANALYSIS REPORT")
        report.append("=" * 60)
        report.append(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Exchange statistics
        report.append("🏭 EXCHANGE STATISTICS:")
        total_futures = sum(exchange_stats.values())
        report.append(f"  MEXC: {len(self.get_mexc_futures())} futures")
        for exchange, count in exchange_stats.items():
            status = "✅" if count > 0 else "❌"
            report.append(f"  {status} {exchange}: {count} futures")
        
        report.append(f"  Total futures from other exchanges: {total_futures}")
        report.append("")
        
        # Unique futures
        report.append(f"🎯 UNIQUE MEXC FUTURES ({len(unique_futures)}):")
        if unique_futures:
            for i, symbol in enumerate(sorted(unique_futures), 1):
                report.append(f"  {i:2d}. {symbol}")
        else:
            report.append("  No unique futures found")
        
        report.append("")
        report.append("📊 ANALYSIS SUMMARY:")
        report.append(f"  MEXC futures analyzed: {len(self.get_mexc_futures())}")
        report.append(f"  Unique ratio: {len(unique_futures)}/{len(self.get_mexc_futures())}")
        report.append(f"  Market coverage: {len(exchange_stats) + 1} exchanges")
        
        report.append("=" * 60)
        
        return "\n".join(report)

    def exchanges_command(self, update: Update, context: CallbackContext):
        """Show exchange information"""
        data = self.load_data()
        exchange_stats = data.get('exchange_stats', {})
        
        exchanges_text = "🏢 <b>Supported Exchanges</b>\n\n"
        exchanges_text += "🎯 <b>MEXC</b> (source)\n"
        
        if exchange_stats:
            exchanges_text += "\n<b>Other exchanges:</b>\n"
            for exchange, count in sorted(exchange_stats.items()):
                status = "✅" if count > 0 else "❌"
                exchanges_text += f"{status} {exchange}: {count} futures\n"
        else:
            exchanges_text += "\nNo data. Use /check first."
        
        exchanges_text += f"\n🔍 Monitoring <b>{len(exchange_stats) + 1}</b> exchanges"
        
        update.message.reply_html(exchanges_text)

    def stats_command(self, update: Update, context: CallbackContext):
        """Show statistics"""
        data = self.load_data()
        stats = data.get('statistics', {})
        exchange_stats = data.get('exchange_stats', {})
        
        stats_text = (
            "📈 <b>Bot Statistics</b>\n\n"
            f"🔄 Checks performed: <b>{stats.get('checks_performed', 0)}</b>\n"
            f"🎯 Max unique found: <b>{stats.get('unique_found_total', 0)}</b>\n"
            f"⏰ Current unique: <b>{len(data.get('unique_futures', []))}</b>\n"
            f"🏢 Exchanges: <b>{len(exchange_stats) + 1}</b>\n"
            f"📅 Running since: {self.format_start_time(stats.get('start_time'))}\n"
            f"🤖 Uptime: {self.get_uptime()}\n"
            f"⚡ Auto-check: {self.update_interval}min"
        )
        
        update.message.reply_html(stats_text)

    def help_command(self, update: Update, context: CallbackContext):
        """Show help information"""
        help_text = (
            "🆘 <b>MEXC Futures Tracker - Help</b>\n\n"
            "<b>Monitoring 8 exchanges:</b>\n"
            "MEXC, Binance, Bybit, OKX,\n"
            "Gate.io, KuCoin, BingX, BitGet\n\n"
            "<b>Main commands:</b>\n"
            "/check - Quick check for unique futures\n"
            "/analysis - Full analysis report\n"
            "/status - Current status\n"
            "/exchanges - Exchange information\n"
            "/stats - Bot statistics\n"
            "/findunique - Find currently unique symbols\n"
            "/forceupdate - Force update Google Sheet\n"
            "/checksymbol SYMBOL - Check specific symbol\n\n"
            f"⚡ Auto-checks every {self.update_interval} minutes\n"
            "🎯 Alerts for new unique futures\n"
            "📊 Comprehensive analysis available\n\n"
            "⚡ <i>Happy trading!</i>"
        )
        update.message.reply_html(help_text)


    # ==================== SCHEDULER ====================

    def setup_scheduler(self):
        """Setup scheduled tasks"""
        # Unique futures monitoring
        schedule.every(self.update_interval).minutes.do(self.monitor_unique_futures_changes)
        
        # Price monitoring (more frequent)
        schedule.every(self.price_check_interval).minutes.do(self.run_price_monitoring)
        
        logger.info(f"Scheduler setup - unique check every {self.update_interval}min, prices every {self.price_check_interval}min")

    def run_price_monitoring(self):
        """Run price monitoring and alert on significant movements"""
        try:
            logger.info("💰 Running price monitoring...")
            
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Check for significant movers
            significant_movers = []
            for item in analyzed_prices[:20]:  # Check top 20
                changes = item.get('changes', {})
                change_5m = changes.get('5m', 0)
                change_1h = changes.get('60m', 0)
                
                # Alert criteria
                if change_5m > 10 or change_1h > 25:  # 10% in 5min or 25% in 1h
                    significant_movers.append(item)
            
            # Send alerts for significant movers
            if significant_movers:
                self.send_price_alert(significant_movers)
            
        except Exception as e:
            logger.error(f"Price monitoring error: {e}")

    def send_price_alert(self, significant_movers):
        """Send alert for significant price movements"""
        try:
            message = "🚨 <b>SIGNIFICANT PRICE MOVEMENTS!</b>\n\n"
            
            for item in significant_movers[:5]:  # Max 5 alerts
                changes = item.get('changes', {})
                message += f"📈 <b>{item['symbol']}</b>\n"
                message += f"   Price: ${item['price']:.4f}\n"
                
                if changes.get('5m', 0) > 10:
                    message += f"   🚀 5m: {self.format_change(changes['5m'])}\n"
                if changes.get('60m', 0) > 25:
                    message += f"   📊 1h: {self.format_change(changes['60m'])}\n"
                
                message += "\n"
            
            self.send_broadcast_message(message)
            
        except Exception as e:
            logger.error(f"Error sending price alert: {e}")

    # ==================== DATA MANAGEMENT ====================

    def init_data_file(self):
        """Initialize data in memory"""
        self.data = self.get_default_data()

    def load_data(self):
        """Load data from memory"""
        return self.data
    
    def save_data(self, data):
        """Save data to memory"""
        self.data = data
        logger.info("Data saved to memory")

    def get_default_data(self):
        """Return default data structure"""
        return {
            "unique_futures": [],
            "last_check": None,
            "statistics": {
                "checks_performed": 0,
                "unique_found_total": 0,
                "start_time": datetime.now().isoformat()
            },
            "exchange_stats": {},
            "google_sheet_url": None,
            "price_alerts_sent": {}
        }

    def send_broadcast_message(self, message):
        """Send message to configured chat"""
        try:
            if self.chat_id:
                self.bot.send_message(
                    chat_id=self.chat_id,
                    text=message,
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Broadcast error: {e}")

    def run(self):
        """Start the bot"""
        try:
            # Load initial data
            data = self.load_data()
            self.last_unique_futures = set(data.get('unique_futures', []))
            
            # Setup scheduler
            self.setup_scheduler()
            
            # Start scheduler in background
            scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
            scheduler_thread.start()
            
            # Start the bot
            self.updater.start_polling()
            
            logger.info("Bot started successfully")
            
            # Send startup message
            startup_msg = (
                "🤖 <b>MEXC Futures Tracker Started</b>\n\n"
                "✅ Monitoring 8 exchanges\n"
                f"⏰ Unique check: {self.update_interval} minutes\n"
                f"💰 Price check: {self.price_check_interval} minutes\n"
                "🎯 Unique futures detection\n"
                "🚀 Price movement alerts\n"
                "💬 Use /help for commands"
            )
            
            self.send_broadcast_message(startup_msg)
            
            logger.info("Bot is now running and ready for commands...")
            
            # Keep running
            self.updater.idle()
                
        except Exception as e:
            logger.error(f"Bot run error: {e}")
            raise

    def run_scheduler(self):
        """Run the scheduler"""
        while True:
            schedule.run_pending()
            time.sleep(1)

def main():
    tracker = MEXCTracker()
    tracker.run()

if __name__ == "__main__":
    print("Starting MEXC Futures Tracker...")
    main()