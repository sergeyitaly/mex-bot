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
        

        # Initialize Google Sheets attributes to None
        self.gs_client = None
        self.spreadsheet = None
        self.creds = None

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
        """Setup Google Sheets connection with spreadsheet discovery"""
        try:
            logger.info("🔧 Starting Google Sheets setup...")
            
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            sheet_email = os.getenv('GOOGLE_SHEET_EMAIL')
            
            logger.info(f"📝 Environment check - CREDENTIALS_JSON: {'✅ Set' if creds_json else '❌ Missing'}")
            logger.info(f"📝 Environment check - SHEET_EMAIL: {'✅ ' + sheet_email if sheet_email else '❌ Missing'}")
            
            if not creds_json:
                logger.error("❌ GOOGLE_CREDENTIALS_JSON is missing")
                return False

            if not sheet_email:
                logger.error("❌ GOOGLE_SHEET_EMAIL is missing")
                return False

            # Parse credentials
            try:
                creds_dict = json.loads(creds_json)
                service_email = creds_dict.get('client_email', 'Unknown')
                logger.info(f"✅ Credentials parsed - Service account: {service_email}")
            except Exception as e:
                logger.error(f"❌ Error parsing credentials: {e}")
                return False

            # Setup authentication
            try:
                scope = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
                self.gs_client = gspread.authorize(self.creds)
                logger.info("✅ Google Sheets client authorized successfully")
            except Exception as e:
                logger.error(f"❌ Authentication failed: {e}")
                return False

            # Discover available spreadsheets
            logger.info("🔍 Discovering available spreadsheets...")
            try:
                all_spreadsheets = self.gs_client.list_spreadsheet_files()
                logger.info(f"📋 Found {len(all_spreadsheets)} spreadsheets accessible by the service account")
                
                if not all_spreadsheets:
                    logger.error("❌ No spreadsheets found. The service account has no access to any spreadsheets.")
                    return False
                
                # Log all available spreadsheets for debugging
                logger.info("📝 Available spreadsheets:")
                for i, spreadsheet in enumerate(all_spreadsheets[:10]):  # Show first 10
                    logger.info(f"   {i+1}. '{spreadsheet['name']}' (ID: {spreadsheet['id']})")
                
                if len(all_spreadsheets) > 10:
                    logger.info(f"   ... and {len(all_spreadsheets) - 10} more")
                
                # Try to find matching spreadsheet
                matching_spreadsheets = [
                    s for s in all_spreadsheets 
                    if sheet_email.lower() in s['name'].lower()
                ]
                
                if matching_spreadsheets:
                    if len(matching_spreadsheets) > 1:
                        logger.info(f"🔍 Found {len(matching_spreadsheets)} matching spreadsheets:")
                        for match in matching_spreadsheets:
                            logger.info(f"   - '{match['name']}' (ID: {match['id']})")
                    
                    # Use the first match
                    spreadsheet_info = matching_spreadsheets[0]
                    logger.info(f"✅ Selecting spreadsheet: '{spreadsheet_info['name']}'")
                    self.spreadsheet = self.gs_client.open_by_key(spreadsheet_info['id'])
                    logger.info(f"✅ Connected to: {self.spreadsheet.title}")
                    
                else:
                    logger.error(f"❌ No spreadsheet found with name containing: '{sheet_email}'")
                    logger.info("💡 Available spreadsheet names:")
                    for s in all_spreadsheets[:5]:
                        logger.info(f"   - '{s['name']}'")
                    
                    # Let's try to create one
                    logger.info(f"🆕 Creating new spreadsheet: '{sheet_email}'")
                    try:
                        self.spreadsheet = self.gs_client.create(sheet_email)
                        logger.info(f"✅ Created new spreadsheet: {self.spreadsheet.title}")
                        
                        # Share with the service account
                        try:
                            self.spreadsheet.share(service_email, perm_type='user', role='writer')
                            logger.info(f"✅ Shared with service account: {service_email}")
                        except Exception as share_error:
                            logger.warning(f"⚠️ Could not share spreadsheet: {share_error}")
                            
                    except Exception as create_error:
                        logger.error(f"❌ Failed to create spreadsheet: {create_error}")
                        return False
                        
            except Exception as e:
                logger.error(f"❌ Error discovering spreadsheets: {e}")
                return False

            # Test connection
            try:
                worksheet = self.spreadsheet.sheet1
                logger.info("✅ Sheet connection test successful")
                return True
            except Exception as test_error:
                logger.error(f"❌ Failed to access worksheet: {test_error}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Google Sheets setup error: {e}")
            return False
        
    # ==================== PRICE MONITORING ====================



    def update_unique_futures_sheet_with_prices(self, unique_futures, analyzed_prices):
        """Update Unique Futures sheet with price information - DEBUG VERSION"""
        try:
            worksheet = self.spreadsheet.worksheet('Unique Futures')
            
            # Clear existing data
            worksheet.clear()
            
            # Enhanced headers with price changes
            headers = [
                'Symbol', 'Current Price', '5m Change %', '15m Change %', 
                '30m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated'
            ]
            worksheet.update([headers], 'A1')
            
            # Prepare data
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # DEBUG: Check what's in analyzed_prices
            debug_symbols = ['QKC_USDT', 'WIN_USDT', 'LAZIO_USDT']  # Test symbols
            logger.info("🔍 DEBUG - Checking analyzed_prices content:")
            for symbol in debug_symbols:
                price_info = next((p for p in analyzed_prices if p['symbol'] == symbol), None)
                if price_info:
                    logger.info(f"  ✅ {symbol}: ${price_info.get('price')}")
                else:
                    logger.info(f"  ❌ {symbol}: Not in analyzed_prices")
            
            # Create mapping for quick price lookup
            price_map = {item['symbol']: item for item in analyzed_prices}
            logger.info(f"🔍 Price map size: {len(price_map)} symbols")
            
            # Check specific symbol in price_map
            if 'QKC_USDT' in price_map:
                qkc_info = price_map['QKC_USDT']
                logger.info(f"🔍 QKC_USDT in price_map: ${qkc_info.get('price')}")
            else:
                logger.info("❌ QKC_USDT NOT in price_map")
            
            for symbol in sorted(unique_futures):
                price_info = price_map.get(symbol)
                changes = price_info.get('changes', {}) if price_info else {}
                price = price_info.get('price') if price_info else None
                
                # DEBUG specific symbol
                if symbol == 'QKC_USDT':
                    logger.info(f"🔍 Processing QKC_USDT - price_info: {price_info is not None}, price: {price}")
                
                # Format price display
                if price:
                    if price >= 1:
                        price_display = f"${price:.4f}"
                    elif price >= 0.01:
                        price_display = f"${price:.6f}"
                    else:
                        price_display = f"${price:.8f}"
                else:
                    price_display = 'N/A'
                    # DEBUG: Log why specific symbols are N/A
                    if symbol in ['QKC_USDT', 'WIN_USDT', 'LAZIO_USDT']:
                        logger.info(f"🔍 {symbol} marked as N/A - price: {price}, price_info: {price_info is not None}")
                
                row = [
                    symbol,
                    price_display,
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{price_info.get('score', 0):.2f}" if price_info else 'N/A',
                    'UNIQUE',
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet in batches
            if sheet_data:
                batch_size = 100
                for i in range(0, len(sheet_data), batch_size):
                    batch = sheet_data[i:i + batch_size]
                    worksheet.update(batch, f'A{i+2}')
                
                logger.info(f"✅ Updated Unique Futures with {len(sheet_data)} records")
            else:
                logger.warning("No unique futures data to update")
                
        except Exception as e:
            logger.error(f"Error updating Unique Futures sheet with prices: {e}")




    def get_all_mexc_prices(self):
        """Get price data for MEXC futures - CONSISTENT with check command"""
        try:
            # Use the EXACT SAME approach as check command
            batch_data = self.get_mexc_prices_batch_working()
            
            # Get unique futures
            unique_futures, _ = self.find_unique_futures_robust()
            
            # Apply the same matching logic as check command
            price_data = {}
            
            for symbol in unique_futures:
                # Try exact match first
                if symbol in batch_data:
                    price_data[symbol] = batch_data[symbol]
                else:
                    # Try alternative formats (same as check command)
                    alt_formats = [
                        symbol.replace('_', ''),
                        symbol.replace('_', '-'), 
                        symbol.replace('_', '/'),
                    ]
                    
                    found = False
                    for alt_format in alt_formats:
                        if alt_format in batch_data:
                            price_data[symbol] = batch_data[alt_format].copy()
                            price_data[symbol]['symbol'] = symbol  # Fix symbol name
                            found = True
                            break
                    
                    if not found:
                        # Symbol not found in batch, add with None price
                        price_data[symbol] = {
                            'symbol': symbol,
                            'price': None,
                            'changes': {},
                            'timestamp': datetime.now(),
                            'source': 'not_found'
                        }
            
            return price_data
            
        except Exception as e:
            logger.error(f"Error in get_all_mexc_prices: {e}")
            return {}
    
    def _get_price_sources_summary(self, price_data):
        """Get summary of where prices came from"""
        sources = {}
        for symbol, data in price_data.items():
            if data.get('price') is not None:
                source = data.get('source', 'unknown')
                sources[source] = sources.get(source, 0) + 1
        return sources
   
    def get_mexc_prices_batch_working(self):
        """Get prices using working MEXC API endpoint - WITH RATE LIMITING & RETRIES"""
        try:
            url = "https://contract.mexc.com/api/v1/contract/ticker"
            
            # Add retry logic
            for attempt in range(3):
                try:
                    response = requests.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('success'):
                            tickers = data.get('data', [])
                            price_data = {}
                            
                            for ticker in tickers:
                                try:
                                    symbol = ticker.get('symbol')
                                    price_str = ticker.get('lastPrice')
                                    
                                    if symbol and price_str:
                                        price = float(price_str)
                                        
                                        # FIX: ACCEPT ALL VALID PRICES, EVEN VERY SMALL ONES
                                        # Only skip negative prices
                                        if price < 0:
                                            continue
                                            
                                        change_rate = float(ticker.get('riseFallRate', 0)) * 100
                                        
                                        price_data[symbol] = {
                                            'symbol': symbol,
                                            'price': price,
                                            'changes': {
                                                '5m': change_rate,
                                                '60m': change_rate,
                                                '240m': change_rate
                                            },
                                            'timestamp': datetime.now(),
                                            'source': 'batch_ticker'
                                        }
                                except (ValueError, TypeError) as e:
                                    continue
                            
                            logger.info(f"✅ Batch prices: {len(price_data)} symbols (attempt {attempt + 1})")
                            return price_data
                    
                    # If we get here, the request failed
                    if attempt < 2:  # Don't sleep on last attempt
                        time.sleep(1)  # Wait 1 second before retry
                        
                except requests.exceptions.Timeout:
                    logger.warning(f"⚠️ Batch API timeout on attempt {attempt + 1}")
                    if attempt < 2:
                        time.sleep(2)
                except requests.exceptions.ConnectionError:
                    logger.warning(f"⚠️ Batch API connection error on attempt {attempt + 1}")
                    if attempt < 2:
                        time.sleep(2)
                except Exception as e:
                    logger.warning(f"⚠️ Batch API error on attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        time.sleep(1)
            
            return {}
            
        except Exception as e:
            logger.error(f"Batch price error: {e}")
            return {}
    
    def get_mexc_price_data_working(self, symbol):
        """Get individual price data - ACCEPT MICRO-CAP PRICES"""
        try:
            endpoints = [
            f"https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}",
            f"https://contract.mexc.com/api/v1/contract/detail?symbol={symbol}",
            f"https://futures.mexc.com/api/v1/contract/ticker?symbol={symbol}"  # Alternative domain
        ]
            
            for url in endpoints:
                try:
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success', False):
                            ticker_data = data.get('data', {})
                            
                            if isinstance(ticker_data, list) and ticker_data:
                                ticker_data = ticker_data[0]
                            
                            price_str = ticker_data.get('lastPrice') or ticker_data.get('price')
                            if price_str:
                                price = float(price_str)
                                
                                # FIX: ACCEPT ALL PRICES, EVEN MICRO-CAP
                                # Only filter out truly invalid prices (negative or None)
                                if price is None or price < 0:
                                    logger.debug(f"⚠️ Skipping {symbol} - invalid price: {price}")
                                    continue
                                    
                                change_rate = float(ticker_data.get('riseFallRate', 0)) * 100
                                
                                return {
                                    'symbol': symbol,
                                    'price': price,
                                    'changes': {
                                        '5m': change_rate,
                                        '60m': change_rate,
                                        '240m': change_rate
                                    },
                                    'timestamp': datetime.now(),
                                    'source': 'individual'
                                }
                except Exception as endpoint_error:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Individual price error for {symbol}: {e}")
            return None
    
    def get_mexc_price_data(self, symbol):
        """Main price data method - use the working version"""
        return self.get_mexc_price_data_working(symbol)    

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




    def get_consistent_price_data(self):
        """Get consistent price data with caching to avoid rate limiting issues"""
        try:
            # Use a simple cache to avoid multiple API calls in quick succession
            current_time = datetime.now()
            cache_key = "price_data_cache"
            cache_duration = 30  # seconds
            
            # Check if we have recent cached data
            if hasattr(self, '_price_data_cache') and hasattr(self, '_price_cache_time'):
                if (current_time - self._price_cache_time).seconds < cache_duration:
                    logger.info("🔄 Using cached price data")
                    return self._price_data_cache.copy()
            
            # Get fresh data from batch API
            batch_data = self.get_mexc_prices_batch_working()
            logger.info(f"📊 Fresh batch data: {len(batch_data)} symbols")
            
            # Get unique futures
            unique_futures, _ = self.find_unique_futures_robust()
            
            # Apply matching logic
            price_data = {}
            matched_symbols = 0
            
            for symbol in unique_futures:
                # Try exact match first
                if symbol in batch_data:
                    price_data[symbol] = batch_data[symbol]
                    matched_symbols += 1
                else:
                    # Try alternative formats
                    alt_formats = [
                        symbol.replace('_', ''),
                        symbol.replace('_', '-'), 
                        symbol.replace('_', '/'),
                    ]
                    
                    found = False
                    for alt_format in alt_formats:
                        if alt_format in batch_data:
                            price_data[symbol] = batch_data[alt_format].copy()
                            price_data[symbol]['symbol'] = symbol
                            matched_symbols += 1
                            found = True
                            break
                    
                    if not found:
                        price_data[symbol] = {
                            'symbol': symbol,
                            'price': None,
                            'changes': {},
                            'timestamp': current_time,
                            'source': 'not_found'
                        }
            
            # Cache the results
            self._price_data_cache = price_data.copy()
            self._price_cache_time = current_time
            
            logger.info(f"💰 Consistent price data: {matched_symbols}/{len(unique_futures)} matched")
            return price_data
            
        except Exception as e:
            logger.error(f"Consistent price data error: {e}")
            return {}


    def get_all_mexc_prices(self):
        """Get price data for MEXC futures - USE CONSISTENT APPROACH"""
        return self.get_consistent_price_data()

    def analyze_price_movements(self, price_data):
        """Analyze price movements with debugging"""
        try:
            logger.info(f"🔍 Analyzing price movements for {len(price_data)} symbols")
            
            # DEBUG: Check specific symbols
            debug_symbols = ['QKC_USDT', 'WIN_USDT', 'LAZIO_USDT']
            for symbol in debug_symbols:
                if symbol in price_data:
                    logger.info(f"  ✅ {symbol} in price_data: ${price_data[symbol].get('price')}")
                else:
                    logger.info(f"  ❌ {symbol} NOT in price_data")
            
            symbols_with_changes = []
            
            for symbol, data in price_data.items():
                changes = data.get('changes', {})
                price = data.get('price', 0)
                
                # If we have no historical changes, create minimal data
                if not changes:
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
            
            logger.info(f"✅ Analyzed {len(symbols_with_changes)} symbols")
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
        """Monitor changes in unique futures without threading"""
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
            
            logger.info(f"🔄 Changes: +{len(new_futures)}, -{len(lost_futures)}, Total: {len(current_unique_set)}")
            
            return new_futures, lost_futures
            
        except Exception as e:
            logger.error(f"Error monitoring unique futures: {e}")
            return set(), set()
            
    def format_change(self, change):
        """Format price change with color emoji"""
        if change > 0:
            return f"🟢 +{change:.2f}%"
        elif change < 0:
            return f"🔴 {change:.2f}%"
        else:
            return f"⚪ {change:.2f}%"

    # ==================== ENHANCED GOOGLE SHEETS ====================



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

    def create_and_send_excel(self, update: Update, context: CallbackContext):
        """Create and send Excel file via Telegram - FIXED to use same method as check"""
        try:
            update.message.reply_html("📊 <b>Creating comprehensive Excel report...</b>")
            
            # Get all the data needed
            all_futures_data = []
            exchanges = {
                'MEXC': self.get_mexc_futures,
                'Binance': self.get_binance_futures,
                'Bybit': self.get_bybit_futures,
                'OKX': self.get_okx_futures,
                'Gate.io': self.get_gate_futures,
                'KuCoin': self.get_kucoin_futures,
                'BingX': self.get_bingx_futures,
                'BitGet': self.get_bitget_futures
            }
            
            symbol_coverage = {}
            current_time = datetime.now().isoformat()
            
            # Collect data from all exchanges
            for name, method in exchanges.items():
                try:
                    futures = method()
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
                        
                except Exception as e:
                    logger.error(f"Error getting {name} data: {e}")
            
            # Get unique futures
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            
            # FIX: Use the EXACT SAME approach as check command
            batch_data = self.get_consistent_price_data()
            logger.info(f"📊 Excel - Batch data: {len(batch_data)} symbols")
            
            # Create price_data by matching unique symbols with batch data (SAME AS CHECK)
            price_data = {}
            matched_symbols = 0
            
            for symbol in unique_futures:
                # Try exact match first
                if symbol in batch_data:
                    price_data[symbol] = batch_data[symbol]
                    matched_symbols += 1
                else:
                    # Try alternative formats (SAME AS CHECK)
                    alt_formats = [
                        symbol.replace('_', ''),
                        symbol.replace('_', '-'), 
                        symbol.replace('_', '/'),
                    ]
                    
                    found = False
                    for alt_format in alt_formats:
                        if alt_format in batch_data:
                            price_data[symbol] = batch_data[alt_format].copy()
                            price_data[symbol]['symbol'] = symbol  # Fix symbol name
                            matched_symbols += 1
                            found = True
                            break
                    
                    if not found:
                        # Symbol not found in batch, add with None price
                        price_data[symbol] = {
                            'symbol': symbol,
                            'price': None,
                            'changes': {},
                            'timestamp': datetime.now(),
                            'source': 'not_found'
                        }
            
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # DEBUG: Log what we found
            logger.info(f"🔍 Excel - Price coverage: {matched_symbols}/{len(unique_futures)} ({matched_symbols/len(unique_futures)*100:.1f}%)")
            
            # Create Excel file
            excel_content = self.create_mexc_analysis_excel(all_futures_data, symbol_coverage, analyzed_prices)
            
            if not excel_content:
                update.message.reply_html("❌ <b>Failed to create Excel file</b>")
                return
            
            # Send file via Telegram
            filename = f"mexc_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            
            update.message.reply_document(
                document=io.BytesIO(excel_content),
                filename=filename,
                caption=(
                    f"📊 <b>MEXC Futures Analysis Report</b>\n\n"
                    f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                    f"🎯 Unique Futures: {len(unique_futures)}\n"
                    f"🏢 Exchanges: 8\n"
                    f"💰 Price Data: {matched_symbols}/{len(unique_futures)} ({matched_symbols/len(unique_futures)*100:.1f}%)\n\n"
                    f"<i>Sheets included: Dashboard, Unique Futures, All Futures, MEXC Analysis, Price Analysis, Exchange Stats</i>"
                ),
                parse_mode='HTML'
            )
            
            logger.info("✅ Excel file sent successfully")
            
        except Exception as e:
            error_msg = f"❌ <b>Error creating Excel file:</b>\n<code>{str(e)}</code>"
            update.message.reply_html(error_msg)
            logger.error(f"Excel creation error: {e}")

    def update_google_sheet_with_prices(self):
        """Update Google Sheet with price data - FIXED to use same method as check"""
        if not self.gs_client or not self.spreadsheet:
            return
        
        try:
            logger.info("🔄 Starting Google Sheet update (same as check command)...")
            
            # Get unique futures
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            logger.info(f"🎯 Unique futures for sheet: {len(unique_futures)}")
            
            # FIX: Use the EXACT SAME approach as check command
            batch_data = self.get_consistent_price_data()
            logger.info(f"📊 Google Sheets - Batch data: {len(batch_data)} symbols")
            
            # Create price_data by matching unique symbols with batch data (SAME AS CHECK)
            price_data = {}
            matched_symbols = 0
            
            for symbol in unique_futures:
                # Try exact match first
                if symbol in batch_data:
                    price_data[symbol] = batch_data[symbol]
                    matched_symbols += 1
                else:
                    # Try alternative formats (SAME AS CHECK)
                    alt_formats = [
                        symbol.replace('_', ''),
                        symbol.replace('_', '-'), 
                        symbol.replace('_', '/'),
                    ]
                    
                    found = False
                    for alt_format in alt_formats:
                        if alt_format in batch_data:
                            price_data[symbol] = batch_data[alt_format].copy()
                            price_data[symbol]['symbol'] = symbol  # Fix symbol name
                            matched_symbols += 1
                            found = True
                            break
                    
                    if not found:
                        # Symbol not found in batch, add with None price
                        price_data[symbol] = {
                            'symbol': symbol,
                            'price': None,
                            'changes': {},
                            'timestamp': datetime.now(),
                            'source': 'not_found'
                        }
            
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Calculate coverage statistics
            coverage_percent = (matched_symbols / len(unique_futures)) * 100 if unique_futures else 0
            
            logger.info(f"💰 Google Sheets - Price coverage: {matched_symbols}/{len(unique_futures)} ({coverage_percent:.1f}%)")
            
            # Update sheets
            self.update_unique_futures_sheet_with_prices(unique_futures, analyzed_prices)
            self.update_price_analysis_sheet(analyzed_prices)
            
            # Update dashboard with enhanced stats
            self.update_dashboard_with_comprehensive_stats(
                exchange_stats, 
                len(unique_futures),
                len(unique_futures), 
                analyzed_prices
            )
            
            logger.info("✅ Google Sheet updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating Google Sheet with prices: {e}")


    def create_unique_futures_sheet(self, wb, all_futures_data, symbol_coverage, analyzed_prices):
        """Create Unique Futures sheet"""
        ws = wb.create_sheet("Unique Futures")
        
        # Headers
        headers = ['Symbol', 'Current Price', '5m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Get unique futures
        unique_futures, _ = self.find_unique_futures_robust()
        
        # Create price mapping
        price_map = {item['symbol']: item for item in analyzed_prices} if analyzed_prices else {}
        
        # Add data
        row = 2
        for symbol in sorted(unique_futures):
            price_info = price_map.get(symbol, {})
            changes = price_info.get('changes', {})
            
            ws.cell(row=row, column=1).value = symbol
            ws.cell(row=row, column=2).value = price_info.get('price', 'N/A')
            ws.cell(row=row, column=3).value = self.format_change_for_excel(changes.get('5m'))
            ws.cell(row=row, column=4).value = self.format_change_for_excel(changes.get('60m'))
            ws.cell(row=row, column=5).value = self.format_change_for_excel(changes.get('240m'))
            ws.cell(row=row, column=6).value = price_info.get('score', 0)
            ws.cell(row=row, column=7).value = 'UNIQUE'
            ws.cell(row=row, column=8).value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row += 1
        
        # Adjust column widths
        for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
            ws.column_dimensions[col].width = 15

    def create_all_futures_sheet(self, wb, all_futures_data, symbol_coverage):
        """Create All Futures sheet"""
        ws = wb.create_sheet("All Futures")
        
        # Headers
        headers = ['Symbol', 'Exchange', 'Normalized', 'Available On', 'Coverage', 'Timestamp', 'Unique']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Add data
        row = 2
        for future in all_futures_data:
            normalized = self.normalize_symbol_for_comparison(future['symbol'])
            exchanges_list = symbol_coverage.get(normalized, set())
            available_on = ", ".join(sorted(exchanges_list)) if exchanges_list else "MEXC Only"
            coverage = f"{len(exchanges_list)} exchanges"
            is_unique = "✅" if len(exchanges_list) == 1 else ""
            
            ws.cell(row=row, column=1).value = future['symbol']
            ws.cell(row=row, column=2).value = future['exchange']
            ws.cell(row=row, column=3).value = normalized
            ws.cell(row=row, column=4).value = available_on
            ws.cell(row=row, column=5).value = coverage
            ws.cell(row=row, column=6).value = future['timestamp']
            ws.cell(row=row, column=7).value = is_unique
            row += 1
        
        # Adjust column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 15
        ws.column_dimensions['C'].width = 25
        ws.column_dimensions['D'].width = 40
        ws.column_dimensions['E'].width = 15
        ws.column_dimensions['F'].width = 20
        ws.column_dimensions['G'].width = 10

    def create_mexc_analysis_sheet(self, wb, all_futures_data, symbol_coverage, analyzed_prices):
        """Create MEXC Analysis sheet"""
        ws = wb.create_sheet("MEXC Analysis")
        
        # Headers
        headers = ['MEXC Symbol', 'Normalized', 'Available On', 'Exchanges Count', 'Current Price', '5m Change %', '1h Change %', '4h Change %', 'Status', 'Unique']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Get MEXC futures and price mapping
        mexc_futures = [f for f in all_futures_data if f['exchange'] == 'MEXC']
        price_map = {item['symbol']: item for item in analyzed_prices} if analyzed_prices else {}
        
        # Add data
        row = 2
        for future in mexc_futures:
            symbol = future['symbol']
            normalized = self.normalize_symbol_for_comparison(symbol)
            exchanges_list = symbol_coverage.get(normalized, set())
            available_on = ", ".join(sorted(exchanges_list)) if exchanges_list else "MEXC Only"
            exchange_count = len(exchanges_list)
            status = "Unique" if exchange_count == 1 else "Multi-exchange"
            unique_flag = "✅" if exchange_count == 1 else "🔸"
            
            price_info = price_map.get(symbol, {})
            changes = price_info.get('changes', {})
            
            ws.cell(row=row, column=1).value = symbol
            ws.cell(row=row, column=2).value = normalized
            ws.cell(row=row, column=3).value = available_on
            ws.cell(row=row, column=4).value = exchange_count
            ws.cell(row=row, column=5).value = price_info.get('price', 'N/A')
            ws.cell(row=row, column=6).value = self.format_change_for_excel(changes.get('5m'))
            ws.cell(row=row, column=7).value = self.format_change_for_excel(changes.get('60m'))
            ws.cell(row=row, column=8).value = self.format_change_for_excel(changes.get('240m'))
            ws.cell(row=row, column=9).value = status
            ws.cell(row=row, column=10).value = unique_flag
            row += 1
        
        # Adjust column widths
        for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']:
            ws.column_dimensions[col].width = 15

    def create_price_analysis_sheet(self, wb, analyzed_prices):
        """Create Price Analysis sheet"""
        ws = wb.create_sheet("Price Analysis")
        
        # Headers
        headers = ['Rank', 'Symbol', 'Current Price', '5m %', '1h %', '4h %', 'Score', 'Trend', 'Last Updated']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Add data - top performers
        row = 2
        valid_prices = [p for p in analyzed_prices if p.get('price') is not None] if analyzed_prices else []
        valid_prices.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        for i, item in enumerate(valid_prices[:50], 1):
            changes = item.get('changes', {})
            
            # Determine trend
            latest_change = item.get('latest_change', 0)
            if latest_change > 5:
                trend = "STRONG UP"
            elif latest_change > 2:
                trend = "UP"
            elif latest_change < -5:
                trend = "STRONG DOWN"
            elif latest_change < -2:
                trend = "DOWN"
            else:
                trend = "FLAT"
            
            ws.cell(row=row, column=1).value = i
            ws.cell(row=row, column=2).value = item['symbol']
            ws.cell(row=row, column=3).value = item.get('price', 'N/A')
            ws.cell(row=row, column=4).value = self.format_change_for_excel(changes.get('5m'))
            ws.cell(row=row, column=5).value = self.format_change_for_excel(changes.get('60m'))
            ws.cell(row=row, column=6).value = self.format_change_for_excel(changes.get('240m'))
            ws.cell(row=row, column=7).value = item.get('score', 0)
            ws.cell(row=row, column=8).value = trend
            ws.cell(row=row, column=9).value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row += 1
        
        # Adjust column widths
        for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']:
            ws.column_dimensions[col].width = 12

    def create_exchange_stats_sheet(self, wb, all_futures_data):
        """Create Exchange Stats sheet"""
        ws = wb.create_sheet("Exchange Stats")
        
        # Headers
        headers = ['Exchange', 'Futures Count', 'Status', 'Last Updated']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Count futures by exchange
        exchange_counts = {}
        for future in all_futures_data:
            exchange = future['exchange']
            exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
        
        # Add data
        row = 2
        for exchange in sorted(exchange_counts.keys()):
            count = exchange_counts[exchange]
            status = "WORKING" if count > 0 else "FAILED"
            
            ws.cell(row=row, column=1).value = exchange
            ws.cell(row=row, column=2).value = count
            ws.cell(row=row, column=3).value = status
            ws.cell(row=row, column=4).value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row += 1
        
        # Adjust column widths
        for col in ['A', 'B', 'C', 'D']:
            ws.column_dimensions[col].width = 20

    def format_change_for_excel(self, change):
        """Format change for Excel"""
        if change is None:
            return 'N/A'
        return f"{change:+.2f}%"



    def create_dashboard_sheet(self, wb, all_futures_data, symbol_coverage, analyzed_prices):
        """Create Dashboard sheet"""
        ws = wb.create_sheet("Dashboard")
        
        # Title
        ws['A1'] = 'MEXC FUTURES AUTO-UPDATE DASHBOARD'
        ws['A1'].font = Font(bold=True, size=14)
        
        # Get statistics
        unique_futures, exchange_stats = self.find_unique_futures_robust()
        working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
        total_exchanges = len(exchange_stats)
        
        # Statistics data
        stats_data = [
            ["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ["Update Interval", f"{self.update_interval} minutes"],
            ["", ""],
            ["EXCHANGE STATISTICS", ""],
            ["Working Exchanges", f"{working_exchanges}/{total_exchanges}"],
            ["Total Unique Symbols", len(symbol_coverage)],
            ["Unique MEXC Futures", len(unique_futures)],
            ["", ""],
            ["PRICE ANALYSIS", ""],
            ["Symbols with Price Data", f"{len(analyzed_prices) if analyzed_prices else 0}"],
            ["MEXC Futures Count", len([f for f in all_futures_data if f['exchange'] == 'MEXC'])],
            ["", ""],
            ["PERFORMANCE", ""],
            ["Next Auto-Update", (datetime.now() + timedelta(minutes=self.update_interval)).strftime('%H:%M:%S')],
            ["Status", "RUNNING"],
        ]
        
        # Add data to sheet
        for i, (label, value) in enumerate(stats_data, 2):
            ws[f'A{i}'] = label
            ws[f'B{i}'] = value
            
            # Format headers
            if label and any(keyword in label for keyword in ["STATISTICS", "ANALYSIS", "PERFORMANCE"]):
                ws[f'A{i}'].font = Font(bold=True)
                ws[f'A{i}'].fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
                ws[f'B{i}'].fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        
        # Adjust column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 25


    def create_mexc_analysis_excel(self, all_futures_data, symbol_coverage, analyzed_prices=None):
        """Create comprehensive Excel file matching Google Sheets content"""
        try:
            wb = Workbook()
            
            # Remove default sheet
            wb.remove(wb.active)
            
            # Create all sheets matching Google Sheets structure
            self.create_dashboard_sheet(wb, all_futures_data, symbol_coverage, analyzed_prices)
            self.create_unique_futures_sheet(wb, all_futures_data, symbol_coverage, analyzed_prices)
            self.create_all_futures_sheet(wb, all_futures_data, symbol_coverage)
            self.create_mexc_analysis_sheet(wb, all_futures_data, symbol_coverage, analyzed_prices)
            self.create_price_analysis_sheet(wb, analyzed_prices)
            self.create_exchange_stats_sheet(wb, all_futures_data)
            
            # Save to bytes
            output = io.BytesIO()
            wb.save(output)
            excel_content = output.getvalue()
            output.close()
            
            logger.info("✅ Excel file created successfully")
            return excel_content
            
        except Exception as e:
            logger.error(f"Error creating Excel file: {e}")
            return None

    # ==================== CORE UNIQUE FUTURES LOGIC ====================


    def normalize_symbol_for_comparison(self, symbol):
        """Robust symbol normalization - FIXED STOCK SYMBOLS"""
        if not symbol:
            return ""
        
        # Convert to uppercase
        symbol = symbol.upper()
        
        # DON'T remove stock-related suffixes - this is the main bug!
        # Keep STOCK, SHARE, etc. as they are important for stock symbols
        
        # Only remove common futures/perp suffixes
        patterns = [
            r'[-_/]?PERP(ETUAL)?$',
            r'[-_/]?SWAP$',
            r'[-_/]?FUTURES?$',
            r'[-_/]?CONTRACT$',
        ]
        
        normalized = symbol
        for pattern in patterns:
            normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
        
        # Remove separators but keep the symbol structure
        normalized = re.sub(r'[-_/]', '', normalized)
        
        # DON'T remove trailing numbers - stock symbols often have numbers
        # normalized = re.sub(r'\d+$', '', normalized)  # REMOVE THIS LINE
        
        return normalized.strip()

    def find_unique_futures_robust(self, timeout=60):
        """Find unique futures without threading to avoid thread errors"""
        try:
            logger.info("🔍 Starting unique futures search...")
            
            # Get MEXC futures
            mexc_futures = self.get_mexc_futures()
            if not mexc_futures:
                logger.error("❌ No MEXC futures found")
                return set(), {}
            
            logger.info(f"📊 MEXC futures to check: {len(mexc_futures)}")
            
            # Get futures from other exchanges
            all_other_futures, exchange_stats = self.get_all_exchanges_futures()
            logger.info(f"📊 Other exchanges futures: {len(all_other_futures)}")
            
            # Find unique futures (futures that are ONLY on MEXC)
            unique_futures = set()
            
            # Normalize all other futures for comparison
            logger.info("🔄 Normalizing symbols for comparison...")
            normalized_other_futures = set()
            for symbol in all_other_futures:
                try:
                    normalized = self.normalize_symbol_for_comparison(symbol)
                    if normalized:
                        normalized_other_futures.add(normalized)
                except Exception as e:
                    logger.debug(f"Could not normalize {symbol}: {e}")
            
            logger.info(f"📊 Normalized other futures: {len(normalized_other_futures)}")
            
            # Check each MEXC future against normalized other futures
            checked_count = 0
            for mexc_symbol in mexc_futures:
                try:
                    if checked_count % 100 == 0:
                        logger.info(f"🔍 Checked {checked_count}/{len(mexc_futures)} symbols...")
                    
                    normalized_mexc = self.normalize_symbol_for_comparison(mexc_symbol)
                    if normalized_mexc and normalized_mexc not in normalized_other_futures:
                        unique_futures.add(mexc_symbol)
                    
                    checked_count += 1
                    
                except Exception as e:
                    logger.error(f"Error checking {mexc_symbol}: {e}")
                    continue
            
            logger.info(f"🎯 Found {len(unique_futures)} unique futures")
            return unique_futures, exchange_stats
            
        except Exception as e:
            logger.error(f"❌ Unique futures search error: {e}")
            return set(), {}
        
    def send_new_unique_notification(self, new_futures, all_unique):
        """Send notification about new unique futures - PROPER MICRO-CAP FORMATTING"""
        try:
            display_futures = list(new_futures)[:10]
            
            message = "🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
            
            # Get ALL prices
            all_price_data = self.get_all_mexc_prices()
            
            valid_count = 0
            for symbol in display_futures:
                price_info = all_price_data.get(symbol)
                
                if price_info and price_info.get('price') is not None and price_info['price'] >= 0:
                    # VALID PRICE (including micro-cap)
                    changes = price_info.get('changes', {})
                    change_5m = changes.get('5m', 0)
                    change_1h = changes.get('60m', 0)
                    price = price_info['price']
                    
                    message += f"✅ <b>{symbol}</b>\n"
                    
                    # IMPROVED PRICE FORMATTING FOR ALL RANGES
                    if price >= 1000:
                        message += f"   Price: ${price:,.2f}\n"
                    elif price >= 1:
                        message += f"   Price: ${price:.2f}\n"
                    elif price >= 0.1:
                        message += f"   Price: ${price:.4f}\n"
                    elif price >= 0.001:
                        message += f"   Price: ${price:.6f}\n"
                    elif price >= 0.000001:
                        message += f"   Price: ${price:.8f}\n"
                    else:
                        # For extremely small prices (like 3e-08)
                        message += f"   Price: ${price:.2e}\n"
                    
                    message += f"   5m: {self.format_change(change_5m)}\n"
                    
                    # Only show 1h if it's different from 5m
                    if change_1h != change_5m:
                        message += f"   1h: {self.format_change(change_1h)}\n"
                    
                    message += "\n"
                    valid_count += 1
                    
                else:
                    # TRULY MISSING PRICE
                    message += f"✅ <b>{symbol}</b> (price data unavailable)\n\n"
            
            if len(new_futures) > len(display_futures):
                message += f"... and {len(new_futures) - len(display_futures)} more symbols\n\n"
            
            message += f"📊 Total unique: <b>{len(all_unique)}</b>"
            message += f"\n💰 With prices: <b>{valid_count}/{len(display_futures)}</b> shown symbols"
            
            self.send_broadcast_message(message)
            
        except Exception as e:
            logger.error(f"Error sending new unique notification: {e}")


    def send_lost_unique_notification(self, lost_futures, remaining_unique):
        """Send notification about lost unique futures - OPTIMIZED"""
        try:
            # Limit the number of symbols to process
            display_futures = list(lost_futures)[:10]  # Show max 10 symbols
            
            message = "📉 <b>FUTURES NO LONGER UNIQUE:</b>\n\n"
            
            for symbol in display_futures:
                # Use fast verification with cached data
                try:
                    # For lost futures, we know they were previously unique
                    # Just show they're no longer unique without detailed coverage check
                    message += f"❌ <b>{symbol}</b>\n"
                    message += f"   No longer exclusive to MEXC\n\n"
                except Exception as e:
                    logger.error(f"Error checking coverage for {symbol}: {e}")
                    message += f"❌ <b>{symbol}</b> (verification failed)\n\n"
            
            if len(lost_futures) > len(display_futures):
                message += f"... and {len(lost_futures) - len(display_futures)} more symbols\n\n"
            
            message += f"📊 Remaining unique: <b>{len(remaining_unique)}</b>"
            
            self.send_broadcast_message(message)
            
        except Exception as e:
            logger.error(f"Error sending lost unique notification: {e}")

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

    def verify_symbol_coverage(self, symbol, all_futures_cache=None, mexc_futures_cache=None):
        """FAST symbol coverage check using cached data - FIXED"""
        coverage = []
        
        # Use cached data if provided
        if mexc_futures_cache is not None and symbol in mexc_futures_cache:
            coverage.append('MEXC')
        
        # Check other exchanges using cached data
        # FIX: Use LESS aggressive normalization for stock symbols
        normalized_target = self.normalize_symbol_for_comparison(symbol)
        
        # For stock symbols, be more careful with normalization
        if 'STOCK' in symbol.upper():
            # For stock symbols, try multiple normalization approaches
            normalized_variations = [
                normalized_target,
                symbol.upper().replace('_', '').replace('-', '').replace('/', ''),
                symbol.upper().replace('STOCK', '').replace('_', '').replace('-', '').replace('/', ''),
            ]
        else:
            normalized_variations = [normalized_target]
        
        exchange_checks = {
            'Binance': all_futures_cache,
            'Bybit': all_futures_cache, 
            'OKX': all_futures_cache,
            'Gate.io': all_futures_cache,
            'KuCoin': all_futures_cache,
            'BingX': all_futures_cache,
            'BitGet': all_futures_cache
        }
        
        for exchange_name, futures_cache in exchange_checks.items():
            if futures_cache is None:
                continue
                
            found = False
            for fut in futures_cache:
                # Try all normalization variations
                for normalized_variation in normalized_variations:
                    normalized_fut = self.normalize_symbol_for_comparison(fut)
                    if normalized_variation == normalized_fut:
                        found = True
                        break
                if found:
                    break
            
            if found:
                coverage.append(exchange_name)
        
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

    def excel_command(self, update: Update, context: CallbackContext):
        """Download comprehensive Excel report"""
        self.create_and_send_excel(update, context)

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
        self.dispatcher.add_handler(CommandHandler("excel", self.excel_command))
        self.dispatcher.add_handler(CommandHandler("download", self.excel_command))
        self.dispatcher.add_handler(CommandHandler("pricedebug", self.price_debug_command))
        self.dispatcher.add_handler(CommandHandler("symboldebug", self.symbol_debug_command))
        self.dispatcher.add_handler(CommandHandler("dataflow", self.data_flow_debug_command))
        self.dispatcher.add_handler(CommandHandler("qkctest", self.qkc_test_command))
        self.dispatcher.add_handler(CommandHandler("validateprices", self.validate_prices_command))
        self.dispatcher.add_handler(CommandHandler("symbolsearch", self.symbol_search_command))
        self.dispatcher.add_handler(CommandHandler("debugdatasources", self.debug_data_sources))


    def symbol_search_command(self, update: Update, context: CallbackContext):
        """Search for symbols in MEXC API - CORRECTED"""
        try:
            if not context.args:
                update.message.reply_html("Usage: /symbolsearch SYMBOL\nExample: /symbolsearch BOBBSC")
                return
            
            search_term = context.args[0].upper()
            update.message.reply_html(f"🔍 <b>Searching for:</b> {search_term}")
            
            # Get batch data to see what's available
            batch_data = self.get_mexc_prices_batch_working()
            
            # Search for matching symbols
            matching_symbols = [s for s in batch_data.keys() if search_term in s]
            
            # Get MEXC futures to see what should be there
            mexc_futures = self.get_mexc_futures()
            mexc_matches = [s for s in mexc_futures if search_term in s]
            
            message = (
                f"🔍 <b>Symbol Search: {search_term}</b>\n\n"
                f"📊 <b>MEXC Futures List:</b> {len(mexc_matches)} matches\n"
            )
            
            if mexc_matches:
                message += "\n".join([f"• {s}" for s in mexc_matches[:10]]) + "\n"
            else:
                message += "• No matches found\n"
            
            message += f"\n📊 <b>Batch API Data:</b> {len(matching_symbols)} matches\n"
            
            if matching_symbols:
                for symbol in matching_symbols[:10]:
                    price = batch_data[symbol].get('price')
                    message += f"• {symbol}: ${price}\n"
            else:
                message += "• No matches in batch API\n"
            
            # FIXED: Create proper test symbols based on input
            message += f"\n🔧 <b>Direct API Tests:</b>\n"
            
            # If search_term already has _USDT, test it directly
            if search_term.endswith('_USDT'):
                test_symbols = [search_term]  # Just test the exact symbol
            else:
                # If it's a base symbol, add _USDT suffix
                test_symbols = [f"{search_term}_USDT"]
            
            for test_symbol in test_symbols:
                try:
                    url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={test_symbol}"
                    response = requests.get(url, timeout=5)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success') and data.get('data'):
                            # Handle both list and dict response formats
                            ticker_data = data['data']
                            if isinstance(ticker_data, list) and ticker_data:
                                price = ticker_data[0].get('lastPrice')
                            else:
                                price = ticker_data.get('lastPrice')
                                
                            if price:
                                message += f"• {test_symbol}: ✅ FOUND (${price})\n"
                            else:
                                message += f"• {test_symbol}: ✅ FOUND but no price data\n"
                        else:
                            message += f"• {test_symbol}: ❌ API returned success=False\n"
                    else:
                        message += f"• {test_symbol}: ❌ HTTP {response.status_code}\n"
                except Exception as e:
                    message += f"• {test_symbol}: ❌ ERROR: {str(e)[:50]}...\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Search error: {str(e)}")
            

    def debug_data_sources(self, update: Update, context: CallbackContext):
        """Debug where data is coming from"""
        try:
            update.message.reply_html("🔍 <b>Debugging data sources...</b>")
            
            # Source 1: MEXC futures list
            mexc_futures = self.get_mexc_futures()
            
            # Source 2: Batch price data
            batch_data = self.get_mexc_prices_batch_working()
            
            # Source 3: Unique futures
            unique_futures, _ = self.find_unique_futures_robust()
            
            # Test symbols
            test_symbols = ['BOBBSC_USDT', 'MANYU_USDT', 'RVV_USDT', 'AAPLSTOCK_USDT', 'LAZIO_USDT']
            
            message = "🔍 <b>Data Source Analysis</b>\n\n"
            
            for symbol in test_symbols:
                message += f"<b>{symbol}</b>\n"
                message += f"• MEXC Futures: {'✅' if symbol in mexc_futures else '❌'}\n"
                message += f"• Batch Prices: {'✅' if symbol in batch_data else '❌'}\n"
                message += f"• Unique Futures: {'✅' if symbol in unique_futures else '❌'}\n"
                message += "\n"
            
            # Check if there are symbol format differences
            message += "<b>🔍 Symbol Format Analysis</b>\n"
            batch_symbols_sample = list(batch_data.keys())[:5]
            message += f"Batch API symbols sample: {batch_symbols_sample}\n\n"
            
            mexc_futures_sample = list(mexc_futures)[:5]
            message += f"MEXC Futures sample: {mexc_futures_sample}\n\n"
            
            unique_sample = list(unique_futures)[:5]
            message += f"Unique Futures sample: {unique_sample}"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Debug error: {str(e)}")

    def validate_prices_command(self, update: Update, context: CallbackContext):
        """Validate prices for symbols with issues"""
        try:
            update.message.reply_html("🔍 <b>Validating price data quality...</b>")
            
            # Test problematic symbols
            test_symbols = [
                'BOBBSC_USDT',  # Shows $0.000000
                'MANYU_USDT',   # Shows $0.000000  
                'RVV_USDT',     # Shows unavailable
                'AAPLSTOCK_USDT', # Shows valid price
                'LAZIO_USDT'    # Shows valid price
            ]
            
            results = []
            all_price_data = self.get_all_mexc_prices()
            
            for symbol in test_symbols:
                price_info = all_price_data.get(symbol)
                
                if not price_info:
                    results.append(f"❌ {symbol}: NOT IN PRICE DATA")
                    continue
                    
                price = price_info.get('price')
                source = price_info.get('source', 'unknown')
                
                if price is None:
                    results.append(f"❌ {symbol}: PRICE IS NONE")
                elif price <= 0:
                    results.append(f"⚠️ {symbol}: ZERO/INVALID PRICE (${price}) - {source}")
                else:
                    results.append(f"✅ {symbol}: VALID ${price:.6f} - {source}")
            
            message = "🔍 <b>Price Validation Results</b>\n\n" + "\n".join(results)
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Validation error: {str(e)}")
            

    def qkc_test_command(self, update: Update, context: CallbackContext):
        """Test QKC_USDT specifically"""
        try:
            update.message.reply_html("🔍 <b>Testing QKC_USDT data flow...</b>")
            
            # Test each step
            batch_data = self.get_mexc_prices_batch_working()
            unique_futures, _ = self.find_unique_futures_robust()
            all_prices = self.get_all_mexc_prices()
            
            message = (
                f"🔍 <b>QKC_USDT Test Results</b>\n\n"
                f"📊 <b>Batch API:</b> {'✅ PRESENT' if 'QKC_USDT' in batch_data else '❌ MISSING'}\n"
                f"🎯 <b>Unique Futures:</b> {'✅ PRESENT' if 'QKC_USDT' in unique_futures else '❌ MISSING'}\n"
                f"💰 <b>Final Prices:</b> {'✅ PRESENT' if 'QKC_USDT' in all_prices else '❌ MISSING'}\n\n"
            )
            
            if 'QKC_USDT' in batch_data:
                message += f"• Batch Price: ${batch_data['QKC_USDT'].get('price')}\n"
            if 'QKC_USDT' in all_prices:
                message += f"• Final Price: ${all_prices['QKC_USDT'].get('price')}\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Test error: {str(e)}")


    def data_flow_debug_command(self, update: Update, context: CallbackContext):
        """Debug the complete data flow for a symbol"""
        try:
            if not context.args:
                update.message.reply_html("Usage: /dataflow SYMBOL\nExample: /dataflow QKC_USDT")
                return
            
            symbol = context.args[0].upper()
            update.message.reply_html(f"🔍 <b>Data Flow Debug:</b> {symbol}")
            
            # Step 1: Check batch API
            batch_data = self.get_mexc_prices_batch_working()
            in_batch = symbol in batch_data
            batch_price = batch_data.get(symbol, {}).get('price') if in_batch else None
            
            # Step 2: Check individual API
            individual_data = self.get_mexc_price_data_working(symbol)
            individual_works = individual_data is not None
            individual_price = individual_data.get('price') if individual_data else None
            
            # Step 3: Check get_all_mexc_prices
            all_prices = self.get_all_mexc_prices()
            in_all_prices = symbol in all_prices
            all_price_value = all_prices.get(symbol, {}).get('price') if in_all_prices else None
            
            # Step 4: Check analyzed_prices
            analyzed_prices = self.analyze_price_movements(all_prices)
            in_analyzed = any(p['symbol'] == symbol for p in analyzed_prices)
            analyzed_info = next((p for p in analyzed_prices if p['symbol'] == symbol), None)
            analyzed_price = analyzed_info.get('price') if analyzed_info else None
            
            message = (
                f"🔍 <b>Data Flow Debug: {symbol}</b>\n\n"
                f"📊 <b>Step 1 - Batch API:</b>\n"
                f"• Present: {'✅ YES' if in_batch else '❌ NO'}\n"
                f"• Price: ${batch_price if batch_price else 'N/A'}\n\n"
                
                f"📊 <b>Step 2 - Individual API:</b>\n"
                f"• Works: {'✅ YES' if individual_works else '❌ NO'}\n"
                f"• Price: ${individual_price if individual_price else 'N/A'}\n\n"
                
                f"📊 <b>Step 3 - get_all_mexc_prices:</b>\n"
                f"• Present: {'✅ YES' if in_all_prices else '❌ NO'}\n"
                f"• Price: ${all_price_value if all_price_value else 'N/A'}\n\n"
                
                f"📊 <b>Step 4 - analyze_price_movements:</b>\n"
                f"• Present: {'✅ YES' if in_analyzed else '❌ NO'}\n"
                f"• Price: ${analyzed_price if analyzed_price else 'N/A'}\n\n"
            )
            
            # Identify where the data is lost
            if in_all_prices and not in_analyzed:
                message += "❌ <b>ISSUE:</b> Data lost in analyze_price_movements()\n"
            elif in_analyzed and analyzed_price is None:
                message += "❌ <b>ISSUE:</b> Price is None in analyzed data\n"
            else:
                message += "✅ <b>DATA FLOW:</b> All steps working\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Data flow debug error: {str(e)}")



    def symbol_debug_command(self, update: Update, context: CallbackContext):
        """Debug why specific symbols don't have price data"""
        try:
            if not context.args:
                update.message.reply_html("Usage: /symboldebug SYMBOL\nExample: /symboldebug DEVVE_USDT")
                return
            
            symbol = context.args[0].upper()
            update.message.reply_html(f"🔧 <b>Debugging symbol:</b> {symbol}")
            
            # Get unique symbols
            unique_futures, _ = self.find_unique_futures_robust()
            
            # Check if symbol is unique
            is_unique = symbol in unique_futures
            batch_data = self.get_mexc_prices_batch_working()
            in_batch = symbol in batch_data
            
            # Try individual price fetch
            individual_price = self.get_mexc_price_data_working(symbol)
            
            # Try alternative formats
            alt_formats = self.get_alternative_symbol_formats(symbol)
            alt_matches = []
            for alt in alt_formats:
                if alt in batch_data:
                    alt_matches.append(f"{alt} (price: ${batch_data[alt].get('price', 'N/A')})")
            
            message = (
                f"🔧 <b>Symbol Debug: {symbol}</b>\n\n"
                f"📊 <b>Status:</b>\n"
                f"• In unique list: {'✅ YES' if is_unique else '❌ NO'}\n"
                f"• In batch API: {'✅ YES' if in_batch else '❌ NO'}\n"
                f"• Individual API: {'✅ WORKING' if individual_price else '❌ FAILED'}\n\n"
            )
            
            if individual_price:
                message += f"💰 <b>Price Data:</b>\n"
                message += f"• Price: ${individual_price['price']}\n"
                message += f"• 5m Change: {individual_price['changes'].get('5m', 0):.2f}%\n"
                message += f"• Source: {individual_price.get('source', 'unknown')}\n\n"
            
            if alt_matches:
                message += f"🔄 <b>Alternative format matches:</b>\n"
                message += "\n".join([f"• {match}" for match in alt_matches[:3]])
            else:
                message += f"🔄 <b>Alternative formats:</b> No matches found\n"
                message += f"   Tried: {', '.join(alt_formats[:3])}\n"
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Debug error: {str(e)}")
            

    def price_debug_command(self, update: Update, context: CallbackContext):
        """Debug price fetching issues"""
        update.message.reply_html("🔧 <b>Debugging price data...</b>")
        
        try:
            # Test with a few known symbols
            test_symbols = [
                'BTC_USDT', 'ETH_USDT', 'ADA_USDT',  # Common symbols that should work
                'WIN_USDT', 'LAZIO_USDT', 'BOOST_USDT',  # Your unique symbols
                'FARM_USDT', 'ZKSYNC_USDT'  # Symbols that showed prices before
            ]
            
            results = []
            successful = 0
            
            for symbol in test_symbols:
                try:
                    price_info = self.get_mexc_price_data(symbol)
                    if price_info and price_info.get('price'):
                        results.append(f"✅ {symbol}: ${price_info['price']}")
                        successful += 1
                    else:
                        results.append(f"❌ {symbol}: No price data")
                    
                    time.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    results.append(f"❌ {symbol}: Error - {str(e)}")
            
            # Test batch method
            batch_data = self.get_mexc_prices_batch()
            batch_count = len(batch_data)
            
            message = (
                f"🔧 <b>Price Debug Results</b>\n\n"
                f"Individual API: {successful}/{len(test_symbols)} successful\n"
                f"Batch API: {batch_count} symbols found\n\n"
                f"<b>Detailed Results:</b>\n" + "\n".join(results) +
                f"\n\n<b>Batch API Status:</b> {'✅ WORKING' if batch_count > 0 else '❌ FAILED'}"
            )
            
            update.message.reply_html(message)
            
        except Exception as e:
            update.message.reply_html(f"❌ Debug error: {str(e)}")



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
                'Bybit': self.get_bybit_futures,
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
        """Update Unique Futures sheet with price information - DEBUG VERSION"""
        try:
            worksheet = self.spreadsheet.worksheet('Unique Futures')
            
            # Clear existing data
            worksheet.clear()
            
            # Enhanced headers with price changes
            headers = [
                'Symbol', 'Current Price', '5m Change %', '15m Change %', 
                '30m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated'
            ]
            worksheet.update([headers], 'A1')  # CORRECT: values first, range second
            
            # Prepare data
            sheet_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # DEBUG: Check what's in analyzed_prices
            debug_symbols = ['QKC_USDT', 'WIN_USDT', 'LAZIO_USDT']  # Test symbols
            logger.info("🔍 DEBUG - Checking analyzed_prices content:")
            for symbol in debug_symbols:
                price_info = next((p for p in analyzed_prices if p['symbol'] == symbol), None)
                if price_info:
                    logger.info(f"  ✅ {symbol}: ${price_info.get('price')}")
                else:
                    logger.info(f"  ❌ {symbol}: Not in analyzed_prices")
            
            # Create mapping for quick price lookup
            price_map = {item['symbol']: item for item in analyzed_prices}
            logger.info(f"🔍 Price map size: {len(price_map)} symbols")
            
            # Check specific symbol in price_map
            if 'QKC_USDT' in price_map:
                qkc_info = price_map['QKC_USDT']
                logger.info(f"🔍 QKC_USDT in price_map: ${qkc_info.get('price')}")
            else:
                logger.info("❌ QKC_USDT NOT in price_map")
            
            for symbol in sorted(unique_futures):
                price_info = price_map.get(symbol)
                changes = price_info.get('changes', {}) if price_info else {}
                price = price_info.get('price') if price_info else None
                
                # DEBUG specific symbol
                if symbol == 'QKC_USDT':
                    logger.info(f"🔍 Processing QKC_USDT - price_info: {price_info is not None}, price: {price}")
                
                # Format price display
                if price:
                    if price >= 1:
                        price_display = f"${price:.4f}"
                    elif price >= 0.01:
                        price_display = f"${price:.6f}"
                    else:
                        price_display = f"${price:.8f}"
                else:
                    price_display = 'N/A'
                    # DEBUG: Log why specific symbols are N/A
                    if symbol in ['QKC_USDT', 'WIN_USDT', 'LAZIO_USDT']:
                        logger.info(f"🔍 {symbol} marked as N/A - price: {price}, price_info: {price_info is not None}")
                
                row = [
                    symbol,
                    price_display,
                    self.format_change_for_sheet(changes.get('5m')),
                    self.format_change_for_sheet(changes.get('15m')),
                    self.format_change_for_sheet(changes.get('30m')),
                    self.format_change_for_sheet(changes.get('60m')),
                    self.format_change_for_sheet(changes.get('240m')),
                    f"{price_info.get('score', 0):.2f}" if price_info else 'N/A',
                    'UNIQUE',
                    current_time
                ]
                sheet_data.append(row)
            
            # Update sheet in batches - CORRECTED PARAMETER ORDER
            if sheet_data:
                batch_size = 100
                for i in range(0, len(sheet_data), batch_size):
                    batch = sheet_data[i:i + batch_size]
                    worksheet.update(batch, f'A{i+2}')  # CORRECT: values first, range second
                
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

    def get_prices_for_unique_symbols(self):
        """Get price data specifically for all unique symbols"""
        try:
            # Get unique symbols
            unique_futures, _ = self.find_unique_futures_robust()
            logger.info(f"🎯 Getting prices for {len(unique_futures)} unique symbols")
            
            # Start with batch data
            batch_data = self.get_mexc_prices_batch_working()
            price_data = batch_data.copy()
            
            # Check which unique symbols are missing from batch
            missing_symbols = [s for s in unique_futures if s not in batch_data]
            logger.info(f"🔍 {len(missing_symbols)} unique symbols missing from batch API")
            
            # Try alternative symbol formats for missing symbols
            found_with_alt_format = 0
            for symbol in missing_symbols[:]:
                # Try different symbol formats that might be in batch data
                alt_formats = self.get_alternative_symbol_formats(symbol)
                for alt_format in alt_formats:
                    if alt_format in batch_data:
                        price_data[symbol] = batch_data[alt_format]
                        missing_symbols.remove(symbol)
                        found_with_alt_format += 1
                        logger.debug(f"✅ Found {symbol} as {alt_format} in batch")
                        break
            
            if found_with_alt_format > 0:
                logger.info(f"🔄 Found {found_with_alt_format} symbols with alternative formats")
            
            # Get individual prices for remaining missing symbols
            successful_individual = 0
            logger.info(f"🔍 Getting individual prices for {len(missing_symbols)} remaining symbols")
            
            for symbol in missing_symbols[:80]:  # Limit to avoid timeout
                try:
                    price_info = self.get_mexc_price_data_working(symbol)
                    if price_info and price_info.get('price'):
                        price_data[symbol] = price_info
                        successful_individual += 1
                    
                    time.sleep(0.2)  # Conservative rate limiting
                    
                except Exception as e:
                    logger.debug(f"Individual price failed for {symbol}: {e}")
                    continue
            
            logger.info(f"✅ Unique symbols price coverage: {len([s for s in unique_futures if s in price_data])}/{len(unique_futures)}")
            return price_data
            
        except Exception as e:
            logger.error(f"Error getting prices for unique symbols: {e}")
            return {}

    def get_alternative_symbol_formats(self, symbol):
        """Generate alternative symbol formats that might match batch data"""
        alternatives = []
        
        # Common format variations
        if '_' in symbol:
            # Try without underscore
            alternatives.append(symbol.replace('_', ''))
            # Try with different case
            alternatives.append(symbol.lower())
            alternatives.append(symbol.upper())
        
        # Try common suffix variations
        if symbol.endswith('_USDT'):
            base = symbol[:-5]  # Remove _USDT
            alternatives.append(f"{base}USDT")
            alternatives.append(f"{base}-USDT")
            alternatives.append(f"{base}/USDT")
        
        elif symbol.endswith('_USDC'):
            base = symbol[:-5]  # Remove _USDC
            alternatives.append(f"{base}USDC")
            alternatives.append(f"{base}-USDC")
            alternatives.append(f"{base}/USDC")
        
        return alternatives





    def update_dashboard_with_comprehensive_stats(self, exchange_stats, unique_symbols_count, unique_futures_count, analyzed_prices):
        """Update the dashboard with comprehensive statistics including price coverage"""
        if not self.spreadsheet:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet("Dashboard")
            
            # Count working exchanges
            working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
            total_exchanges = len(exchange_stats)
            
            # Calculate price statistics
            valid_prices = [p for p in analyzed_prices if p.get('price') is not None] if analyzed_prices else []
            top_performers = valid_prices[:10] if valid_prices else []
            strong_movers = [p for p in valid_prices if abs(p.get('latest_change', 0)) > 5]
            
            # Get unique futures for price coverage calculation
            unique_futures, _ = self.find_unique_futures_robust()
            price_coverage = len(valid_prices) / max(len(unique_futures), 1) * 100
            
            stats_update = [
                ["🤖 MEXC FUTURES AUTO-UPDATE DASHBOARD", ""],
                ["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Update Interval", f"{self.update_interval} minutes"],
                ["", ""],
                ["📊 EXCHANGE STATISTICS", ""],
                ["Working Exchanges", f"{working_exchanges}/{total_exchanges}"],
                ["Total Unique Symbols", unique_symbols_count],
                ["Unique MEXC Futures", f"{unique_futures_count} 🎯"],
                ["", ""],
                ["💰 PRICE ANALYSIS", ""],
                ["Symbols with Price Data", f"{len(valid_prices)}/{unique_futures_count}"],
                ["Price Data Coverage", f"{price_coverage:.1f}%"],
                ["Top Performers Tracked", len(top_performers)],
                ["Strong Movers (>5%)", len(strong_movers)],
                ["", ""],
                ["⚡ PERFORMANCE", ""],
                ["Next Auto-Update", (datetime.now() + timedelta(minutes=self.update_interval)).strftime('%H:%M:%S')],
                ["Status", "🟢 RUNNING"],
                ["Price System", "✅ WORKING"],
            ]
            
            # Update dashboard
            worksheet.clear()
            worksheet.update(stats_update, 'A1')  # Fixed parameter order
            
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
        """Ensure all required sheets exist with comprehensive error handling"""
        try:
            # First, check if Google Sheets is properly configured
            if not self.gs_client or not self.spreadsheet:
                logger.error("❌ Google Sheets not properly configured")
                # Try to reinitialize
                if not self.setup_google_sheets():
                    logger.error("❌ Failed to reinitialize Google Sheets")
                    return False
            
            # Test connection by getting spreadsheet info
            try:
                spreadsheet_title = self.spreadsheet.title
                logger.info(f"📊 Working with spreadsheet: {spreadsheet_title}")
            except Exception as e:
                logger.error(f"❌ Cannot access spreadsheet: {e}")
                return False

            # Define required sheets
            required_sheets = [
                'Dashboard',
                'Unique Futures', 
                'All Futures',
                'MEXC Analysis',
                'Price Analysis',
                'Exchange Stats'
            ]

            # Get existing sheets
            try:
                existing_worksheets = self.spreadsheet.worksheets()
                existing_sheet_names = [sheet.title for sheet in existing_worksheets]
                logger.info(f"📋 Found {len(existing_worksheets)} existing sheets: {existing_sheet_names}")
            except Exception as e:
                logger.error(f"❌ Failed to get existing sheets: {e}")
                return False

            # Create missing sheets
            sheets_created = 0
            for sheet_name in required_sheets:
                try:
                    if sheet_name in existing_sheet_names:
                        logger.info(f"✅ Sheet exists: {sheet_name}")
                        continue
                    
                    # Create new sheet
                    logger.info(f"🆕 Creating new sheet: {sheet_name}")
                    new_sheet = self.spreadsheet.add_worksheet(
                        title=sheet_name, 
                        rows="1000", 
                        cols="20"
                    )
                    
                    # Set basic headers based on sheet type
                    headers = self.get_sheet_headers(sheet_name)
                    if headers:
                        new_sheet.update('A1', [headers])
                        logger.info(f"✅ Added headers to {sheet_name}")
                    
                    sheets_created += 1
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create sheet {sheet_name}: {e}")
                    continue

            # Setup Dashboard content
            try:
                dashboard = self.spreadsheet.worksheet('Dashboard')
                self.setup_dashboard_sheet(dashboard)
                logger.info("✅ Dashboard setup completed")
            except Exception as e:
                logger.error(f"❌ Failed to setup dashboard: {e}")

            logger.info(f"✅ Sheet initialization complete. Created {sheets_created} new sheets.")
            return True
            
        except Exception as e:
            logger.error(f"❌ Critical error in sheet initialization: {e}")
            return False

    def get_sheet_headers(self, sheet_name):
        """Get appropriate headers for each sheet type"""
        headers_map = {
            'Dashboard': ['Section', 'Value'],
            'Unique Futures': [
                'Symbol', 'Current Price', '5m Change %', '15m Change %', 
                '30m Change %', '1h Change %', '4h Change %', 'Score', 'Status', 'Last Updated'
            ],
            'All Futures': [
                'Symbol', 'Exchange', 'Normalized', 'Available On', 
                'Coverage', 'Timestamp', 'Unique', 'Current Price'
            ],
            'MEXC Analysis': [
                'MEXC Symbol', 'Normalized', 'Available On', 'Exchanges Count', 
                'Current Price', '5m Change %', '1h Change %', '4h Change %', 
                'Status', 'Unique', 'Timestamp'
            ],
            'Price Analysis': [
                'Rank', 'Symbol', 'Current Price', '5m %', '15m %', '30m %', 
                '1h %', '4h %', 'Score', 'Trend', 'Volume', 'Last Updated'
            ],
            'Exchange Stats': [
                'Exchange', 'Futures Count', 'Status', 'Last Updated', 
                'Success Rate', 'Price Data Available'
            ]
        }
        return headers_map.get(sheet_name, [])




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
            
            # Apply formatting
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
                    
            except Exception as format_error:
                logger.warning(f"⚠️ Could not format dashboard: {format_error}")
            
            logger.info("✅ Dashboard content updated")
            
        except Exception as e:
            logger.error(f"❌ Error setting up dashboard: {e}")
            raise


    def update_google_sheet_dashboard(self):
        """Update Google Sheet dashboard with current statistics"""
        if not self.gs_client or not self.spreadsheet:
            logger.warning("Google Sheets not available for dashboard update")
            return
        
        try:
            # Get current data
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            
            # Get all futures data for statistics
            all_futures_data = []
            exchanges = {
                'MEXC': self.get_mexc_futures,
                'Binance': self.get_binance_futures,
                'Bybit': self.get_bybit_futures,
                'OKX': self.get_okx_futures,
                'Gate.io': self.get_gate_futures,
                'KuCoin': self.get_kucoin_futures,
                'BingX': self.get_bingx_futures,
                'BitGet': self.get_bitget_futures
            }
            
            symbol_coverage = {}
            for name, method in exchanges.items():
                try:
                    futures = method()
                    for symbol in futures:
                        all_futures_data.append({
                            'symbol': symbol,
                            'exchange': name,
                            'timestamp': datetime.now().isoformat()
                        })
                        
                        # Track symbol coverage
                        normalized = self.normalize_symbol_for_comparison(symbol)
                        if normalized not in symbol_coverage:
                            symbol_coverage[normalized] = set()
                        symbol_coverage[normalized].add(name)
                except Exception as e:
                    logger.error(f"Error getting {name} data: {e}")
            
            # Get price data
            price_data = self.get_all_mexc_prices()
            analyzed_prices = self.analyze_price_movements(price_data)
            
            # Update dashboard
            self.update_dashboard_with_comprehensive_stats(exchange_stats, len(symbol_coverage), len(unique_futures), analyzed_prices)
            
            logger.info("✅ Google Sheet dashboard updated")
            
        except Exception as e:
            logger.error(f"Error updating Google Sheet dashboard: {e}")
            

    def force_update_command(self, update: Update, context: CallbackContext):
        """Force immediate Google Sheet update with comprehensive data"""
        try:
            update.message.reply_html("🔄 <b>Force updating Google Sheet with comprehensive data...</b>")
            
            # Step 1: Initialize Google Sheets connection
            update.message.reply_html("🔧 <b>Step 1:</b> Initializing Google Sheets connection...")
            
            if not self.gs_client or not self.spreadsheet:
                update.message.reply_html("🔄 Reinitializing Google Sheets connection...")
                if not self.setup_google_sheets():
                    update.message.reply_html(
                        "❌ <b>Failed to initialize Google Sheets.</b>\n\n"
                        "Please check:\n"
                        "1. GOOGLE_CREDENTIALS_JSON environment variable\n"
                        "2. GOOGLE_SHEET_EMAIL environment variable\n"  # CHANGED: ID → EMAIL
                        "3. Spreadsheet sharing permissions\n"
                        "4. Service account has editor access to the spreadsheet"
                    )
                    return False
            
            # Step 2: Ensure sheets are initialized
            update.message.reply_html("📋 <b>Step 2:</b> Ensuring all sheets are initialized...")
            if not self.ensure_sheets_initialized():
                update.message.reply_html("❌ <b>Failed to initialize sheets.</b>\n\nPlease check if the Google Sheet exists and is accessible.")
                return False
            
            # Step 3: Run the comprehensive update
            update.message.reply_html("📊 <b>Step 3:</b> Running comprehensive data update...")
            self.update_google_sheet()
            
            # Get spreadsheet URL for the message
            sheet_url = self.spreadsheet.url if self.spreadsheet else 'Not available'
            
            update.message.reply_html(
                f"✅ <b>Google Sheet updated successfully!</b>\n\n"
                f"📊 <a href='{sheet_url}'>Open Your Sheet</a>\n\n"
                f"<b>Sheets Updated:</b>\n"
                f"• 📈 Dashboard - Overview and stats\n"
                f"• 🎯 Unique Futures - MEXC-only symbols\n"
                f"• 📋 All Futures - All exchange data\n"
                f"• 🔍 MEXC Analysis - Detailed coverage\n"
                f"• 💰 Price Analysis - Top performers\n"
                f"• 📊 Exchange Stats - Performance metrics",
                reply_markup=ReplyKeyboardRemove()
            )
            return True
            
        except Exception as e:
            error_msg = (
                f"❌ <b>Force update failed:</b>\n\n"
                f"<b>Error:</b> {str(e)}\n\n"
                f"<b>Debugging steps:</b>\n"
                f"1. Check GOOGLE_CREDENTIALS_JSON is valid JSON\n"
                f"2. Verify GOOGLE_SHEET_EMAIL is correct\n"  # CHANGED: ID → EMAIL
                f"3. Ensure service account has edit permissions\n"
                f"4. Check if spreadsheet exists and is accessible"
            )
            update.message.reply_html(error_msg)
            logger.error(f"Force update command error: {e}")
            return False
        
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
            "/pricedebug - Price debug\n"
            "/symboldebug - Symbol debug\n"
            "/excel - Download excel\n"
            "/analysis - Full analysis\n"
            "/dataflow - Dataflow SYmbol"
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
        """Perform immediate check with colorful visual progress bar - FIXED PRICE DATA FLOW"""
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
                ("Collecting price data", "💰 Fetching current prices..."),
                ("Analyzing results", "📊 Compiling comprehensive report..."),
                ("Finalizing", "✅ Completing analysis...")
            ]

            exchange_results = {}
            data_before = self.load_data()
            unique_before = set(data_before.get('unique_futures', []))
            
            # VARIABLES TO STORE RESULTS
            unique_after = set()
            new_futures = set()
            lost_futures = set()
            price_data = {}
            
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
                        # Get unique futures directly
                        unique_after, exchange_stats = self.find_unique_futures_robust()
                        
                        # Calculate changes
                        new_futures = unique_after - unique_before
                        lost_futures = unique_before - unique_after
                        current_count = len(unique_after)
                        
                    elif step_name == "Collecting price data":
                        # CRITICAL FIX: Use the EXACT SAME method as symbolsearch
                        # Get batch data directly (what symbolsearch uses)
                        batch_data = self.get_consistent_price_data()
                        logger.info(f"📊 Batch data collected: {len(batch_data)} symbols")
                        
                        # Create price_data by matching unique symbols with batch data
                        price_data = {}
                        matched_symbols = 0
                        
                        for symbol in unique_after:
                            # Try exact match first
                            if symbol in batch_data:
                                price_data[symbol] = batch_data[symbol]
                                matched_symbols += 1
                            else:
                                # Try alternative formats (what symbolsearch does)
                                alt_formats = [
                                    symbol.replace('_', ''),
                                    symbol.replace('_', '-'), 
                                    symbol.replace('_', '/'),
                                ]
                                
                                found = False
                                for alt_format in alt_formats:
                                    if alt_format in batch_data:
                                        price_data[symbol] = batch_data[alt_format].copy()
                                        price_data[symbol]['symbol'] = symbol  # Fix symbol name
                                        matched_symbols += 1
                                        found = True
                                        break
                                
                                if not found:
                                    # Symbol not found in batch, add with None price
                                    price_data[symbol] = {
                                        'symbol': symbol,
                                        'price': None,
                                        'changes': {},
                                        'timestamp': datetime.now(),
                                        'source': 'not_found'
                                    }
                        
                        logger.info(f"💰 Price data: {matched_symbols}/{len(unique_after)} symbols matched")
                        current_count = matched_symbols
                        
                    elif step_name == "Analyzing results":
                        # Analysis is already done, just update progress
                        pass
                        
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
            
            # Calculate price coverage statistics
            unique_with_prices = len([s for s in unique_after if s in price_data and price_data[s].get('price') is not None])
            price_coverage_percent = (unique_with_prices / len(unique_after)) * 100 if unique_after else 0

            # DEBUG: Log what we found for specific symbols
            debug_symbols = ['METASTOCK_USDT', 'TRY_USDT', 'BOBBSC_USDT']
            logger.info("🔍 DEBUG - Checking specific symbols in price_data:")
            for symbol in debug_symbols:
                if symbol in price_data:
                    price_info = price_data[symbol]
                    logger.info(f"  {symbol}: ${price_info.get('price')} (source: {price_info.get('source')})")
                else:
                    logger.info(f"  {symbol}: NOT in price_data")

            # Create final report WITH PRICE DATA
            final_message = "🎯 <b>COMPREHENSIVE CHECK COMPLETE</b>\n\n"
            
            # Exchange Statistics
            final_message += "📊 <b>EXCHANGE STATISTICS</b>\n"
            final_message += f"✅ Working: {len(working_exchanges)}/{len(exchange_results)} exchanges\n"
            final_message += f"📈 Total Futures: {total_futures}\n"
            final_message += f"🎯 MEXC Unique: {len(unique_after)}\n"
            final_message += f"💰 Price Coverage: {unique_with_prices}/{len(unique_after)} ({price_coverage_percent:.1f}%)\n\n"
            
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

            # ADD NEW SECTION: SHOW NEW UNIQUE FUTURES WITH PRICES
            if new_futures:
                final_message += f"\n\n🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
                
                priced_count = 0
                for symbol in list(new_futures)[:10]:  # Show first 10
                    price_info = price_data.get(symbol)
                    
                    if price_info and price_info.get('price') is not None:
                        price = price_info['price']
                        changes = price_info.get('changes', {})
                        change_5m = changes.get('5m', 0)
                        
                        final_message += f"✅ <b>{symbol}</b>\n"
                        final_message += f"   Price: ${price}\n"
                        final_message += f"   5m: {self.format_change(change_5m)}\n\n"
                        priced_count += 1
                    else:
                        final_message += f"✅ <b>{symbol}</b> (price data unavailable)\n\n"
                
                if len(new_futures) > 10:
                    final_message += f"... and {len(new_futures) - 10} more symbols\n\n"
                
                final_message += f"📊 Total unique: <b>{len(unique_after)}</b>\n"
                final_message += f"💰 With prices: <b>{priced_count}/10</b> shown symbols"

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
        """Create comprehensive analysis with both Google Sheet and Excel updates"""
        update.message.reply_html("📈 <b>Creating comprehensive analysis...</b>")
        
        try:
            # Step 1: Update Google Sheet dashboard first
            update.message.reply_html("🔄 <b>Step 1:</b> Updating Google Sheet dashboard...")
            self.update_google_sheet_dashboard()
            
            # Step 2: Get fresh data for reports
            update.message.reply_html("📊 <b>Step 2:</b> Gathering latest data...")
            unique_futures, exchange_stats = self.find_unique_futures_robust()
            
            # Step 3: Create and send text report
            update.message.reply_html("📄 <b>Step 3:</b> Creating text report...")
            report = self.create_analysis_report(unique_futures, exchange_stats)
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
            
            # Step 4: Create and send Excel file
            update.message.reply_html("📁 <b>Step 4:</b> Creating Excel report...")
            self.create_and_send_excel(update, context)
            
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
            "/pricedebug - Price debug\n"
            "/symboldebug - Symbol debug\n"
            "/excel - Download excel\n"
            "/analysis - Full analysis report\n"
            "/dataflow - Dataflow SYmbol"
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