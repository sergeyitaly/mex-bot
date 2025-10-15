import requests
import json
import logging
import os
import time
import schedule
from datetime import datetime, timedelta
from telegram import Bot, Update
from telegram import Bot, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
from telegram.error import TelegramError
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import fcntl
import threading
import atexit
import io
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from datetime import datetime
import io
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
        self.data_file = 'data.json'
        
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        
        self.updater = Updater(token=self.bot_token, use_context=True)
        self.dispatcher = self.updater.dispatcher
        self.bot = Bot(token=self.bot_token)
        self.setup_handlers()
        self.init_data_file()
        self.last_unique_futures = set()
        
        # Google Sheets setup
        self.setup_google_sheets()
    
    def setup_google_sheets(self):
        """Setup Google Sheets connection using the existing spreadsheet"""
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

            # Use the scope that worked in diagnostics
            scope = ['https://www.googleapis.com/auth/spreadsheets']
            
            try:
                self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
                self.gs_client = gspread.authorize(self.creds)
                
                # Connect to your existing spreadsheet
                self.spreadsheet = self.gs_client.open_by_key("1Axc4-JmvDtYV-uWhVxNqDHPOaXbwo2rJqnBKgnxGJY0")
                logger.info(f"✅ Connected to existing spreadsheet: {self.spreadsheet.title}")
                
            except Exception as e:
                logger.error(f"Failed to connect to spreadsheet: {e}")
                self.gs_client = None
                self.spreadsheet = None

        except Exception as e:
            logger.error(f"Google Sheets setup error: {e}")
            self.gs_client = None
            self.spreadsheet = None

    def auto_sheet_command(self, update: Update, context: CallbackContext):
        """Setup auto-update on the existing Google Sheet"""
        if not self.gs_client or not self.spreadsheet:
            update.message.reply_html("❌ Google Sheets not configured or connected.")
            return
        
        try:
            update.message.reply_html("🔄 <b>Setting up auto-update on your existing Google Sheet...</b>")
            
            # Ensure sheets are properly initialized
            if not self.ensure_sheets_initialized():
                update.message.reply_html("❌ Failed to initialize sheets.")
                return
            
            # Do initial data update
            self.update_google_sheet()
            
            update.message.reply_html(
                f"✅ <b>Auto-Update Configured!</b>\n\n"
                f"📊 <a href='{self.spreadsheet.url}'>Open Your Sheet</a>\n\n"
                f"• Using existing: {self.spreadsheet.title}\n"
                f"• Auto-updates every {self.update_interval} minutes\n"
                f"• Live data from all exchanges\n"
                f"• Real-time unique futures tracking\n\n"
                f"<i>Your sheet will automatically update with fresh data.</i>",
                reply_markup=ReplyKeyboardRemove()
            )
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Auto-sheet error:</b>\n{str(e)}")
                    
    def force_update_command(self, update: Update, context: CallbackContext):
        """Force immediate Google Sheet update"""
        if not self.gs_client:
            update.message.reply_html("❌ Google Sheets not configured.")
            return
        
        try:
            update.message.reply_html("🔄 <b>Force updating Google Sheet...</b>")
            self.update_google_sheet()
            update.message.reply_html("✅ <b>Google Sheet updated successfully!</b>")
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Force update error:</b>\n{str(e)}")
            

    def setup_handlers(self):
        """Setup command handlers"""
        self.dispatcher.add_handler(CommandHandler("start", self.start_command))
        self.dispatcher.add_handler(CommandHandler("status", self.status_command))
        self.dispatcher.add_handler(CommandHandler("check", self.check_command))
        self.dispatcher.add_handler(CommandHandler("help", self.help_command))
        self.dispatcher.add_handler(CommandHandler("stats", self.stats_command))
        self.dispatcher.add_handler(CommandHandler("exchanges", self.exchanges_command))
        self.dispatcher.add_handler(CommandHandler("analysis", self.analysis_command))
        self.dispatcher.add_handler(CommandHandler("sheet", self.sheet_command))
        self.dispatcher.add_handler(CommandHandler("export", self.export_command))
        self.dispatcher.add_handler(CommandHandler("autosheet", self.auto_sheet_command))
        self.dispatcher.add_handler(CommandHandler("forceupdate", self.force_update_command))

        from telegram.ext import MessageHandler, Filters
        self.dispatcher.add_handler(MessageHandler(
            Filters.text & (
                Filters.regex('^(📊 Excel Export|📁 JSON Export|🔗 View Google Sheet|❌ Cancel)$')
            ), 
            self.handle_export
        ))
        
    def init_data_file(self):
        """Initialize data file"""
        if not os.path.exists(self.data_file):
            data = {
                "unique_futures": [],
                "last_check": None,
                "statistics": {
                    "checks_performed": 0,
                    "unique_found_total": 0,
                    "start_time": datetime.now().isoformat()
                },
                "exchange_stats": {},
                "google_sheet_url": None
            }
            self.save_data(data)
    
    def load_data(self):
        """Load data from JSON file with error handling"""
        try:
            # Проверяем существует ли файл
            if not os.path.exists(self.data_file):
                return self.get_default_data()
            
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading data from {self.data_file}: {e}")
            return self.get_default_data()

    def save_data(self, data):
        """Save data to JSON file with error handling"""
        try:
            # Создаем backup на случай ошибки
            backup_file = f"{self.data_file}.backup"
            if os.path.exists(self.data_file):
                import shutil
                shutil.copy2(self.data_file, backup_file)
            
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Data saved to {self.data_file}")
            
        except Exception as e:
            logger.error(f"Error saving data to {self.data_file}: {e}")
            # Пробуем сохранить в временный файл
            try:
                temp_file = f"{self.data_file}.temp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Data saved to temporary file: {temp_file}")
            except Exception as e2:
                logger.error(f"Failed to save even to temporary file: {e2}")

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
            "google_sheet_url": None
        }

    # ==================== EXCHANGE API METHODS ====================
    
    def get_mexc_futures(self):
        """Get futures from MEXC"""
        try:
            url = "https://contract.mexc.com/api/v1/contract/detail"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for contract in data.get('data', []):
                symbol = contract.get('symbol', '')
                if symbol and symbol.endswith('_USDT'):
                    futures.add(symbol)
            
            logger.info(f"MEXC: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"MEXC error: {e}")
            return set()
    
    def get_binance_futures(self):
        """Get futures from Binance with better error handling"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=15)
            response.raise_for_status()  # This will raise an exception for bad status codes
            
            data = response.json()
            
            futures = set()
            for symbol_data in data.get('symbols', []):
                if symbol_data.get('contractType') == 'PERPETUAL' and symbol_data.get('status') == 'TRADING':
                    futures.add(symbol_data['symbol'])
            
            logger.info(f"Binance: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"Binance error: {e} - Response: {getattr(response, 'text', 'No response')}")
            return set()

    def get_bybit_futures(self):
        """Get futures from Bybit with better error handling"""
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, timeout=15, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            futures = set()
            if data.get('retCode') == 0:
                for item in data.get('result', {}).get('list', []):
                    if item.get('status') == 'Trading':
                        symbol = item.get('symbol', '')
                        if symbol:
                            futures.add(symbol)
            
            logger.info(f"Bybit: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"Bybit error: {e} - Response: {getattr(response, 'text', 'No response')}")
            return set()

    def get_bitget_futures(self):
        """Get futures from BitGet with better error handling"""
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            futures = set()
            if data.get('code') == '00000' and data.get('data'):
                for item in data.get('data', []):
                    symbol = item.get('symbol', '')
                    if symbol and item.get('status') == 'normal':
                        futures.add(symbol)
            
            logger.info(f"BitGet: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"BitGet error: {e} - Response: {getattr(response, 'text', 'No response')}")
            return set()
    
    def get_okx_futures(self):
        """Get futures from OKX"""
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('data', []):
                inst_id = item.get('instId', '')
                if inst_id and '-USDT-' in inst_id and 'SWAP' in inst_id:
                    futures.add(inst_id)
            
            logger.info(f"OKX: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"OKX error: {e}")
            return set()
    
    def get_gate_futures(self):
        """Get futures from Gate.io"""
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
        """Get futures from KuCoin"""
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
        """Get futures from BingX"""
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
    
   
    def get_all_exchanges_futures(self):
        """Get futures from all exchanges and return statistics"""
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
                futures = method()
                all_futures.update(futures)
                exchange_stats[name] = len(futures)
                logger.info(f"{name}: {len(futures)} futures")
            except Exception as e:
                logger.error(f"Exchange {name} error: {e}")
                exchange_stats[name] = 0
        
        # Save exchange statistics
        data = self.load_data()
        data['exchange_stats'] = exchange_stats
        self.save_data(data)
        
        logger.info(f"Total futures from all exchanges: {len(all_futures)}")
        return all_futures, exchange_stats
    
    def normalize_symbol(self, symbol):
        """Normalize symbol for comparison across exchanges"""
        normalized = symbol.upper()
        
        suffixes = ['_USDT', 'USDT', 'USDT-PERP', 'PERP', '-PERPETUAL', 'PERPETUAL']
        for suffix in suffixes:
            normalized = normalized.replace(suffix, '')
        
        separators = ['-', '_', ' ']
        for sep in separators:
            normalized = normalized.replace(sep, '')
        
        patterns = ['SWAP:', 'FUTURES:', 'FUTURE:']
        for pattern in patterns:
            normalized = normalized.replace(pattern, '')
        
        return normalized
    
    def find_unique_futures(self):
        """Find futures that are only on MEXC and not on other exchanges"""
        try:
            mexc_futures = self.get_mexc_futures()
            if not mexc_futures:
                logger.error("No MEXC futures retrieved")
                return set(), {}
            
            other_futures, exchange_stats = self.get_all_exchanges_futures()
            
            mexc_normalized = {self.normalize_symbol(s): s for s in mexc_futures}
            other_normalized = {self.normalize_symbol(s) for s in other_futures}
            
            unique_futures = set()
            for normalized, original in mexc_normalized.items():
                if normalized not in other_normalized:
                    unique_futures.add(original)
            
            logger.info(f"Found {len(unique_futures)} unique futures on MEXC")
            return unique_futures, exchange_stats
            
        except Exception as e:
            logger.error(f"Error finding unique futures: {e}")
            return set(), {}

    # ==================== GOOGLE SHEETS ANALYSIS ====================
    
    def create_comprehensive_analysis(self):
        """Create comprehensive Google Sheets analysis"""
        if not self.gs_client:
            return "Google Sheets not configured. Set GOOGLE_CREDENTIALS_JSON in .env"
        
        try:
            # Collect all futures data
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
            for name, method in exchanges.items():
                futures = method()
                exchange_stats[name] = len(futures)
                
                for symbol in futures:
                    all_futures_data.append({
                        'symbol': symbol,
                        'exchange': name,
                        'timestamp': datetime.now().isoformat()
                    })
                
                time.sleep(0.5)  # Rate limiting
            
            # Analyze data
            symbol_coverage = {}
            for future in all_futures_data:
                normalized = self.normalize_symbol(future['symbol'])
                if normalized not in symbol_coverage:
                    symbol_coverage[normalized] = set()
                symbol_coverage[normalized].add(future['exchange'])
            
            # Create Google Sheet
            spreadsheet_name = f"MEXC Futures Analysis {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            spreadsheet = self.gs_client.create(spreadsheet_name)
            
            # Share with email if configured
            share_email = os.getenv('GOOGLE_SHEET_EMAIL')
            if share_email:
                spreadsheet.share(share_email, perm_type='user', role='writer')
            
            # Summary sheet
            summary_sheet = spreadsheet.get_worksheet(0)
            summary_sheet.update_title("Summary")
            
            summary_data = [
                ["COMPREHENSIVE FUTURES ANALYSIS", ""],
                ["Created", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["", ""],
                ["EXCHANGE", "FUTURES COUNT", "STATUS"]
            ]
            
            total_futures = 0
            for exchange, count in exchange_stats.items():
                status = "✅" if count > 0 else "❌"
                summary_data.append([exchange, count, status])
                total_futures += count
            
            unique_count = len([s for s in symbol_coverage.values() if len(s) == 1])
            
            summary_data.extend([
                ["", "", ""],
                ["TOTAL FUTURES", total_futures, ""],
                ["UNIQUE SYMBOLS", len(symbol_coverage), ""],
                ["EXCLUSIVE LISTINGS", unique_count, ""],
                ["EXCHANGES", len(exchanges), ""]
            ])
            
            summary_sheet.update('A1', summary_data)
            
            # All Futures sheet
            all_sheet = spreadsheet.add_worksheet(title="All Futures", rows="5000", cols="6")
            all_data = [["Symbol", "Exchange", "Normalized", "Available On", "Coverage", "Timestamp"]]
            
            for future in all_futures_data:
                normalized = self.normalize_symbol(future['symbol'])
                exchanges_list = symbol_coverage[normalized]
                available_on = ", ".join(sorted(exchanges_list))
                coverage = f"{len(exchanges_list)}/{len(exchanges)}"
                
                all_data.append([
                    future['symbol'],
                    future['exchange'],
                    normalized,
                    available_on,
                    coverage,
                    future['timestamp']
                ])
            
            all_sheet.update('A1', all_data)
            
            # Unique Futures sheet
            unique_sheet = spreadsheet.add_worksheet(title="Unique Futures", rows="1000", cols="5")
            unique_data = [["Symbol", "Exchange", "Normalized", "Exchanges", "Timestamp"]]
            
            for normalized, exchanges_set in symbol_coverage.items():
                if len(exchanges_set) == 1:
                    exchange = list(exchanges_set)[0]
                    # Find original symbol
                    original_symbol = next((f['symbol'] for f in all_futures_data 
                                          if self.normalize_symbol(f['symbol']) == normalized 
                                          and f['exchange'] == exchange), normalized)
                    
                    unique_data.append([
                        original_symbol,
                        exchange,
                        normalized,
                        ", ".join(exchanges_set),
                        datetime.now().isoformat()
                    ])
            
            unique_sheet.update('A1', unique_data)
            
            # MEXC Analysis sheet
            mexc_sheet = spreadsheet.add_worksheet(title="MEXC Analysis", rows="1000", cols="6")
            mexc_data = [["MEXC Symbol", "Normalized", "Available On", "Exchanges", "Status", "Unique"]]
            
            mexc_futures = [f for f in all_futures_data if f['exchange'] == 'MEXC']
            for future in mexc_futures:
                normalized = self.normalize_symbol(future['symbol'])
                exchanges_list = symbol_coverage[normalized]
                available_on = ", ".join(sorted(exchanges_list))
                status = "Unique" if len(exchanges_list) == 1 else "Multi-exchange"
                unique_flag = "✅" if len(exchanges_list) == 1 else "🔸"
                
                mexc_data.append([
                    future['symbol'],
                    normalized,
                    available_on,
                    len(exchanges_list),
                    status,
                    unique_flag
                ])
            
            mexc_sheet.update('A1', mexc_data)
            
            # Save URL
            data = self.load_data()
            data['google_sheet_url'] = spreadsheet.url
            self.save_data(data)
            
            logger.info(f"Google Sheet created: {spreadsheet.url}")
            return spreadsheet.url
            
        except Exception as e:
            logger.error(f"Google Sheets analysis error: {e}")
            return f"Error creating analysis: {str(e)}"

    # ==================== TELEGRAM COMMANDS ====================
    
    def start_command(self, update: Update, context: CallbackContext):
        """Send welcome message"""
        user = update.effective_user
        welcome_text = (
            f"🤖 Hello {user.mention_html()}!\n\n"
            "I'm <b>MEXC Unique Futures Tracker</b>\n\n"
            "<b>Features:</b>\n"
            "• Real-time monitoring of 8 exchanges\n"
            "• Unique futures detection\n"
            "• Google Sheets analysis\n"
            "• Automatic alerts\n\n"
            "<b>Commands:</b>\n"
            "/start - Welcome message\n"
            "/status - Current status\n"
            "/check - Immediate check\n"
            "/analysis - Full analysis\n"
            "/sheet - Google Sheet link\n"
            "/exchanges - Exchange info\n"
            "/stats - Bot statistics\n"
            "/help - Help information"
        )
        update.message.reply_html(welcome_text)
    
    def status_command(self, update: Update, context: CallbackContext):
        """Send current status"""
        data = self.load_data()
        unique_count = len(data.get('unique_futures', []))
        last_check = data.get('last_check', 'Never')
        
        if last_check != 'Never':
            try:
                last_dt = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                last_check = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        status_text = (
            "📊 <b>Current Status</b>\n\n"
            f"🔄 Unique futures: <b>{unique_count}</b>\n"
            f"⏰ Last check: {last_check}\n"
            f"🔍 Check interval: {self.update_interval}min\n"
            f"🤖 Uptime: {self.get_uptime()}"
        )
        
        if unique_count > 0:
            status_text += "\n\n<b>Unique futures:</b>\n"
            for symbol in sorted(data['unique_futures'])[:5]:
                status_text += f"• {symbol}\n"
            if unique_count > 5:
                status_text += f"• ... and {unique_count - 5} more"
        
        update.message.reply_html(status_text)
    
    def check_command(self, update: Update, context: CallbackContext):
        """Perform immediate check"""
        update.message.reply_html("🔍 <b>Checking all exchanges...</b>")
        
        try:
            unique_futures, exchange_stats = self.find_unique_futures()
            data = self.load_data()
            
            stats = data.get('statistics', {})
            stats['checks_performed'] = stats.get('checks_performed', 0) + 1
            stats['unique_found_total'] = max(stats.get('unique_found_total', 0), len(unique_futures))
            
            data['unique_futures'] = list(unique_futures)
            data['last_check'] = datetime.now().isoformat()
            data['statistics'] = stats
            data['exchange_stats'] = exchange_stats
            self.save_data(data)
            
            if unique_futures:
                message = "✅ <b>Check Complete!</b>\n\n"
                message += f"🎯 Found <b>{len(unique_futures)}</b> unique futures:\n\n"
                for symbol in sorted(unique_futures)[:8]:
                    message += f"• {symbol}\n"
                if len(unique_futures) > 8:
                    message += f"• ... and {len(unique_futures) - 8} more"
            else:
                message = "✅ <b>Check Complete!</b>\n\nNo unique futures found."
            
            update.message.reply_html(message)
            
        except Exception as e:
            error_msg = f"❌ <b>Check failed:</b>\n{str(e)}"
            update.message.reply_html(error_msg)
    
    def analysis_command(self, update: Update, context: CallbackContext):
        """Create comprehensive analysis without Google Sheets"""
        update.message.reply_html("📈 <b>Creating comprehensive analysis...</b>")
        
        try:
            # Collect data from all exchanges
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
            
            for name, method in exchanges.items():
                try:
                    futures = method()
                    exchange_stats[name] = len(futures)
                    
                    for symbol in futures:
                        all_futures_data.append({
                            'symbol': symbol,
                            'exchange': name,
                            'timestamp': datetime.now().isoformat()
                        })
                        
                        # Track symbol coverage
                        normalized = self.normalize_symbol(symbol)
                        if normalized not in symbol_coverage:
                            symbol_coverage[normalized] = set()
                        symbol_coverage[normalized].add(name)
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Exchange {name} error: {e}")
                    exchange_stats[name] = 0
            
            # Create and send analysis files directly
            self.send_comprehensive_analysis(update, all_futures_data, exchange_stats, symbol_coverage)
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Analysis error:</b>\n{str(e)}")

    def create_mexc_analysis_excel(self, all_futures_data, symbol_coverage):
        """Create MEXC analysis as Excel file"""
        wb = Workbook()
        ws = wb.active
        ws.title = "MEXC Analysis"
        
        # Header
        ws['A1'] = 'MEXC FUTURES ANALYSIS'
        ws['A1'].font = Font(bold=True, size=14)
        
        # Headers
        headers = ['Symbol', 'Normalized Symbol', 'Available Exchanges']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=2, column=col)
            cell.value = header
            cell.font = Font(bold=True)
        
        # MEXC data
        row = 3
        for future in all_futures_data:
            if future['exchange'] == 'MEXC':
                normalized = self.normalize_symbol(future['symbol'])
                exchanges_list = symbol_coverage.get(normalized, [])
                available_on = ', '.join(sorted(exchanges_list))
                
                ws[f'A{row}'] = future['symbol']
                ws[f'B{row}'] = normalized
                ws[f'C{row}'] = available_on
                row += 1
        
        # Adjust column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 25
        ws.column_dimensions['C'].width = 40
        
        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        excel_content = output.getvalue()
        output.close()
        
        return excel_content

    def send_comprehensive_analysis(self, update: Update, all_futures_data, exchange_stats, symbol_coverage):
        """Send comprehensive analysis as Excel files"""
        try:
            # File 1: Complete analysis
            excel1_content = self.create_complete_analysis_excel(all_futures_data, symbol_coverage, exchange_stats)
            file1 = io.BytesIO(excel1_content)  # Remove .encode('utf-8')
            file1.name = f'futures_complete_analysis_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'  # Change to .xlsx
            
            # File 2: Unique futures only
            excel2_content = self.create_unique_futures_excel(symbol_coverage, all_futures_data)
            file2 = io.BytesIO(excel2_content)  # Remove .encode('utf-8')
            file2.name = f'unique_futures_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'  # Change to .xlsx
            
            # File 3: MEXC analysis - FIX THIS FUNCTION TO RETURN EXCEL TOO
            excel3_content = self.create_mexc_analysis_excel(all_futures_data, symbol_coverage)  # Rename to create_mexc_analysis_excel
            file3 = io.BytesIO(excel3_content)  # Remove .encode('utf-8')
            file3.name = f'mexc_analysis_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'  # Change to .xlsx
            
            # Send files
            update.message.reply_document(
                document=file1,
                caption="📊 <b>Complete Futures Analysis</b>\n\nAll futures from all exchanges",
                parse_mode='HTML'
            )
            
            update.message.reply_document(
                document=file2,
                caption="💎 <b>Unique Futures</b>\n\nFutures available on only one exchange",
                parse_mode='HTML'
            )
            
            update.message.reply_document(
                document=file3,
                caption="🎯 <b>MEXC Analysis</b>\n\nDetailed MEXC futures coverage",
                parse_mode='HTML'
            )
            
            # Send summary
            unique_count = len([s for s in symbol_coverage.values() if len(s) == 1])
            working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
            
            summary = (
                "📈 <b>Analysis Complete!</b>\n\n"
                f"🏢 Exchanges working: {working_exchanges}/{len(exchange_stats)}\n"
                f"📊 Total symbols: {len(symbol_coverage)}\n"
                f"💎 Unique listings: {unique_count}\n"
                f"🔄 MEXC futures: {exchange_stats.get('MEXC', 0)}\n"
                f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
            update.message.reply_html(summary)
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Error sending analysis:</b>\n{str(e)}")
        
    def create_complete_analysis_excel(self, all_futures_data, symbol_coverage, exchange_stats):
        """Create complete analysis Excel file"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Complete Analysis"
        
        # Header styling
        header_font = Font(bold=True, size=14)
        title_font = Font(bold=True, size=12)
        normal_font = Font(size=10)
        
        # Write header
        ws.merge_cells('A1:E1')
        ws['A1'] = 'COMPLETE FUTURES ANALYSIS'
        ws['A1'].font = header_font
        ws['A1'].alignment = Alignment(horizontal='center')
        
        ws['A2'] = 'Generated'
        ws['B2'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ws['A2'].font = title_font
        ws['B2'].font = normal_font
        
        # Exchange summary
        ws['A4'] = 'EXCHANGE SUMMARY'
        ws['A4'].font = title_font
        
        # Exchange summary headers
        headers = ['Exchange', 'Status', 'Futures Count']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=col)
            cell.value = header
            cell.font = title_font
            cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
        
        # Exchange summary data
        row = 6
        for exchange, count in exchange_stats.items():
            status = 'WORKING' if count > 0 else 'FAILED'
            ws[f'A{row}'] = exchange
            ws[f'B{row}'] = status
            ws[f'C{row}'] = count
            row += 1
        
        # Detailed futures data
        ws[f'A{row+2}'] = 'DETAILED FUTURES DATA'
        ws[f'A{row+2}'].font = title_font
        
        # Detailed data headers
        detail_headers = ['Symbol', 'Exchange', 'Normalized Symbol', 'Available On', 'Coverage']
        for col, header in enumerate(detail_headers, 1):
            cell = ws.cell(row=row+3, column=col)
            cell.value = header
            cell.font = title_font
            cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
        
        # Detailed data
        data_row = row + 4
        for future in all_futures_data:
            normalized = self.normalize_symbol(future['symbol'])
            exchanges_list = symbol_coverage.get(normalized, [])
            available_on = ', '.join(sorted(exchanges_list))
            coverage = f"{len(exchanges_list)} exchanges"
            
            ws[f'A{data_row}'] = future['symbol']
            ws[f'B{data_row}'] = future['exchange']
            ws[f'C{data_row}'] = normalized
            ws[f'D{data_row}'] = available_on
            ws[f'E{data_row}'] = coverage
            data_row += 1
        
        # SIMPLE COLUMN WIDTH ADJUSTMENT - NO ITERATION OVER COLUMNS
        # Manually set reasonable column widths
        ws.column_dimensions['A'].width = 20  # Symbol
        ws.column_dimensions['B'].width = 15  # Exchange
        ws.column_dimensions['C'].width = 20  # Normalized Symbol
        ws.column_dimensions['D'].width = 30  # Available On
        ws.column_dimensions['E'].width = 15  # Coverage
        
        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        excel_content = output.getvalue()
        output.close()
        
        return excel_content

    def create_unique_futures_excel(self, symbol_coverage, all_futures_data):
        """Create unique futures Excel file"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Unique Futures"
        
        # Styling
        header_font = Font(bold=True, size=14)
        title_font = Font(bold=True, size=12)
        normal_font = Font(size=10)
        
        # Write header
        ws.merge_cells('A1:C1')
        ws['A1'] = 'UNIQUE FUTURES ANALYSIS'
        ws['A1'].font = header_font
        ws['A1'].alignment = Alignment(horizontal='center')
        
        ws['A2'] = 'Generated'
        ws['B2'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ws['A2'].font = title_font
        ws['B2'].font = normal_font
        
        # Headers
        headers = ['Symbol', 'Exchange', 'Normalized Symbol']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=4, column=col)
            cell.value = header
            cell.font = title_font
            cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
        
        # Data
        row = 5
        unique_count = 0
        for normalized, exchanges_set in symbol_coverage.items():
            if len(exchanges_set) == 1:
                unique_count += 1
                exchange = list(exchanges_set)[0]
                original_symbol = next((f['symbol'] for f in all_futures_data 
                                    if self.normalize_symbol(f['symbol']) == normalized 
                                    and f['exchange'] == exchange), normalized)
                
                ws[f'A{row}'] = original_symbol
                ws[f'B{row}'] = exchange
                ws[f'C{row}'] = normalized
                row += 1
        
        # Summary
        ws[f'A{row+2}'] = 'SUMMARY'
        ws[f'A{row+2}'].font = title_font
        
        ws[f'A{row+3}'] = 'Total unique futures'
        ws[f'B{row+3}'] = unique_count
        ws[f'A{row+3}'].font = title_font
        ws[f'B{row+3}'].font = normal_font
        
        # SIMPLE COLUMN WIDTH ADJUSTMENT - NO ITERATION OVER COLUMNS
        ws.column_dimensions['A'].width = 25  # Symbol
        ws.column_dimensions['B'].width = 15  # Exchange
        ws.column_dimensions['C'].width = 25  # Normalized Symbol
        
        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        excel_content = output.getvalue()
        output.close()
        
        return excel_content

    def sheet_command(self, update: Update, context: CallbackContext):
        """Get Google Sheet link or create new one"""
        data = self.load_data()
        sheet_url = data.get('google_sheet_url')
        
        if sheet_url:
            keyboard = [
                ['📊 Open Existing Sheet', '📈 Create New Analysis'],
                ['❌ Cancel']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
            
            update.message.reply_html(
                f"📋 <b>Google Sheet Available</b>\n\n"
                f"Existing sheet: {sheet_url}\n\n"
                f"Choose an option:",
                reply_markup=reply_markup
            )
            
            # Store context for handling the choice
            context.user_data['existing_sheet_url'] = sheet_url
        else:
            update.message.reply_html(
                "No analysis sheet found. Use /export and choose Google Sheets option to create one."
            )
            
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
            "/analysis - Full analysis (Excel files)\n"
            "/export - Download data (Excel/JSON/Google Sheets)\n"
            "/autosheet - Auto-updating Google Sheet\n"
            "/forceupdate - Force update Google Sheet\n"
            "/status - Current status\n"
            "/exchanges - Exchange information\n\n"
            "<b>Auto-features:</b>\n"
            f"• Checks every {self.update_interval} minutes\n"
            "• Alerts for new unique futures\n"
            "• Google Sheets auto-updates\n"
            "• Data export available\n\n"
            "⚡ <i>Happy trading!</i>"
        )
        update.message.reply_html(help_text)
            
    def get_uptime(self):
        """Calculate bot uptime"""
        data = self.load_data()
        start_time = data.get('statistics', {}).get('start_time')
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                uptime = datetime.now() - start_dt
                days = uptime.days
                hours = uptime.seconds // 3600
                minutes = (uptime.seconds % 3600) // 60
                return f"{days}d {hours}h {minutes}m"
            except:
                pass
        return "Unknown"
    
    def format_start_time(self, start_time):
        """Format start time for display"""
        if start_time:
            try:
                dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                return dt.strftime("%Y-%m-%d %H:%M")
            except:
                pass
        return "Unknown"
    
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
        
    def setup_scheduler(self):
        """Setup scheduled tasks"""
        # Auto-check for unique futures
        schedule.every(self.update_interval).minutes.do(self.run_auto_check)
        
        # Google Sheets auto-update (same interval or different)
        schedule.every(self.update_interval).minutes.do(self.update_google_sheet)
        
        logger.info(f"Scheduler setup - checking every {self.update_interval} minutes")

    def run_auto_check(self):
        """Run automatic check and update Google Sheet"""
        try:
            logger.info("Running scheduled check...")
            
            unique_futures, exchange_stats = self.find_unique_futures()
            current_unique = set(unique_futures)
            
            # Update Google Sheet with fresh data
            self.update_google_sheet()
            
            if current_unique != self.last_unique_futures:
                new_futures = current_unique - self.last_unique_futures
                removed_futures = self.last_unique_futures - current_unique
                
                data = self.load_data()
                data['unique_futures'] = list(current_unique)
                data['last_check'] = datetime.now().isoformat()
                data['exchange_stats'] = exchange_stats
                self.save_data(data)
                
                if new_futures:
                    # Get auto-update sheet URL for the message
                    auto_sheet_url = data.get('auto_update_sheet_url', 'N/A')
                    message = "🚀 <b>NEW UNIQUE FUTURES!</b>\n\n"
                    for symbol in sorted(new_futures):
                        message += f"✅ {symbol}\n"
                    message += f"\n📊 Total: {len(current_unique)}"
                    if auto_sheet_url != 'N/A':
                        message += f"\n📊 <a href='{auto_sheet_url}'>View in Google Sheet</a>"
                    self.send_broadcast_message(message)
                
                if removed_futures:
                    message = "📉 <b>FUTURES NO LONGER UNIQUE:</b>\n\n"
                    for symbol in sorted(removed_futures):
                        message += f"❌ {symbol}\n"
                    message += f"\n📊 Remaining: {len(current_unique)}"
                    self.send_broadcast_message(message)
                
                self.last_unique_futures = current_unique
            
        except Exception as e:
            logger.error(f"Auto-check error: {e}")
                
    def run_scheduler(self):
        """Run the scheduler"""
        while True:
            schedule.run_pending()
            time.sleep(1)
        
    def export_command(self, update: Update, context: CallbackContext):
        """Export data to Excel/JSON or show Google Sheet"""
        update.message.reply_html("🔄 <b>Getting fresh data from exchanges...</b>")
        
        try:
            # Get fresh data directly from APIs
            unique_futures, exchange_stats = self.find_unique_futures()
            
            if not unique_futures:
                update.message.reply_html("❌ No unique futures found to export.")
                return
            
            # Create simplified keyboard
            keyboard = [
                ['📊 Excel Export', '📁 JSON Export'],
            ]
            
            if self.spreadsheet:
                # Only show the option to view existing sheet
                keyboard.append(['🔗 View Google Sheet'])
            
            keyboard.append(['❌ Cancel'])
                
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
            
            # Save data in context
            context.user_data['export_data'] = {
                'unique_futures': list(unique_futures),
                'exchange_stats': exchange_stats,
                'mexc_futures': list(self.get_mexc_futures()),
                'timestamp': datetime.now().isoformat()
            }
            
            message = f"✅ <b>Data collected!</b>\n\n🎯 Unique futures: {len(unique_futures)}\n🏢 Exchanges: {len(exchange_stats) + 1}\n\n"
            
            if self.spreadsheet:
                message += "<b>Choose export format:</b>"
            else:
                message += "<b>Choose export format:</b>\n\n⚠️ Google Sheets not configured"
            
            update.message.reply_html(message, reply_markup=reply_markup)
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Error collecting data:</b>\n{str(e)}")
                    
    def format_google_sheet(self, worksheet):
        """Apply basic formatting to Google Sheet"""
        try:
            # Format header row
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            # Auto-resize columns
            worksheet.columns_auto_resize(0, 10)
            
        except Exception as e:
            logger.error(f"Sheet formatting error: {e}")
          




    def handle_export(self, update: Update, context: CallbackContext):
        """Handle export format selection"""
        choice = update.message.text
        logger.info(f"Export menu selected: {choice}")
        
        if choice == '❌ Cancel':
            update.message.reply_html("Export cancelled.", reply_markup=ReplyKeyboardRemove())
            return
        
        if choice == '📊 Excel Export':
            export_data = context.user_data.get('export_data', {})
            if export_data:
                self.export_to_excel(update, export_data)
            else:
                update.message.reply_html("❌ No export data found. Use /export first.")
        
        elif choice == '📁 JSON Export':
            export_data = context.user_data.get('export_data', {})
            if export_data:
                self.export_to_json(update, export_data)
            else:
                update.message.reply_html("❌ No export data found. Use /export first.")
        
        elif choice == '🔗 View Google Sheet':
            if self.spreadsheet:
                update.message.reply_html(
                    f"📊 <b>Your Auto-Update Google Sheet</b>\n\n"
                    f"🔗 <a href='{self.spreadsheet.url}'>Open Google Sheet</a>\n\n"
                    f"• Real-time data from all exchanges\n"
                    f"• Auto-updates every {self.update_interval} minutes\n"
                    f"• Unique futures tracking\n"
                    f"• Comprehensive analysis",
                    reply_markup=ReplyKeyboardRemove()
                )
            else:
                update.message.reply_html("❌ Google Sheets not configured.")
        
        else:
            update.message.reply_html("❌ Unknown export option.", reply_markup=ReplyKeyboardRemove())
        
        # Clear context
        context.user_data.pop('export_data', None)
                                
    def export_to_excel(self, update: Update, export_data):
        """Export to Excel format"""
        try:
            unique_futures = export_data['unique_futures']
            exchange_stats = export_data['exchange_stats']
            mexc_futures = export_data['mexc_futures']
            
            # Create Excel workbook in memory
            wb = Workbook()
            ws = wb.active
            ws.title = "MEXC Export"
            
            # Styling
            header_font = Font(bold=True, size=14)
            title_font = Font(bold=True, size=12)
            normal_font = Font(size=10)
            
            # Header
            ws.merge_cells('A1:C1')
            ws['A1'] = 'MEXC UNIQUE FUTURES EXPORT'
            ws['A1'].font = header_font
            ws['A1'].alignment = Alignment(horizontal='center')
            
            ws['A2'] = 'Generated'
            ws['B2'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ws['A2'].font = title_font
            ws['B2'].font = normal_font
            
            # Unique futures section
            ws['A4'] = 'UNIQUE FUTURES ON MEXC'
            ws['A4'].font = title_font
            
            # Headers for unique futures
            headers = ['Symbol', 'Status', 'Timestamp']
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=5, column=col)
                cell.value = header
                cell.font = title_font
                cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
            
            # Unique futures data
            row = 6
            for symbol in sorted(unique_futures):
                ws[f'A{row}'] = symbol
                ws[f'B{row}'] = 'UNIQUE'
                ws[f'C{row}'] = export_data['timestamp']
                row += 1
            
            # Exchange statistics section
            ws[f'A{row+2}'] = 'EXCHANGE STATISTICS'
            ws[f'A{row+2}'].font = title_font
            
            # Exchange headers
            exchange_headers = ['Exchange', 'Futures Count']
            for col, header in enumerate(exchange_headers, 1):
                cell = ws.cell(row=row+3, column=col)
                cell.value = header
                cell.font = title_font
                cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
            
            # Exchange data
            stat_row = row + 4
            ws[f'A{stat_row}'] = 'MEXC'
            ws[f'B{stat_row}'] = len(mexc_futures)
            stat_row += 1
            
            for exchange, count in sorted(exchange_stats.items()):
                ws[f'A{stat_row}'] = exchange
                ws[f'B{stat_row}'] = count
                stat_row += 1
            
            # Summary section
            ws[f'A{stat_row+2}'] = 'SUMMARY'
            ws[f'A{stat_row+2}'].font = title_font
            
            summary_data = [
                ['Total Unique Futures', len(unique_futures)],
                ['Total Exchanges', len(exchange_stats) + 1],
                ['Total MEXC Futures', len(mexc_futures)]
            ]
            
            for i, (label, value) in enumerate(summary_data, start=stat_row+3):
                ws[f'A{i}'] = label
                ws[f'B{i}'] = value
                ws[f'A{i}'].font = title_font
            
            # Adjust column widths
            ws.column_dimensions['A'].width = 25
            ws.column_dimensions['B'].width = 15
            ws.column_dimensions['C'].width = 20
            
            # Save to bytes
            output = io.BytesIO()
            wb.save(output)
            excel_content = output.getvalue()
            output.close()
            
            # Prepare file for sending
            file_obj = io.BytesIO(excel_content)
            file_obj.name = f'mexc_unique_futures_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'  # Changed to .xlsx
            
            update.message.reply_document(
                document=file_obj,
                caption="📊 <b>MEXC Unique Futures Export</b>\n\n"
                    f"✅ {len(unique_futures)} unique futures\n"
                    f"🏢 {len(exchange_stats) + 1} exchanges monitored\n"
                    f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>Excel export error:</b>\n{str(e)}")
            logger.error(f"Excel export error: {e}")
            
    def export_to_json(self, update: Update, export_data):
        """Export to JSON format"""
        try:
            # Создаем структуру данных для JSON
            json_data = {
                "metadata": {
                    "export_timestamp": export_data['timestamp'],
                    "total_exchanges": len(export_data['exchange_stats']) + 1,
                    "unique_futures_count": len(export_data['unique_futures']),
                    "mexc_futures_count": len(export_data['mexc_futures'])
                },
                "unique_futures": export_data['unique_futures'],
                "exchange_statistics": export_data['exchange_stats'],
                "mexc_futures": export_data['mexc_futures']
            }
            
            # Конвертируем в JSON строку
            json_str = json.dumps(json_data, indent=2, ensure_ascii=False)
            
            # Подготавливаем файл для отправки
            file_obj = io.BytesIO(json_str.encode('utf-8'))
            file_obj.name = f'mexc_futures_data_{datetime.now().strftime("%Y%m%d_%H%M")}.json'
            
            update.message.reply_document(
                document=file_obj,
                caption="📁 <b>MEXC Futures Data Export</b>\n\n"
                    "Complete dataset in JSON format",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
            
        except Exception as e:
            update.message.reply_html(f"❌ <b>JSON export error:</b>\n{str(e)}")
            logger.error(f"JSON export error: {e}")

    def export_full_analysis(self, update: Update):
        """Create and send full analysis files"""
        update.message.reply_html("📈 <b>Creating full analysis export...</b>")
        
        def create_analysis():
            try:
                # Collect all data
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
                
                for name, method in exchanges.items():
                    futures = method()
                    exchange_stats[name] = len(futures)
                    
                    for symbol in futures:
                        all_futures_data.append({
                            'symbol': symbol,
                            'exchange': name,
                            'timestamp': datetime.now().isoformat()
                        })
                        
                        # Track symbol coverage
                        normalized = self.normalize_symbol(symbol)
                        if normalized not in symbol_coverage:
                            symbol_coverage[normalized] = set()
                        symbol_coverage[normalized].add(name)
                    
                    time.sleep(0.5)
                
                # Create comprehensive Excel
                import Excel
                import io
                
                # Excel 1: All futures with coverage
                output1 = io.StringIO()
                writer1 = Excel.writer(output1)
                writer1.writerow(['Symbol', 'Exchange', 'Normalized Symbol', 'Available On', 'Coverage'])
                
                for future in all_futures_data:
                    normalized = self.normalize_symbol(future['symbol'])
                    exchanges_list = symbol_coverage[normalized]
                    available_on = ', '.join(sorted(exchanges_list))
                    coverage = f"{len(exchanges_list)}/{len(exchanges)}"
                    
                    writer1.writerow([
                        future['symbol'],
                        future['exchange'],
                        normalized,
                        available_on,
                        coverage
                    ])
                
                Excel1_data = output1.getvalue().encode('utf-8')
                file1 = io.BytesIO(Excel1_data)
                file1.name = f'futures_complete_analysis_{datetime.now().strftime("%Y%m%d_%H%M")}.excel'
                
                # Excel 2: Unique futures only
                output2 = io.StringIO()
                writer2 = Excel.writer(output2)
                writer2.writerow(['Symbol', 'Exchange', 'Normalized Symbol', 'Exchanges Count'])
                
                unique_count = 0
                for normalized, exchanges_set in symbol_coverage.items():
                    if len(exchanges_set) == 1:
                        unique_count += 1
                        exchange = list(exchanges_set)[0]
                        original_symbol = next((f['symbol'] for f in all_futures_data 
                                            if self.normalize_symbol(f['symbol']) == normalized 
                                            and f['exchange'] == exchange), normalized)
                        
                        writer2.writerow([
                            original_symbol,
                            exchange,
                            normalized,
                            len(exchanges_set)
                        ])
                
                Excel2_data = output2.getvalue().encode('utf-8')
                file2 = io.BytesIO(Excel2_data)
                file2.name = f'unique_futures_{datetime.now().strftime("%Y%m%d_%H%M")}.excel'
                
                # Send files
                update.message.reply_document(
                    document=file1,
                    caption="📊 <b>Complete Futures Analysis</b>\n\n"
                        f"Total symbols: {len(symbol_coverage)}\n"
                        f"Unique listings: {unique_count}\n"
                        f"Exchanges: {len(exchanges)}",
                    parse_mode='HTML'
                )
                
                update.message.reply_document(
                    document=file2,
                    caption="💎 <b>Unique Futures Only</b>\n\n"
                        f"Found {unique_count} exclusive listings",
                    parse_mode='HTML'
                )
                
            except Exception as e:
                update.message.reply_html(f"❌ <b>Analysis export error:</b>\n{str(e)}")
        
        # Run in background
        import threading
        thread = threading.Thread(target=create_analysis)
        thread.start()
            
    def setup_auto_update_sheet(self):
        """Setup auto-update using the existing spreadsheet"""
        if not self.gs_client or not self.spreadsheet:
            logger.error("Google Sheets client or spreadsheet not available")
            return None
        
        try:
            # Use the existing spreadsheet we're already connected to
            spreadsheet = self.spreadsheet
            
            # Initialize or update the sheets structure
            self.initialize_auto_update_sheets(spreadsheet)
            
            # Save URL for reference
            data = self.load_data()
            data['auto_update_sheet_url'] = spreadsheet.url
            self.save_data(data)
            
            logger.info(f"✅ Auto-update configured for: {spreadsheet.title}")
            return spreadsheet
                
        except Exception as e:
            logger.error(f"Error setting up auto-update sheet: {e}")
            return None

    def initialize_auto_update_sheets(self, spreadsheet):
        """Initialize the sheet structure in your existing spreadsheet"""
        try:
            # Clear existing worksheets except the first one (Лист1)
            existing_sheets = spreadsheet.worksheets()
            
            # Keep the first sheet, remove others
            if len(existing_sheets) > 1:
                for sheet in existing_sheets[1:]:
                    spreadsheet.del_worksheet(sheet)
            
            # Rename first sheet to Dashboard
            main_sheet = existing_sheets[0]
            main_sheet.update_title("Dashboard")
            
            # Create other sheets
            sheets_to_create = [
                ("Unique Futures", 5),
                ("All Futures", 7), 
                ("MEXC Analysis", 6),
                ("Exchange Stats", 5)
            ]
            
            for sheet_name, cols in sheets_to_create:
                try:
                    spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=str(cols))
                except Exception as e:
                    logger.warning(f"Could not create sheet {sheet_name}: {e}")
            
            # Setup Dashboard
            self.setup_dashboard_sheet(main_sheet)
            
            logger.info("✅ Sheet structure initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing sheets: {e}")

    def initialize_sheet_with_headers(self, spreadsheet, sheet_name, headers):
        """Initialize a sheet with headers if it doesn't exist"""
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            # Clear existing data but keep headers
            if worksheet.row_count > 1:
                worksheet.delete_rows(2, worksheet.row_count)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows='1000', cols=str(len(headers[0])))
            worksheet.update('A1', headers)

    def format_auto_update_sheets(self, spreadsheet):
        """Apply formatting to auto-update sheets"""
        try:
            for sheet_name in ['Dashboard', 'Unique Futures', 'All Futures', 'MEXC Analysis', 'Exchange Stats']:
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    
                    # Format headers
                    worksheet.format('A1:Z1', {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8},
                        'horizontalAlignment': 'CENTER'
                    })
                    
                    # Auto-resize columns
                    worksheet.columns_auto_resize(0, worksheet.col_count)
                    
                    # Add freeze panes for headers
                    worksheet.freeze(rows=1)
                    
                except gspread.WorksheetNotFound:
                    continue
                    
        except Exception as e:
            logger.error(f"Error formatting sheets: {e}")

    def ensure_sheets_initialized(self):
        """Ensure all required sheets exist and have proper headers with enough rows"""
        if not self.spreadsheet:
            return False
        
        try:
            # Delete all existing sheets except the first one
            existing_sheets = self.spreadsheet.worksheets()
            if len(existing_sheets) > 1:
                for sheet in existing_sheets[1:]:
                    self.spreadsheet.del_worksheet(sheet)
            
            # Rename first sheet to Dashboard
            main_sheet = existing_sheets[0]
            main_sheet.update_title("Dashboard")
            
            # Define sheets with proper row counts
            sheets_config = {
                'Unique Futures': {
                    'rows': 1000,
                    'cols': 5,
                    'headers': [['Symbol', 'Status', 'Last Updated', 'Normalized Symbol', 'First Detected']]
                },
                'All Futures': {
                    'rows': 3000,  # Increased for more data
                    'cols': 7,
                    'headers': [['Symbol', 'Exchange', 'Normalized', 'Available On', 'Coverage', 'Timestamp', 'Unique']]
                },
                'MEXC Analysis': {
                    'rows': 1000,
                    'cols': 7,
                    'headers': [['MEXC Symbol', 'Normalized', 'Available On', 'Exchanges', 'Status', 'Unique', 'Timestamp']]
                },
                'Exchange Stats': {
                    'rows': 20,
                    'cols': 5,
                    'headers': [['Exchange', 'Futures Count', 'Status', 'Last Updated', 'Success Rate']]
                }
            }
            
            for sheet_name, config in sheets_config.items():
                try:
                    worksheet = self.spreadsheet.add_worksheet(
                        title=sheet_name, 
                        rows=config['rows'],
                        cols=config['cols']
                    )
                    worksheet.update('A1', config['headers'])
                    logger.info(f"Created sheet: {sheet_name} with {config['rows']} rows")
                except Exception as e:
                    logger.error(f"Error creating sheet {sheet_name}: {e}")
            
            # Setup Dashboard
            self.setup_dashboard_sheet(main_sheet)
            
            logger.info("✅ All sheets initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring sheets initialized: {e}")
            return False
        
    def update_google_sheet(self):
        """Update the Google Sheet with fresh data - FIXED"""
        if not self.gs_client or not self.spreadsheet:
            logger.warning("Google Sheets not available for update")
            return
        
        try:
            logger.info("🔄 Starting Google Sheet update...")
            
            # Collect fresh data
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
            current_time = datetime.now().isoformat()
            
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
                        normalized = self.normalize_symbol(symbol)
                        if normalized not in symbol_coverage:
                            symbol_coverage[normalized] = set()
                        symbol_coverage[normalized].add(name)
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Exchange {name} error during sheet update: {e}")
                    exchange_stats[name] = 0
            
            logger.info(f"Total futures collected: {len(all_futures_data)}")
            logger.info(f"Unique symbols: {len(symbol_coverage)}")
            
            # Update all sheets with fresh data
            self.update_unique_futures_sheet(self.spreadsheet, symbol_coverage, all_futures_data, current_time)
            self.update_all_futures_sheet(self.spreadsheet, all_futures_data, symbol_coverage, current_time)
            self.update_mexc_analysis_sheet(self.spreadsheet, all_futures_data, symbol_coverage, current_time)
            self.update_exchange_stats_sheet(self.spreadsheet, exchange_stats, current_time)
            self.update_dashboard_stats(exchange_stats, len(symbol_coverage))
            
            logger.info("✅ Google Sheet update completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Google Sheet update error: {e}")
            
    def update_unique_futures_sheet(self, spreadsheet, symbol_coverage, all_futures_data, timestamp):
        """Update Unique Futures sheet with batch writing"""
        try:
            worksheet = spreadsheet.worksheet('Unique Futures')
            
            # Clear existing data (keep headers)
            if worksheet.row_count > 1:
                worksheet.clear()
                # Re-add headers
                worksheet.update('A1', [['Symbol', 'Status', 'Last Updated', 'Normalized Symbol', 'First Detected']])
            
            unique_data = []
            for normalized, exchanges_set in symbol_coverage.items():
                if len(exchanges_set) == 1:
                    exchange = list(exchanges_set)[0]
                    original_symbol = next((f['symbol'] for f in all_futures_data 
                                        if self.normalize_symbol(f['symbol']) == normalized 
                                        and f['exchange'] == exchange), normalized)
                    
                    unique_data.append([
                        original_symbol,
                        'UNIQUE',
                        timestamp,
                        normalized,
                        timestamp  # First detected
                    ])
            
            # Write in batches to avoid API limits
            if unique_data:
                # Split into batches of 100 rows
                batch_size = 100
                for i in range(0, len(unique_data), batch_size):
                    batch = unique_data[i:i + batch_size]
                    worksheet.update(f'A{i+2}', batch)
                
                logger.info(f"Updated Unique Futures with {len(unique_data)} records")
            
        except Exception as e:
            logger.error(f"Error updating Unique Futures sheet: {e}")


    def update_all_futures_sheet(self, spreadsheet, all_futures_data, symbol_coverage, timestamp):
        """Update All Futures sheet with batch writing"""
        try:
            worksheet = spreadsheet.worksheet('All Futures')
            
            # Clear existing data (keep headers)
            if worksheet.row_count > 1:
                worksheet.clear()
                # Re-add headers
                worksheet.update('A1', [['Symbol', 'Exchange', 'Normalized', 'Available On', 'Coverage', 'Timestamp', 'Unique']])
            
            all_data = []
            for future in all_futures_data:
                normalized = self.normalize_symbol(future['symbol'])
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
                    is_unique
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

    def update_mexc_analysis_sheet(self, spreadsheet, all_futures_data, symbol_coverage, timestamp):
        """Update MEXC Analysis sheet - FIXED VERSION"""
        try:
            worksheet = spreadsheet.worksheet('MEXC Analysis')
            
            logger.info(f"Updating MEXC Analysis sheet with {len(all_futures_data)} total futures")
            
            # Get only MEXC futures
            mexc_futures = [f for f in all_futures_data if f['exchange'] == 'MEXC']
            logger.info(f"Found {len(mexc_futures)} MEXC futures")
            
            if not mexc_futures:
                logger.warning("No MEXC futures found to analyze")
                return
            
            # Clear the sheet completely and set up headers
            worksheet.clear()
            headers = [['MEXC Symbol', 'Normalized', 'Available On', 'Exchanges', 'Status', 'Unique', 'Timestamp']]
            worksheet.update('A1', headers)
            
            mexc_data = []
            
            for future in mexc_futures:
                try:
                    symbol = future['symbol']
                    normalized = self.normalize_symbol(symbol)
                    exchanges_list = symbol_coverage.get(normalized, set())
                    available_on = ", ".join(sorted(exchanges_list)) if exchanges_list else "MEXC Only"
                    exchange_count = len(exchanges_list)
                    status = "Unique" if exchange_count == 1 else "Multi-exchange"
                    unique_flag = "✅" if exchange_count == 1 else "🔸"
                    
                    mexc_data.append([
                        symbol,
                        normalized,
                        available_on,
                        exchange_count,
                        status,
                        unique_flag,
                        timestamp
                    ])
                    
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
            raise  # Re-raise to see the full error


    def update_exchange_stats_sheet(self, spreadsheet, exchange_stats, timestamp):
        """Update Exchange Stats sheet"""
        try:
            worksheet = spreadsheet.worksheet('Exchange Stats')
            
            # Clear existing data (keep headers)
            if worksheet.row_count > 1:
                worksheet.clear()
                # Re-add headers
                worksheet.update('A1', [['Exchange', 'Futures Count', 'Status', 'Last Updated', 'Success Rate']])
            
            stats_data = []
            for exchange, count in exchange_stats.items():
                status = "✅ WORKING" if count > 0 else "❌ FAILED"
                stats_data.append([
                    exchange,
                    count,
                    status,
                    timestamp,
                    "100%"  # Placeholder
                ])
            
            if stats_data:
                worksheet.update('A2', stats_data)
                logger.info(f"Updated Exchange Stats with {len(stats_data)} records")
            
        except Exception as e:
            logger.error(f"Error updating Exchange Stats sheet: {e}")


    def update_dashboard_stats(self, exchange_stats, unique_symbols_count):
        """Update the dashboard with current statistics"""
        if not self.spreadsheet:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet("Dashboard")
            
            # Count working exchanges
            working_exchanges = sum(1 for count in exchange_stats.values() if count > 0)
            total_exchanges = len(exchange_stats)
            
            # Get unique futures count
            try:
                unique_ws = self.spreadsheet.worksheet("Unique Futures")
                unique_count = len(unique_ws.get_all_values()) - 1  # Subtract header
            except:
                unique_count = 0
            
            stats_update = [
                ["Total Unique Futures", unique_count],
                ["Total MEXC Futures", exchange_stats.get('MEXC', 0)],
                ["Working Exchanges", f"{working_exchanges}/{total_exchanges}"],
                ["Next Auto-Update", (datetime.now() + timedelta(minutes=self.update_interval)).strftime('%H:%M:%S')]
            ]
            
            # Update stats section (starting at row 6)
            for i, (label, value) in enumerate(stats_update):
                worksheet.update(f'A{6+i}:B{6+i}', [[label, value]])
                
        except Exception as e:
            logger.error(f"Error updating dashboard stats: {e}")



    def update_dashboard_timestamp(self, spreadsheet):
        """Update the last updated timestamp on Dashboard"""
        try:
            worksheet = spreadsheet.worksheet('Dashboard')
            worksheet.update('B2', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            logger.error(f"Error updating dashboard timestamp: {e}")
    def setup_dashboard_sheet(self, worksheet):
        """Setup the dashboard sheet with basic info"""
        try:
            dashboard_data = [
                ["🤖 MEXC FUTURES AUTO-UPDATE DASHBOARD", ""],
                ["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Update Interval", f"{self.update_interval} minutes"],
                ["", ""],
                ["QUICK STATS", ""],
                ["Total Unique Futures", "Will update automatically"],
                ["Total MEXC Futures", "Will update automatically"],
                ["Working Exchanges", "Will update automatically"],
                ["", ""],
                ["BOT STATUS", "🟢 RUNNING"],
                ["Next Auto-Update", "Will update automatically"],
                ["", ""],
                ["SHEETS", ""],
                ["Dashboard", "Overview and stats"],
                ["Unique Futures", "Futures only on MEXC"],
                ["All Futures", "All futures from all exchanges"],
                ["MEXC Analysis", "Detailed MEXC coverage"],
                ["Exchange Stats", "Exchange performance"]
            ]
            
            worksheet.update('A1', dashboard_data)
            logger.info("✅ Dashboard sheet initialized")
            
        except Exception as e:
            logger.error(f"Error setting up dashboard: {e}")


    def test_google_sheets(self, update: Update, context: CallbackContext):
        """Test Google Sheets connection"""
        if not self.gs_client:
            update.message.reply_html("❌ Google Sheets not configured.")
            return
        
        try:
            update.message.reply_html("🔄 Testing Google Sheets connection...")
            
            # Try to create a test sheet
            test_sheet_name = f"Test Sheet {datetime.now().strftime('%H:%M:%S')}"
            spreadsheet = self.gs_client.create(test_sheet_name)
            
            # Clean up test sheet
            self.gs_client.del_spreadsheet(spreadsheet.id)
            
            update.message.reply_html("✅ Google Sheets connection working!")
            
        except Exception as e:
            update.message.reply_html(f"❌ Google Sheets test failed: {str(e)}")


    def run(self):
        """Start the bot with single instance lock"""
        # Acquire lock to ensure only one instance runs
        if not self.acquire_lock():
            logger.error("Another instance is already running. Exiting.")
            return
        
        try:
            # Load initial data
            data = self.load_data()
            self.last_unique_futures = set(data.get('unique_futures', []))
            
            # Setup scheduler
            self.setup_scheduler()
            
            # Start scheduler in background
            import threading
            scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
            scheduler_thread.start()
            
            # Start the bot
            self.updater.start_polling()
            
            logger.info("Bot started successfully - single instance running")
            
            # Send startup message
            startup_msg = (
                "🤖 <b>MEXC Futures Tracker Started</b>\n\n"
                "✅ Monitoring 8 exchanges\n"
                f"⏰ Auto-check: {self.update_interval} minutes\n"
                "📊 Google Sheets auto-updates\n"
                "📤 Data export available (/export)\n"
                "💬 Use /help for commands"
            )
            
            # Add Google Sheets info if configured
            if self.gs_client:
                startup_msg += "\n\n📊 Use /autosheet for auto-updating Google Sheet"
            
            self.send_broadcast_message(startup_msg)
            
            logger.info("Bot is now running and ready for commands...")
            
            # Keep running with proper cleanup
            try:
                self.updater.idle()
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
            finally:
                self.cleanup()
                
        except Exception as e:
            logger.error(f"Bot run error: {e}")
            self.cleanup()
            raise
        
    def acquire_lock(self):
        """Acquire lock to ensure only one instance runs"""
        try:
            self.lock_file = open('/tmp/mexc_bot.lock', 'w')
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            atexit.register(self.cleanup)
            return True
        except (IOError, BlockingIOError):
            return False

    def cleanup(self):
        """Cleanup resources on exit"""
        try:
            if hasattr(self, 'lock_file') and self.lock_file:
                fcntl.flock(self.lock_file, fcntl.LOCK_UN)
                self.lock_file.close()
                try:
                    os.unlink('/tmp/mexc_bot.lock')
                except:
                    pass
            logger.info("Bot cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

def main():
    tracker = MEXCTracker()
    tracker.run()

if __name__ == "__main__":
    print("Starting MEXC Futures Tracker...")
    main()