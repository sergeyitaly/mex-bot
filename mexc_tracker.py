import requests
import json
import logging
import os
import time
import schedule
from datetime import datetime
from telegram import Bot, Update
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram.error import TelegramError
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

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
        """Setup Google Sheets connection"""
        try:
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if creds_json:
                creds_dict = json.loads(creds_json)
                self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
                self.gs_client = gspread.authorize(self.creds)
                logger.info("Google Sheets client initialized")
            else:
                self.gs_client = None
                logger.info("Google Sheets not configured")
                
        except Exception as e:
            logger.error(f"Google Sheets setup error: {e}")
            self.gs_client = None
    
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
        """Load data from JSON file"""
        try:
            with open(self.data_file, 'r') as f:
                return json.load(f)
        except:
            return {"unique_futures": [], "last_check": None, "statistics": {}, "exchange_stats": {}, "google_sheet_url": None}
    
    def save_data(self, data):
        """Save data to JSON file"""
        with open(self.data_file, 'w') as f:
            json.dump(data, f, indent=2)

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
        """Get futures from Binance"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for symbol_data in data.get('symbols', []):
                if symbol_data.get('contractType') == 'PERPETUAL':
                    futures.add(symbol_data['symbol'])
            
            logger.info(f"Binance: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"Binance error: {e}")
            return set()
    
    def get_bybit_futures(self):
        """Get futures from Bybit"""
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('result', {}).get('list', []):
                if item.get('status') == 'Trading':
                    symbol = item.get('symbol', '')
                    if symbol:
                        futures.add(symbol)
            
            logger.info(f"Bybit: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"Bybit error: {e}")
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
    
    def get_bitget_futures(self):
        """Get futures from BitGet"""
        try:
            url = "https://api.bitget.com/api/mix/v1/market/contracts?productType=USDT-FUTURES"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            futures = set()
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                if symbol:
                    futures.add(symbol)
            
            logger.info(f"BitGet: {len(futures)} futures")
            return futures
        except Exception as e:
            logger.error(f"BitGet error: {e}")
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
        """Create comprehensive analysis"""
        update.message.reply_html("📈 <b>Creating comprehensive analysis...</b>\nThis may take a few minutes.")
        
        def send_analysis():
            sheet_url = self.create_comprehensive_analysis()
            if sheet_url.startswith("http"):
                message = "✅ <b>Analysis Complete!</b>\n\n"
                message += f"📊 Comprehensive analysis created:\n{sheet_url}\n\n"
                message += "Use /sheet to get this link later."
            else:
                message = f"❌ <b>Analysis Failed:</b>\n{sheet_url}"
            
            update.message.reply_html(message)
        
        # Run in background to avoid timeout
        import threading
        thread = threading.Thread(target=send_analysis)
        thread.start()
    
    def sheet_command(self, update: Update, context: CallbackContext):
        """Get Google Sheet link"""
        data = self.load_data()
        sheet_url = data.get('google_sheet_url')
        
        if sheet_url:
            message = f"📋 <b>Google Sheet Analysis</b>\n\n{sheet_url}\n\nUse /analysis to create a new one."
        else:
            message = "No analysis sheet found. Use /analysis to create one."
        
        update.message.reply_html(message)
    
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
            "/analysis - Full analysis (Google Sheets)\n"
            "/sheet - Get analysis sheet link\n"
            "/status - Current status\n"
            "/exchanges - Exchange information\n\n"
            "<b>Auto-features:</b>\n"
            "• Checks every 60 minutes\n"
            "• Alerts for new unique futures\n"
            "• Comprehensive analysis available\n\n"
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
    
    def run_auto_check(self):
        """Run automatic check"""
        try:
            logger.info("Running scheduled check...")
            
            unique_futures, exchange_stats = self.find_unique_futures()
            current_unique = set(unique_futures)
            
            if current_unique != self.last_unique_futures:
                new_futures = current_unique - self.last_unique_futures
                removed_futures = self.last_unique_futures - current_unique
                
                data = self.load_data()
                data['unique_futures'] = list(current_unique)
                data['last_check'] = datetime.now().isoformat()
                data['exchange_stats'] = exchange_stats
                self.save_data(data)
                
                if new_futures:
                    message = "🚀 <b>NEW UNIQUE FUTURES!</b>\n\n"
                    for symbol in sorted(new_futures):
                        message += f"✅ {symbol}\n"
                    message += f"\n📊 Total: {len(current_unique)}"
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
    
    def setup_scheduler(self):
        """Setup scheduled tasks"""
        schedule.every(self.update_interval).minutes.do(self.run_auto_check)
        logger.info(f"Scheduler setup - checking every {self.update_interval} minutes")
    
    def run_scheduler(self):
        """Run the scheduler"""
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def run(self):
        """Start the bot"""
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
            
            logger.info("Bot started successfully")
            
            # Send startup message
            self.send_broadcast_message(
                "🤖 <b>MEXC Futures Tracker Started</b>\n\n"
                "✅ Monitoring 8 exchanges\n"
                f"⏰ Auto-check: {self.update_interval} minutes\n"
                "📊 Google Sheets analysis available\n"
                "💬 Use /help for commands"
            )
            
            logger.info("Bot is now running...")
            
            # Keep running
            self.updater.idle()
            
        except Exception as e:
            logger.error(f"Bot run error: {e}")
            raise

def main():
    tracker = MEXCTracker()
    tracker.run()

if __name__ == "__main__":
    print("Starting MEXC Futures Tracker with Google Sheets...")
    main()