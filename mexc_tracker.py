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

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InteractiveFuturesTracker:
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
    
    def setup_handlers(self):
        """Setup command handlers"""
        self.dispatcher.add_handler(CommandHandler("start", self.start_command))
        self.dispatcher.add_handler(CommandHandler("status", self.status_command))
        self.dispatcher.add_handler(CommandHandler("check", self.check_command))
        self.dispatcher.add_handler(CommandHandler("help", self.help_command))
        self.dispatcher.add_handler(CommandHandler("stats", self.stats_command))
        self.dispatcher.add_handler(CommandHandler("exchanges", self.exchanges_command))
    
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
                "exchange_stats": {}
            }
            self.save_data(data)
    
    def load_data(self):
        """Load data from JSON file"""
        try:
            with open(self.data_file, 'r') as f:
                return json.load(f)
        except:
            return {"unique_futures": [], "last_check": None, "statistics": {}, "exchange_stats": {}}
    
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
        # Remove exchange-specific suffixes and prefixes
        normalized = symbol.upper()
        
        # Remove common suffixes
        suffixes = ['_USDT', 'USDT', 'USDT-PERP', 'PERP', '-PERPETUAL', 'PERPETUAL']
        for suffix in suffixes:
            normalized = normalized.replace(suffix, '')
        
        # Remove common separators
        separators = ['-', '_', ' ']
        for sep in separators:
            normalized = normalized.replace(sep, '')
        
        # Remove specific exchange patterns
        patterns = ['SWAP:', 'FUTURES:', 'FUTURE:']
        for pattern in patterns:
            normalized = normalized.replace(pattern, '')
        
        return normalized
    
    def find_unique_futures(self):
        """Find futures that are only on MEXC and not on other exchanges"""
        try:
            # Get MEXC futures
            mexc_futures = self.get_mexc_futures()
            if not mexc_futures:
                logger.error("No MEXC futures retrieved")
                return set(), {}
            
            # Get all other exchanges futures
            other_futures, exchange_stats = self.get_all_exchanges_futures()
            
            # Normalize symbols for comparison
            mexc_normalized = {self.normalize_symbol(s): s for s in mexc_futures}
            other_normalized = {self.normalize_symbol(s) for s in other_futures}
            
            # Find unique futures (only on MEXC)
            unique_futures = set()
            for normalized, original in mexc_normalized.items():
                if normalized not in other_normalized:
                    unique_futures.add(original)
            
            logger.info(f"Found {len(unique_futures)} unique futures on MEXC")
            return unique_futures, exchange_stats
            
        except Exception as e:
            logger.error(f"Error finding unique futures: {e}")
            return set(), {}

    # ==================== TELEGRAM COMMANDS ====================
    
    def start_command(self, update: Update, context: CallbackContext):
        """Send welcome message when command /start is issued."""
        user = update.effective_user
        welcome_text = (
            f"🤖 Hello {user.mention_html()}!\n\n"
            "I'm <b>MEXC Unique Futures Tracker</b>\n"
            "I monitor for perpetual contracts that are available on MEXC but not on other major exchanges.\n\n"
            "<b>Supported Exchanges:</b>\n"
            "• MEXC (source)\n"
            "• Binance, Bybit, OKX\n" 
            "• Gate.io, KuCoin\n"
            "• BingX, BitGet\n\n"
            "<b>Available commands:</b>\n"
            "/start - Show this welcome message\n"
            "/status - Check current status\n"
            "/check - Perform immediate check\n"
            "/stats - Show statistics\n"
            "/exchanges - Exchange information\n"
            "/help - Show help information"
        )
        update.message.reply_html(welcome_text)
        logger.info(f"Start command received from user: {user.id}")
    
    def status_command(self, update: Update, context: CallbackContext):
        """Send current status when command /status is issued."""
        data = self.load_data()
        unique_count = len(data.get('unique_futures', []))
        last_check = data.get('last_check', 'Never')
        exchange_stats = data.get('exchange_stats', {})
        
        if last_check != 'Never':
            try:
                last_dt = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                last_check = last_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            except:
                pass
        
        status_text = (
            "📊 <b>Current Status</b>\n\n"
            f"🔄 Unique futures found: <b>{unique_count}</b>\n"
            f"⏰ Last check: {last_check}\n"
            f"🔍 Auto-check interval: {self.update_interval} minutes\n"
            f"🤖 Bot uptime: {self.get_uptime()}"
        )
        
        if unique_count > 0:
            status_text += "\n\n<b>Current unique futures:</b>\n"
            for symbol in sorted(data['unique_futures'])[:8]:
                status_text += f"• {symbol}\n"
            if unique_count > 8:
                status_text += f"• ... and {unique_count - 8} more"
        
        update.message.reply_html(status_text)
    
    def exchanges_command(self, update: Update, context: CallbackContext):
        """Show exchange information when command /exchanges is issued."""
        data = self.load_data()
        exchange_stats = data.get('exchange_stats', {})
        
        exchanges_text = "🏢 <b>Supported Exchanges</b>\n\n"
        
        # MEXC (source)
        mexc_count = len(self.get_mexc_futures())
        exchanges_text += f"🎯 <b>MEXC</b> (source): {mexc_count} futures\n"
        
        # Other exchanges
        if exchange_stats:
            exchanges_text += "\n<b>Other Exchanges:</b>\n"
            for exchange, count in sorted(exchange_stats.items()):
                status = "✅" if count > 0 else "❌"
                exchanges_text += f"{status} <b>{exchange}</b>: {count} futures\n"
        else:
            exchanges_text += "\nNo exchange data available. Use /check to update."
        
        exchanges_text += f"\n🔍 Monitoring <b>{len(exchange_stats) + 1}</b> exchanges total"
        
        update.message.reply_html(exchanges_text)
    
    def check_command(self, update: Update, context: CallbackContext):
        """Perform immediate check when command /check is issued."""
        update.message.reply_html("🔍 <b>Performing immediate check across all exchanges...</b>")
        
        try:
            unique_futures, exchange_stats = self.find_unique_futures()
            data = self.load_data()
            
            # Update statistics
            stats = data.get('statistics', {})
            stats['checks_performed'] = stats.get('checks_performed', 0) + 1
            stats['unique_found_total'] = max(stats.get('unique_found_total', 0), len(unique_futures))
            
            # Update data
            data['unique_futures'] = list(unique_futures)
            data['last_check'] = datetime.now().isoformat()
            data['statistics'] = stats
            data['exchange_stats'] = exchange_stats
            self.save_data(data)
            
            # Prepare response
            if unique_futures:
                message = "✅ <b>Check Complete!</b>\n\n"
                message += f"🎯 Found <b>{len(unique_futures)}</b> unique futures on MEXC:\n\n"
                for symbol in sorted(unique_futures)[:10]:
                    message += f"• {symbol}\n"
                if len(unique_futures) > 10:
                    message += f"• ... and {len(unique_futures) - 10} more"
                
                # Add exchange summary
                message += f"\n\n🏢 <b>Exchange Summary:</b>\n"
                message += f"• MEXC: {len(self.get_mexc_futures())} futures\n"
                for exchange, count in sorted(exchange_stats.items())[:5]:  # Show top 5
                    message += f"• {exchange}: {count} futures\n"
                if len(exchange_stats) > 5:
                    message += f"• ... and {len(exchange_stats) - 5} more exchanges"
                    
            else:
                message = "✅ <b>Check Complete!</b>\n\n"
                message += "No unique futures found at the moment.\n"
                message += "All MEXC futures are also available on other exchanges."
            
            update.message.reply_html(message)
            
        except Exception as e:
            error_msg = f"❌ <b>Check failed:</b>\n{str(e)}"
            update.message.reply_html(error_msg)
            logger.error(f"Check command error: {e}")
    
    def stats_command(self, update: Update, context: CallbackContext):
        """Show statistics when command /stats is issued."""
        data = self.load_data()
        stats = data.get('statistics', {})
        exchange_stats = data.get('exchange_stats', {})
        
        total_exchanges = len(exchange_stats) + 1  # +1 for MEXC
        
        stats_text = (
            "📈 <b>Bot Statistics</b>\n\n"
            f"🔄 Checks performed: <b>{stats.get('checks_performed', 0)}</b>\n"
            f"🎯 Max unique found: <b>{stats.get('unique_found_total', 0)}</b>\n"
            f"⏰ Current unique: <b>{len(data.get('unique_futures', []))}</b>\n"
            f"🏢 Exchanges monitored: <b>{total_exchanges}</b>\n"
            f"📅 Running since: {self.format_start_time(stats.get('start_time'))}\n"
            f"🤖 Uptime: {self.get_uptime()}\n"
            f"⚡ Auto-check: {self.update_interval} minutes"
        )
        
        update.message.reply_html(stats_text)
    
    def help_command(self, update: Update, context: CallbackContext):
        """Show help information when command /help is issued."""
        help_text = (
            "🆘 <b>Help - MEXC Unique Futures Tracker</b>\n\n"
            "I monitor MEXC exchange for perpetual futures contracts that are NOT available on other major exchanges.\n\n"
            "<b>Supported Exchanges:</b>\n"
            "• MEXC, Binance, Bybit, OKX\n"
            "• Gate.io, KuCoin, BingX, BitGet\n\n"
            "<b>Commands:</b>\n"
            "/start - Welcome message\n"
            "/status - Current status\n" 
            "/check - Immediate check\n"
            "/stats - Bot statistics\n"
            "/exchanges - Exchange info\n"
            "/help - This message\n\n"
            "<b>How it works:</b>\n"
            "• I check all exchanges every 60 minutes\n"
            "• You get notifications for new unique futures\n"
            "• Use /check for immediate verification\n\n"
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
                return dt.strftime("%Y-%m-%d %H:%M:%S")
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
            logger.info("Running scheduled check across all exchanges...")
            
            unique_futures, exchange_stats = self.find_unique_futures()
            current_unique = set(unique_futures)
            
            # Check for changes
            if current_unique != self.last_unique_futures:
                new_futures = current_unique - self.last_unique_futures
                removed_futures = self.last_unique_futures - current_unique
                
                # Update data
                data = self.load_data()
                data['unique_futures'] = list(current_unique)
                data['last_check'] = datetime.now().isoformat()
                data['exchange_stats'] = exchange_stats
                self.save_data(data)
                
                # Send notifications
                if new_futures:
                    message = "🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
                    message += f"🎯 Found <b>{len(new_futures)}</b> new unique futures:\n\n"
                    for symbol in sorted(new_futures):
                        message += f"✅ {symbol}\n"
                    message += f"\n📊 Total unique: {len(current_unique)}"
                    message += f"\n🏢 Monitoring {len(exchange_stats) + 1} exchanges"
                    self.send_broadcast_message(message)
                
                if removed_futures:
                    message = "📉 <b>FUTURES NO LONGER UNIQUE:</b>\n\n"
                    for symbol in sorted(removed_futures):
                        message += f"❌ {symbol}\n"
                    message += f"\n📊 Remaining unique: {len(current_unique)}"
                    self.send_broadcast_message(message)
                
                self.last_unique_futures = current_unique
            
        except Exception as e:
            logger.error(f"Auto-check error: {e}")
    
    def setup_scheduler(self):
        """Setup scheduled tasks"""
        schedule.every(self.update_interval).minutes.do(self.run_auto_check)
        logger.info(f"Scheduler setup - checking every {self.update_interval} minutes")
    
    def run_scheduler(self):
        """Run the scheduler in a separate thread"""
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
            
            # Start scheduler in background thread
            import threading
            scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
            scheduler_thread.start()
            
            # Start the bot
            self.updater.start_polling()
            
            logger.info("Bot started successfully")
            
            # Send startup message
            self.send_broadcast_message(
                "🤖 <b>MEXC Unique Futures Tracker Started</b>\n\n"
                "✅ Monitoring for unique perpetual contracts...\n"
                f"⏰ Auto-check interval: {self.update_interval} minutes\n"
                f"🏢 Monitoring 8 exchanges: Binance, Bybit, OKX, Gate.io, KuCoin, BingX, BitGet\n"
                "💬 Use /help to see available commands"
            )
            
            logger.info("Bot is now running and ready for commands...")
            
            # Keep the main thread running
            self.updater.idle()
            
        except Exception as e:
            logger.error(f"Bot run error: {e}")
            raise

def main():
    tracker = InteractiveFuturesTracker()
    tracker.run()

if __name__ == "__main__":
    print("Starting Interactive MEXC Futures Tracker...")
    main()