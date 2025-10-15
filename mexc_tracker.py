import requests
import json
import logging
import os
import asyncio
import time
from datetime import datetime
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
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
        
        self.application = Application.builder().token(self.bot_token).build()
        self.setup_handlers()
        self.init_data_file()
    
    def setup_handlers(self):
        """Setup command handlers"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("check", self.check_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
    
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
                }
            }
            self.save_data(data)
    
    def load_data(self):
        """Load data from JSON file"""
        try:
            with open(self.data_file, 'r') as f:
                return json.load(f)
        except:
            return {"unique_futures": [], "last_check": None, "statistics": {}}
    
    def save_data(self, data):
        """Save data to JSON file"""
        with open(self.data_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message when command /start is issued."""
        user = update.effective_user
        welcome_text = (
            f"🤖 Hello {user.mention_html()}!\n\n"
            "I'm <b>MEXC Unique Futures Tracker</b>\n"
            "I monitor for perpetual contracts that are available on MEXC but not on other major exchanges.\n\n"
            "<b>Available commands:</b>\n"
            "/start - Show this welcome message\n"
            "/status - Check current status\n"
            "/check - Perform immediate check\n"
            "/stats - Show statistics\n"
            "/help - Show help information"
        )
        await update.message.reply_html(welcome_text)
        logger.info(f"Start command received from user: {user.id}")
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send current status when command /status is issued."""
        data = self.load_data()
        unique_count = len(data.get('unique_futures', []))
        last_check = data.get('last_check', 'Never')
        
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
            for symbol in sorted(data['unique_futures'])[:8]:  # Show first 8
                status_text += f"• {symbol}\n"
            if unique_count > 8:
                status_text += f"• ... and {unique_count - 8} more"
        
        await update.message.reply_html(status_text)
    
    async def check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Perform immediate check when command /check is issued."""
        await update.message.reply_html("🔍 <b>Performing immediate check...</b>")
        
        try:
            unique_futures = await self.find_unique_futures()
            data = self.load_data()
            
            # Update statistics
            stats = data.get('statistics', {})
            stats['checks_performed'] = stats.get('checks_performed', 0) + 1
            stats['unique_found_total'] = max(stats.get('unique_found_total', 0), len(unique_futures))
            
            # Update data
            data['unique_futures'] = list(unique_futures)
            data['last_check'] = datetime.now().isoformat()
            data['statistics'] = stats
            self.save_data(data)
            
            if unique_futures:
                message = "✅ <b>Check Complete!</b>\n\n"
                message += f"🎯 Found <b>{len(unique_futures)}</b> unique futures:\n\n"
                for symbol in sorted(unique_futures)[:10]:
                    message += f"• {symbol}\n"
                if len(unique_futures) > 10:
                    message += f"• ... and {len(unique_futures) - 10} more"
            else:
                message = "✅ <b>Check Complete!</b>\n\nNo unique futures found at the moment."
            
            await update.message.reply_html(message)
            
        except Exception as e:
            error_msg = f"❌ <b>Check failed:</b>\n{str(e)}"
            await update.message.reply_html(error_msg)
            logger.error(f"Check command error: {e}")
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show statistics when command /stats is issued."""
        data = self.load_data()
        stats = data.get('statistics', {})
        
        stats_text = (
            "📈 <b>Bot Statistics</b>\n\n"
            f"🔄 Checks performed: <b>{stats.get('checks_performed', 0)}</b>\n"
            f"🎯 Max unique found: <b>{stats.get('unique_found_total', 0)}</b>\n"
            f"⏰ Current unique: <b>{len(data.get('unique_futures', []))}</b>\n"
            f"📅 Running since: {self.format_start_time(stats.get('start_time'))}\n"
            f"🤖 Uptime: {self.get_uptime()}\n"
            f"⚡ Auto-check: {self.update_interval} minutes"
        )
        
        await update.message.reply_html(stats_text)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help information when command /help is issued."""
        help_text = (
            "🆘 <b>Help - MEXC Unique Futures Tracker</b>\n\n"
            "I monitor MEXC exchange for perpetual futures contracts that are NOT available on other major exchanges like Binance, Bybit, etc.\n\n"
            "<b>Commands:</b>\n"
            "/start - Welcome message\n"
            "/status - Current status and unique futures\n"
            "/check - Perform immediate check\n"
            "/stats - Bot statistics\n"
            "/help - This help message\n\n"
            "<b>How it works:</b>\n"
            "• I automatically check every 60 minutes\n"
            "• You'll get notifications when new unique futures are found\n"
            "• Use /check for immediate verification\n\n"
            "⚡ <i>Happy trading!</i>"
        )
        await update.message.reply_html(help_text)
    
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
    
    async def find_unique_futures(self):
        """Find unique futures on MEXC"""
        try:
            # Get MEXC futures
            mexc_url = "https://contract.mexc.com/api/v1/contract/detail"
            mexc_response = requests.get(mexc_url, timeout=30)
            mexc_data = mexc_response.json()
            
            mexc_futures = set()
            for contract in mexc_data.get('data', []):
                symbol = contract.get('symbol', '')
                if symbol and symbol.endswith('_USDT'):
                    mexc_futures.add(symbol)
            
            logger.info(f"Found {len(mexc_futures)} MEXC futures")
            
            # Get Binance futures
            binance_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            binance_response = requests.get(binance_url, timeout=30)
            binance_data = binance_response.json()
            
            binance_futures = set()
            for symbol_data in binance_data.get('symbols', []):
                if symbol_data.get('contractType') == 'PERPETUAL':
                    binance_futures.add(symbol_data['symbol'])
            
            logger.info(f"Found {len(binance_futures)} Binance futures")
            
            # Normalize and compare
            def normalize(symbol):
                return symbol.upper().replace('_USDT', '').replace('USDT', '').replace('-', '').replace('_', '')
            
            mexc_normalized = {normalize(s): s for s in mexc_futures}
            binance_normalized = {normalize(s) for s in binance_futures}
            
            unique_futures = set()
            for normalized, original in mexc_normalized.items():
                if normalized not in binance_normalized:
                    unique_futures.add(original)
            
            logger.info(f"Found {len(unique_futures)} unique futures")
            return unique_futures
            
        except Exception as e:
            logger.error(f"Error finding unique futures: {e}")
            return set()
    
    async def send_broadcast_message(self, message):
        """Send message to all users (for notifications)"""
        try:
            # This would need to be implemented based on how you track users
            # For now, just send to the configured chat
            if self.chat_id:
                await self.application.bot.send_message(
                    chat_id=self.chat_id,
                    text=message,
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
    
    async def run_auto_checks(self):
        """Run automatic checks in background"""
        while True:
            try:
                await asyncio.sleep(self.update_interval * 60)
                logger.info("Running scheduled check...")
                
                unique_futures = await self.find_unique_futures()
                data = self.load_data()
                previous_futures = set(data.get('unique_futures', []))
                
                if unique_futures != previous_futures:
                    # Update data
                    data['unique_futures'] = list(unique_futures)
                    data['last_check'] = datetime.now().isoformat()
                    self.save_data(data)
                    
                    # Send notification if new futures found
                    new_futures = unique_futures - previous_futures
                    if new_futures:
                        message = "🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
                        for symbol in sorted(new_futures):
                            message += f"✅ {symbol}\n"
                        message += f"\n📊 Total unique: {len(unique_futures)}"
                        await self.send_broadcast_message(message)
                
            except Exception as e:
                logger.error(f"Auto-check error: {e}")
    
    async def run(self):
        """Start the bot"""
        try:
            # Start auto-check background task
            asyncio.create_task(self.run_auto_checks())
            
            # Start the bot
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            logger.info("Bot started successfully")
            
            # Send startup message
            if self.chat_id:
                await self.send_broadcast_message(
                    "🤖 <b>MEXC Unique Futures Tracker Started</b>\n\n"
                    "✅ Monitoring for unique perpetual contracts...\n"
                    f"⏰ Auto-check interval: {self.update_interval} minutes\n"
                    "💬 Use /help to see available commands"
                )
            
            # Keep running
            while True:
                await asyncio.sleep(3600)
                
        except Exception as e:
            logger.error(f"Bot run error: {e}")
            raise
        finally:
            await self.application.stop()

async def main():
    tracker = InteractiveFuturesTracker()
    await tracker.run()

if __name__ == "__main__":
    print("Starting Interactive MEXC Futures Tracker...")
    asyncio.run(main())