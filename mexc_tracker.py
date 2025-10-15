import requests
import json
import time
import logging
import os
import asyncio
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FuturesTracker:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.update_interval = int(os.getenv('UPDATE_INTERVAL', 60))
        self.data_file = '/data/data.json'  # Use persistent storage
        
        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
        
        self.bot = Bot(token=self.bot_token)
        self.scheduler = AsyncIOScheduler()
        self.init_data_file()
    
    def init_data_file(self):
        """Initialize JSON data file"""
        # Create /data directory if it doesn't exist
        os.makedirs('/data', exist_ok=True)
        
        if not os.path.exists(self.data_file):
            data = {
                "unique_futures": [],
                "last_update": None,
                "tracking_history": [],
                "statistics": {
                    "total_unique_found": 0,
                    "total_notifications_sent": 0,
                    "first_run": datetime.now().isoformat()
                }
            }
            self.save_data(data)
            logger.info("Created new data file")
    
    def load_data(self):
        """Load data from JSON file"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"unique_futures": [], "last_update": None, "tracking_history": [], "statistics": {}}
    
    def save_data(self, data):
        """Save data to JSON file"""
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    async def send_telegram_message(self, message, silent=False):
        """Send message to Telegram with error handling"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML',
                disable_notification=silent
            )
            logger.info("Telegram message sent")
            return True
        except TelegramError as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    async def send_startup_message(self):
        """Send startup message"""
        message = (
            "🤖 <b>MEXC Unique Futures Tracker Started</b>\n\n"
            "✅ <i>Monitoring for unique perpetual contracts...</i>\n"
            f"⏰ Check interval: {self.update_interval} minutes\n"
            "🚀 Deployed on Fly.io"
        )
        await self.send_telegram_message(message)
    
    async def send_status_message(self):
        """Send current status"""
        data = self.load_data()
        unique_count = len(data.get('unique_futures', []))
        last_update = data.get('last_update', 'Never')
        
        if last_update and last_update != 'Never':
            try:
                last_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                last_update = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        message = (
            "📊 <b>Current Status</b>\n\n"
            f"🔄 Unique futures: <b>{unique_count}</b>\n"
            f"⏰ Last update: {last_update}\n"
            f"🔍 Next check in: {self.update_interval} minutes"
        )
        
        if unique_count > 0:
            message += "\n\n<b>Current unique futures:</b>\n"
            for symbol in sorted(data['unique_futures'])[:5]:
                message += f"• {symbol}\n"
            if unique_count > 5:
                message += f"... and {unique_count - 5} more"
        
        await self.send_telegram_message(message)
    
    def get_mexc_futures(self):
        """Get futures from MEXC"""
        try:
            url = "https://contract.mexc.com/api/v1/contract/detail"
            response = requests.get(url, timeout=30)
            data = response.json()
            
            futures = []
            for contract in data.get('data', []):
                symbol = contract.get('symbol', '')
                if symbol and symbol.endswith('_USDT'):
                    futures.append(symbol)
            
            logger.info(f"Found {len(futures)} MEXC futures")
            return set(futures)
        except Exception as e:
            logger.error(f"MEXC error: {e}")
            return set()
    
    def get_binance_futures(self):
        """Get futures from Binance"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=30)
            data = response.json()
            
            futures = [s['symbol'] for s in data['symbols'] if s['contractType'] == 'PERPETUAL']
            return set(futures)
        except Exception as e:
            logger.error(f"Binance error: {e}")
            return set()
    
    def get_bybit_futures(self):
        """Get futures from Bybit"""
        try:
            url = "https://api.bybit.com/v2/public/symbols"
            response = requests.get(url, timeout=30)
            data = response.json()
            
            futures = [item['name'] for item in data.get('result', [])]
            return set(futures)
        except Exception as e:
            logger.error(f"Bybit error: {e}")
            return set()
    
    def get_other_exchanges_futures(self):
        """Get futures from other exchanges"""
        exchanges = {
            'binance': self.get_binance_futures,
            'bybit': self.get_bybit_futures
        }
        
        all_futures = set()
        for name, method in exchanges.items():
            try:
                futures = method()
                all_futures.update(futures)
                logger.info(f"{name}: {len(futures)} futures")
            except Exception as e:
                logger.error(f"Exchange {name} error: {e}")
        
        return all_futures
    
    def normalize_symbol(self, symbol):
        """Normalize symbol for comparison"""
        return symbol.upper().replace('_USDT', '').replace('USDT', '').replace('-', '').replace('_', '')
    
    def find_unique_futures(self):
        """Find unique futures on MEXC"""
        mexc_futures = self.get_mexc_futures()
        other_futures = self.get_other_exchanges_futures()
        
        if not mexc_futures:
            return set()
        
        mexc_normalized = {self.normalize_symbol(s): s for s in mexc_futures}
        other_normalized = {self.normalize_symbol(s) for s in other_futures}
        
        unique_futures = set()
        for normalized, original in mexc_normalized.items():
            if normalized not in other_normalized:
                unique_futures.add(original)
        
        logger.info(f"Found {len(unique_futures)} unique futures")
        return unique_futures
    
    async def check_for_changes(self):
        """Check for changes and send notifications"""
        try:
            data = self.load_data()
            current_unique = set(data.get('unique_futures', []))
            new_unique = self.find_unique_futures()
            
            added = new_unique - current_unique
            removed = current_unique - new_unique
            
            # Update statistics
            stats = data.get('statistics', {})
            stats['total_unique_found'] = stats.get('total_unique_found', 0) + len(added)
            stats['total_notifications_sent'] = stats.get('total_notifications_sent', 0) + len(added) + len(removed)
            stats['last_run'] = datetime.now().isoformat()
            
            # Update data
            data['unique_futures'] = list(new_unique)
            data['last_update'] = datetime.now().isoformat()
            data['statistics'] = stats
            
            # Add to history
            for symbol in added:
                data['tracking_history'].append({
                    'symbol': symbol,
                    'event': 'added',
                    'timestamp': datetime.now().isoformat()
                })
            
            for symbol in removed:
                data['tracking_history'].append({
                    'symbol': symbol,
                    'event': 'removed', 
                    'timestamp': datetime.now().isoformat()
                })
            
            # Save data
            self.save_data(data)
            
            # Send notifications
            if added:
                message = "🚀 <b>NEW UNIQUE FUTURES FOUND!</b>\n\n"
                for symbol in sorted(added):
                    message += f"✅ {symbol}\n"
                message += f"\n📊 Total unique: {len(new_unique)}"
                await self.send_telegram_message(message)
            
            if removed:
                message = "📉 <b>FUTURES NO LONGER UNIQUE</b>\n\n"
                for symbol in sorted(removed):
                    message += f"❌ {symbol}\n"
                message += f"\n📊 Remaining unique: {len(new_unique)}"
                await self.send_telegram_message(message)
            
            if not added and not removed:
                logger.info("No changes detected")
            else:
                logger.info(f"Changes: +{len(added)}, -{len(removed)}")
                
        except Exception as e:
            logger.error(f"Error in check_for_changes: {e}")
            await self.send_telegram_message(f"❌ Error during check: {str(e)}")
    
    async def start_scheduler(self):
        """Start the scheduled tasks"""
        # Add the main check job
        self.scheduler.add_job(
            self.check_for_changes,
            trigger='interval',
            minutes=self.update_interval,
            id='main_check'
        )
        
        # Add status report every 6 hours
        self.scheduler.add_job(
            self.send_status_message,
            trigger='interval',
            hours=6,
            id='status_report'
        )
        
        self.scheduler.start()
        logger.info("Scheduler started")
    
    async def run(self):
        """Main run method"""
        try:
            # Send startup message
            await self.send_startup_message()
            
            # Initial check
            await self.check_for_changes()
            
            # Start scheduler
            await self.start_scheduler()
            
            logger.info("Bot is now running...")
            
            # Keep the app running
            while True:
                await asyncio.sleep(3600)  # Sleep for 1 hour
                
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise

async def main():
    tracker = FuturesTracker()
    await tracker.run()

if __name__ == "__main__":
    print("Starting MEXC Futures Tracker...")
    asyncio.run(main())