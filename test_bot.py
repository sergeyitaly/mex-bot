import os
from dotenv import load_dotenv
from telegram import Bot

load_dotenv()

def test_bot():
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("❌ Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env file")
        return
    
    try:
        bot = Bot(token=token)
        
        # Test bot info
        bot_info = bot.get_me()
        print(f"✅ Bot connected: @{bot_info.username}")
        
        # Test message
        bot.send_message(
            chat_id=chat_id,
            text="🤖 <b>Bot Test Successful!</b>\n\nYour MEXC tracker is ready to use.",
            parse_mode='HTML'
        )
        print("✅ Test message sent!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_bot()