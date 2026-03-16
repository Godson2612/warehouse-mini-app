from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonWebApp, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes
import os

BOT_TOKEN = os.environ["8225104783:AAGsMLrMPYHm9lreO54-MiAZfuT0EfuV8IY"]
WEB_APP_URL = os.environ["https://warehouse-mini-app.onrender.com"]  # ejemplo: https://tu-app.onrender.com

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Open Equipment Request App", web_app=WebAppInfo(url=WEB_APP_URL))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Welcome. Use the button below to open the Equipment Request Mini App.",
        reply_markup=reply_markup
    )

async def set_menu(app: Application):
    await app.bot.set_chat_menu_button(
        menu_button=MenuButtonWebApp(
            text="Open App",
            web_app=WebAppInfo(url=WEB_APP_URL)
        )
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.post_init = set_menu
    app.run_polling()

if __name__ == "__main__":
    main()
