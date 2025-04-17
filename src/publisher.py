import pandas as pd
import asyncio
from telegram import Bot

# Конфигурация
BOT_TOKEN = "7495201792:AAFxa0l_zJ03PdbkhRSwJxq6g6_LWG69rr4"
CHANNEL_USERNAME = "@dsjobstest"
CSV_PATH = "data/processed/vacancies_clean.csv"

# Функция форматирования
def format_message(row):
    return f"""🌐 *Город:* {row.get('location', '---') or '---'}
📅 *Должность:* {row.get('title', '---') or '---'}
💼 *Компания:* {row.get('company', '---') or '---'}
💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

[Подробнее на HH]({row['link']})"""

# Главная async-функция
async def main():
    bot = Bot(token=BOT_TOKEN)
    df = pd.read_csv(CSV_PATH)
    rows_to_send = df.head(5)

    for _, row in rows_to_send.iterrows():
        text = format_message(row)
        await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")

    print(f"✅ Отправлено {len(rows_to_send)} вакансий")

# Запуск
if __name__ == "__main__":
    asyncio.run(main())
