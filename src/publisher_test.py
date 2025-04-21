# import pandas as pd
# import asyncio
# from telegram import Bot
# from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv

# def format_message(row):
#     return f"""🌐 *Город:* {row.get('location', '---')}
# 📅 *Должность:* {row.get('title', '---')}
# 💼 *Компания:* {row.get('company', '---')}
# 💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

# [Подробнее на HH]({row['link']})"""

# async def main():
#     csv_path = get_today_processed_csv()
#     df = pd.read_csv(csv_path)

#     # Берем просто первые 5 для теста
#     rows_to_send = df.head(5)

#     if rows_to_send.empty:
#         print("Нет данных для отправки.")
#         return

#     bot = Bot(token=BOT_TOKEN)
#     for _, row in rows_to_send.iterrows():
#         text = format_message(row)
#         await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")

#     print(f"✅ Тест: отправлено {len(rows_to_send)} вакансий.")

# def run_publisher():
#     asyncio.run(main())

# if __name__ == "__main__":
#     run_publisher()


import pandas as pd
import asyncio
from telegram import Bot
from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv

def format_message(row):
    return f"""🌐 *Город:* {row.get('location', '---')}
📅 *Должность:* {row.get('title', '---')}
💼 *Компания:* {row.get('company', '---')}
💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

🧾 *Обязанности:* {row.get('responsibilities', 'Не указано')}
🎯 *Требования:* {row.get('requirements', 'Не указано')}
🏢 *О компании:* {row.get('about_company', 'Не указано')}

[Подробнее на HH]({row['link']})
"""

async def main():
    csv_path = get_today_processed_csv()
    df = pd.read_csv(csv_path)

    # Только первые 5 строк — тест
    rows_to_send = df.head(5)

    if rows_to_send.empty:
        print("❌ Нет данных для отправки.")
        return

    bot = Bot(token=BOT_TOKEN)
    for _, row in rows_to_send.iterrows():
        text = format_message(row)
        await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")

    print(f"✅ Тест: отправлено {len(rows_to_send)} вакансий.")

def run_publisher():
    asyncio.run(main())

if __name__ == "__main__":
    run_publisher()
