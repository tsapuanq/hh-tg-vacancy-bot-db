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


# import pandas as pd
# import asyncio
# from telegram import Bot
# from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv

# def format_message(row):
#     return f"""🌐 *Город:* {row.get('location', '---')}
# 📅 *Должность:* {row.get('title', '---')}
# 💼 *Компания:* {row.get('company', '---')}
# 💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

# 🧾 *Обязанности:* {row.get('responsibilities', 'Не указано')}
# 🎯 *Требования:* {row.get('requirements', 'Не указано')}
# 🏢 *О компании:* {row.get('about_company', 'Не указано')}

# [Подробнее на HH]({row['link']})
# """

# async def main():
#     csv_path = get_today_processed_csv()
#     df = pd.read_csv(csv_path)

#     # Только первые 5 строк — тест
#     rows_to_send = df.head(5)

#     if rows_to_send.empty:
#         print("❌ Нет данных для отправки.")
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
import os
from telegram import Bot
from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv

def format_message(row):
    return f"""🌐 *Город:* {row.get('location', '---')}
📅 *Должность:* {row.get('title', '---')}
💼 *Компания:* {row.get('company', '---')}
💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}
💰 *Опыт:* {row.get('experience') or '---'}
💰 *Тип занятости:* {row.get('employment_type') or '---'}
💰 *График:* {row.get('schedule') or '---'}
💰 *Рабочие часы:* {row.get('working_hours') or '---'}
💰 *Формат работы:* {row.get('work_format') or '---'}
💰 *Формат работы:* {row.get('published_date') or '---'}
💰 *Формат работы:* {row.get('skills') or '---'}

🧾 *Обязанности:* {row.get('responsibilities', 'Не указано')}
🎯 *Требования:* {row.get('requirements', 'Не указано')}
🏢 *О компании:* {row.get('about_company', 'Не указано')}

[Подробнее на HH]({row['link']})
"""

def load_csv_safe(path):
    if not os.path.exists(path):
        print(f"❌ CSV файл не найден: {path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
        if df.empty:
            print(f"❌ CSV пустой: {path}")
        return df
    except pd.errors.EmptyDataError:
        print(f"❌ CSV файл пуст или повреждён: {path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return pd.DataFrame()

async def main():
    csv_path = get_today_processed_csv()
    df = load_csv_safe(csv_path)

    if df.empty:
        print("ℹ️ Нет данных для отправки. Пропуск публикации.")
        return

    bot = Bot(token=BOT_TOKEN)
    for _, row in df.head(5).iterrows():
        text = format_message(row)
        await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")

    print(f"✅ Отправлено {len(df.head(5))} вакансий.")

def run_publisher():
    return main()  # ✅ Возвращаем корутину (НЕ вызываем asyncio.run!)
