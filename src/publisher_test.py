# import pandas as pd
# import asyncio
# import os
# import random
# from telegram import Bot
# from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv
# from src.llm_summary import summarize_description_llm

# # ——— Красивое форматирование вакансии ———
# def format_message(row: pd.Series, summary: dict) -> str:
#     return f"""
# 🌐 *Город:* {row.get('location', '---')}
# 📅 *Должность:* {row.get('title', '---')}
# 💼 *Компания:* {row.get('company', '---')}
# 💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

# 🕓 *Опыт:* {row.get('experience', '---')}
# 🗂 *Тип занятости:* {row.get('employment_type', '---')}
# 📆 *График:* {row.get('schedule', '---')}
# ⏰ *Рабочие часы:* {row.get('working_hours', '---')}
# 🏠 *Формат работы:* {row.get('work_format', '---')}
# 📅 *Дата публикации:* {row.get('published_date', '---')}

# 🧾 *Обязанности:*
# {summary.get('responsibilities', 'Не указано')}

# 🎯 *Требования:*
# {summary.get('requirements', 'Не указано')}

# 🏢 *О компании:*
# {summary.get('about_company', 'Не указано')}

# 👉 [Подробнее на HH]({row['link']})
# """.strip()

# # ——— Безопасная загрузка CSV ———
# def load_csv_safe(path: str) -> pd.DataFrame:
#     if not os.path.exists(path):
#         print(f"❌ CSV файл не найден: {path}")
#         return pd.DataFrame()
#     try:
#         df = pd.read_csv(path)
#         return df if not df.empty else pd.DataFrame()
#     except Exception as e:
#         print(f"❌ Ошибка чтения CSV: {e}")
#         return pd.DataFrame()

# # ——— Основная логика отправки в Telegram ———
# async def main():
#     csv_path = get_today_processed_csv()
#     df = load_csv_safe(csv_path)

#     if df.empty:
#         print("Нет данных для отправки. Пропуск публикации.")
#         return

#     bot = Bot(token=BOT_TOKEN)

#     rows_to_send = df.sort_values("published_date_dt", ascending=False).head(5)

#     for i, (_, row) in enumerate(rows_to_send.iterrows(), 1):
#         summary = summarize_description_llm(row["description"])
#         print("[DEBUG Summary]", summary)
#         text = format_message(row, summary)
#         await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")
#         print(f"✅ [{i}/{len(rows_to_send)}] Вакансия отправлена.")

#         if i < len(rows_to_send):
#             delay = random.uniform(3, 10)
#             print(f"⏱️ Задержка перед следующей: {delay:.2f} сек.")
#             await asyncio.sleep(delay)

#     print(f"\n📬 Всего отправлено: {len(rows_to_send)} вакансий.")

# def run_publisher():
#     return main()
import pandas as pd
import asyncio
import os
import random
from datetime import datetime
from telegram import Bot
from src.config import BOT_TOKEN, CHANNEL_USERNAME, get_today_processed_csv
from src.llm_summary import summarize_description_llm, filter_vacancy_llm

# ——— Красивое форматирование вакансии ———
def format_message(row: pd.Series, summary: dict) -> str:
    return f"""
🌐 *Город:* {row.get('location', '---')}
📅 *Должность:* {row.get('title', '---')}
💼 *Компания:* {row.get('company', '---')}
💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

🎓 *Опыт:* {row.get('experience', '---')}
📂 *Тип занятости:* {row.get('employment_type', '---')}
📆 *График:* {row.get('schedule', '---')}
⏰ *Рабочие часы:* {row.get('working_hours', '---')}
🏠 *Формат работы:* {row.get('work_format', '---')}
📅 *Дата публикации:* {row.get('published_date', '---')}

💾 *Обязанности:*
{summary.get('responsibilities', 'Не указано')}

🌟 *Требования:*
{summary.get('requirements', 'Не указано')}

🏢 *О компании:*
{summary.get('about_company', 'Не указано')}

👉 [Подробнее на HH]({row['link']})
""".strip()

# ——— Загрузка CSV со строкой проверки сегодняшней даты ———
def load_any_rows() -> pd.DataFrame:
    csv_path = get_today_processed_csv()
    if not os.path.exists(csv_path):
        print(f"❌ CSV не найден: {csv_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_path)
        # today_str = datetime.now().strftime("%Y-%m-%d")
        # df = df[df["published_date_dt"] == today_str]
        return df.head(5)
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return pd.DataFrame()
    
def load_today_rows() -> pd.DataFrame:
    csv_path = get_today_processed_csv()
    if not os.path.exists(csv_path):
        print(f"❌ CSV не найден: {csv_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_path)
        today_str = datetime.now().strftime("%Y-%m-%d")
        df = df[df["published_date_dt"] == today_str]
        return df.head(5)
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return pd.DataFrame()

# ——— Основной пайплайн с LLM фильтром и отправкой в Telegram ———
async def main():
    df = load_any_rows()
    if df.empty:
        print("ℹ️ Сегодня нет новых вакансий.")
        return

    filtered = []
    for i, (_, row) in enumerate(df.iterrows(), 1):
        is_relevant = filter_vacancy_llm(row["title"], row["description"])
        print(f"[Gemini Filter] {row['title']} → {'✅' if is_relevant else '❌'}")

        if is_relevant:
            filtered.append(row)

        await asyncio.sleep(4.5)  # лимит Gemini API

    if not filtered:
        print("❌ Нет релевантных вакансий после фильтрации.")
        return

    df_filtered = pd.DataFrame(filtered).head(5)
    bot = Bot(token=BOT_TOKEN)

    for i, (_, row) in enumerate(df_filtered.iterrows(), 1):
        summary = summarize_description_llm(row["description"])
        text = format_message(row, summary)
        await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")
        print(f"✅ [{i}/{len(df_filtered)}] Вакансия отправлена.")

        if i < len(df_filtered):
            delay = random.uniform(3, 10)
            print(f"⏱️ Задержка перед следующей: {delay:.2f} сек.")
            await asyncio.sleep(delay)

    print(f"\n📬 Всего отправлено: {len(df_filtered)} вакансий.")

def run_publisher():
    return main()
