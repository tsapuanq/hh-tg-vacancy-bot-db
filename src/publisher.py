import pandas as pd
import asyncio
import random
import ast
from telegram import Bot
from src.config import TELEGRAM_BOT_TOKEN, CHANNEL_USERNAME
from src.llm_summary import summarize_description_llm, filter_vacancy_llm
from src.config import SENT_IDS_PATH, SENT_LINKS_PATH
from src.utils import get_today_processed_csv
from src.utils import load_sent_ids
from src.utils import append_sent_ids
from src.utils import extract_vacancy_id
from src.utils import load_sent_links
from src.utils import append_sent_links
from src.utils import load_today_rows

# ——— Вспомогательная конверсия LLM-результатов в Markdown-буллеты ———
def _to_bullets(x) -> str:
    if isinstance(x, list):
        lines = x
    else:
        try:
            parsed = ast.literal_eval(x)
            lines = parsed if isinstance(parsed, list) else [x]
        except Exception:
            lines = [x]
    bullets = []
    for item in lines:
        s = str(item).strip().strip("'\"")
        if s:
            bullets.append(f"• {s}")
    return "\n".join(bullets) or "Не указано"

# ——— Форматирование вакансии для Telegram ———
def format_message(row: pd.Series, summary: dict) -> str:
    title = f"**{row.get('title','---')}**"
    pub_date = f"**{row.get('published_date','---')}**"
    resp = _to_bullets(summary.get('responsibilities', 'Не указано'))
    reqs = _to_bullets(summary.get('requirements', 'Не указано'))
    about = str(summary.get('about_company', 'Не указано')).strip().strip("'\"")

    return f"""
🌐 *Город:* {row.get('location', '---')}
📅 *Должность:* {title}
💼 *Компания:* {row.get('company', '---')}
💰 *ЗП:* {row.get('salary_range') or row.get('salary') or '---'}

🎓 *Опыт:* {row.get('experience', '---')}
📂 *Тип занятости:* {row.get('employment_type', '---')}
📆 *График:* {row.get('schedule', '---')}
🕒 *Рабочие часы:* {row.get('working_hours', '---')}
🏠 *Формат работы:* {row.get('work_format', '---')}
📅 *Дата публикации:* {pub_date}

🧾 *Обязанности:*
{resp}

🎯 *Требования:*
{reqs}

🏢 *О компании:*
{about}

🔎 [Подробнее на hh]({row['link']})
""".strip()

# ——— Основной пайплайн публикации вакансий ———
async def main():
    df = load_today_rows()
    if df.empty:
        print("ℹ️ Сегодня нет новых вакансий.")
        return

    sent_links = load_sent_links()
    sent_ids = load_sent_ids()

    # Фильтруем по ссылкам
    df = df[~df["link"].isin(sent_links)]

    # Фильтруем дополнительно по vacancy_id
    df["vacancy_id"] = df["link"].apply(lambda x: extract_vacancy_id(x))
    df = df[~df["vacancy_id"].isin(sent_ids)]

    if df.empty:
        print("ℹ️ Все вакансии уже были отправлены ранее.")
        return

    # Фильтрация через Gemini LLM
    filtered = []
    for i, (_, row) in enumerate(df.iterrows(), 1):
        is_relevant = filter_vacancy_llm(row["title"], row["description"])
        print(f"[Gemini Filter] {row['title']} → {'✅' if is_relevant else '❌'}")
        if is_relevant:
            filtered.append(row)
        await asyncio.sleep(4.5)  # лимит 15 rpm

    if not filtered:
        print("❌ Нет релевантных вакансий после фильтрации.")
        return

    df_filtered = pd.DataFrame(filtered)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    for i, (_, row) in enumerate(df_filtered.iterrows(), 1):
        vacancy_id = row["vacancy_id"]

        if vacancy_id in sent_ids:
            print(f"⏭️ Вакансия {vacancy_id} уже отправлена. Пропускаем.")
            continue

        summary = summarize_description_llm(row["description"])
        text = format_message(row, summary)
        await bot.send_message(chat_id=CHANNEL_USERNAME, text=text, parse_mode="Markdown")
        print(f"✅ [{i}/{len(df_filtered)}] Вакансия отправлена.")

        sent_ids.add(vacancy_id)  # Добавляем ID в множество сразу после отправки

        if i < len(df_filtered):
            delay = random.uniform(3, 10)
            print(f"⏱️ Задержка перед следующей: {delay:.2f} сек.")
            await asyncio.sleep(delay)

            print(f"✅ [{i}/{len(df_filtered)}] Вакансия отправлена.")
            if i < len(df_filtered):
                delay = random.uniform(3, 10)
                print(f"⏱️ Задержка перед следующей: {delay:.2f} сек.")
                await asyncio.sleep(delay)

    append_sent_links(df_filtered["link"].tolist())
    append_sent_ids(df_filtered["vacancy_id"].tolist())

    print(f"\n📬 Всего отправлено: {len(df_filtered)} вакансий.")

def run_publisher():
    return main()
