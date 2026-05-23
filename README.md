#  **HH → Telegram Vacancy Bot**

Автоматический конвейер, который собирает **Data-вакансии с HH.kz**, фильтрует их с помощью **AI (Gemini)** и публикует в **Telegram**.  
Все работает полностью автономно.

**Канал:** [@KZDataJobs](https://t.me/KZDataJobs)

---

## Что делает бот

1. **Собирает** вакансии по ролям Data Scientist / Analyst / Engineer и др.  
2. **Фильтрует** нерелевантные с помощью Google Gemini.  
3. **Создает саммари** (о компании, обязанностях, требованиях).  
4. **Сохраняет** в PostgreSQL (Supabase).  
5. **Публикует** в Telegram автоматически.

---

## Технологии

| Задача | Технология |
|--------|-------------|
| Скрейпинг | Playwright |
| AI-анализ | Google Gemini |
| База данных | Supabase (PostgreSQL) |
| Telegram | python-telegram-bot |
| CI/CD | GitHub Actions |
| Язык | Python |

---

## Алгоритм работы

**HH.kz Scraper → AI фильтрация → AI саммари → Supabase DB → Telegram Publisher**

Дополнительный источник:

**Telegram source channel → GPT фильтрация → Supabase DB (`telegram_posts`) → Telegram Publisher**

---

## Автоматизация

GitHub Actions запускает пайплайн 3 раза в день:

1. `python run_all.py` — основной HH-пайплайн.
2. `python run_telegram.py` — Telegram source pipeline.

Для Telegram source pipeline нужны secrets:

```text
TELEGRAM_API_ID
TELEGRAM_API_HASH
TELEGRAM_SOURCE_CHANNEL
TELEGRAM_SESSION_STRING
```

`TELEGRAM_SESSION_STRING` создаётся локально:

```bash
python scripts/export_telegram_session.py
```

Для безопасной проверки БД без публикации:

```bash
python run_telegram.py --no-publish
```

Настройки по умолчанию:

```text
TELEGRAM_LOOKBACK_DAYS=2
TELEGRAM_SOURCE_LIMIT=50
TELEGRAM_PROCESS_LIMIT=20
```

Пайплайн читает посты за последние 2 дня, но повторно не отправляет уже
обработанные `source_url`: дедупликация хранится в PostgreSQL. Один пост обычно
стоит примерно 1.2k-2k input tokens и 200-500 output tokens; лимит 20 новых
постов даёт ориентир около 25k-40k input tokens за худший запуск. Ссылка на
исходный Telegram-канал используется только внутри БД для дедупликации и не
публикуется в итоговом сообщении.



---

## Результат

✅ Только релевантные вакансии  
✅ Полностью автономный процесс  
✅ Удобные AI-описания  
✅ Хранение данных для аналитики

---

## Автор

**Sapsan Talaspay**  
📍 Data Science @ SDU  
📢 [@KZDataJobs](https://t.me/KZDataJobs) | 💬 [@tsapuanq](https://t.me/tsapuanq)
