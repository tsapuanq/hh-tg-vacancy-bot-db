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

**HH.kz Scraper → AI фильтрация (Gemini) → AI саммари → Supabase DB → Telegram Publisher**

---

## Автоматизация

GitHub Actions запускает пайплайн 3 раза в день:



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
