# #scraper.py
# import logging
# from src.cleaning import parse_russian_date, clean_text_safe 
# from datetime import datetime

# async def get_vacancy_details(link: str, page) -> dict | None:
#     try:
#         await page.goto(link, timeout=20000, wait_until="domcontentloaded")
#         await page.wait_for_selector('h1[data-qa="vacancy-title"]', timeout=15000)
#     except Exception as e:
#         logging.warning(f"[Vacancy Load] ❌ Не удалось загрузить {link}: {e}")
#         return None

#     async def clean(selector: str, default="Не указано"):
#         try:
#             text = await page.inner_text(selector)
#             return clean_text_safe(text)
#         except Exception:
#             return default

#     try:
#         # Извлекаем дату публикации и преобразуем в datetime
#         # published_date_raw = await clean("p.vacancy-creation-time-redesigned span")
#         # published_at = (
#         #     datetime.strptime(parse_russian_date(published_date_raw), "%Y-%m-%d")
#         #     if published_date_raw != "Не указано" and parse_russian_date(published_date_raw) != "Не указано"
#         #     else None
#         # )

#         # Нормализация URL: оставляем только базовый путь
#         normalized_url = link.split("?")[0]  # Убираем параметры

#         data = {
#             "title": await clean('h1[data-qa="vacancy-title"]'),
#             "company": await clean('a[data-qa="vacancy-company-name"]'),
#             "location": await clean('p.vacancy-creation-time-redesigned'),  # Содержит дату и город
#             "salary": await clean('span[data-qa="vacancy-salary-compensation-type-net"]'),
#             "description": await clean('div[data-qa="vacancy-description"]'),
#             "experience": await clean('span[data-qa="vacancy-experience"]'),
#             "employment_type": await clean('div[data-qa="common-employment-text"]'),
#             "schedule": await clean('p[data-qa="work-schedule-by-days-text"]'),
#             "working_hours": await clean('div[data-qa="working-hours-text"]'),
#             "work_format": await clean('p[data-qa="work-formats-text"]'),
#             "published_date": await clean("p.vacancy-creation-time-redesigned span"),
#             "link": normalized_url,  # Нормализованный URL
#         }

#         skills_selector = '[data-qa="skills-element"]'
#         skills_elements = page.locator(skills_selector)
#         raw_skills = (
#             await skills_elements.all_inner_texts()
#             if await skills_elements.count() > 0
#             else []
#         )
#         data["skills"] = (
#             clean_text_safe(",".join([s.strip() for s in raw_skills]))
#             if raw_skills
#             else "Не указано"
#         )

#         logging.info(f"[Vacancy Parsed] ✅ {data['title']} — {data['company']}")
#         return data

#     except Exception as e:
#         logging.warning(f"[Vacancy Parse] ❌ Ошибка при парсинге {link}: {e}")
#         return None



# scraper.py
import logging
from datetime import datetime, date
# Импортируем чистящие функции
from src.cleaning import (
    clean_text_safe, parse_russian_date, extract_city, normalize_city_name,
    extract_salary_range_with_currency, clean_skills, clean_working_hours, clean_work_format, clean_schedule
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from src.config import SCRAPER_TIMEOUT_GOTO, SCRAPER_TIMEOUT_SELECTOR


async def get_vacancy_details(link: str, page) -> dict | None:
    """
    Переходит по ссылке вакансии, извлекает и очищает детали.
    """
    try:
        # Переход на страницу и ожидание загрузки основного контента
        await page.goto(link, timeout=SCRAPER_TIMEOUT_GOTO, wait_until="domcontentloaded")
        # Ожидание ключевого селектора, подтверждающего загрузку страницы вакансии
        await page.wait_for_selector('h1[data-qa="vacancy-title"]', timeout=SCRAPER_TIMEOUT_SELECTOR)
    except PlaywrightTimeoutError:
         logging.warning(f"[Vacancy Load] ⏳ Таймаут при загрузке или ожидании селектора для {link}")
         return None
    except Exception as e:
        logging.warning(f"[Vacancy Load] ❌ Не удалось загрузить {link}: {e}")
        return None

    async def safe_inner_text(selector: str, default="Не указано") -> str:
        """
        Безопасно извлекает inner_text, применяет clean_text_safe и обрабатывает ошибки.
        """
        try:
            text = await page.inner_text(selector)
            return clean_text_safe(text)
        except PlaywrightTimeoutError:
             # Таймаут ожидания селектора внутри safe_inner_text
             # logging.debug(f"Таймаут ожидания селектора {selector} на {link}") # Можно добавить детальное логирование
             return default
        except Exception:
            # Любая другая ошибка при извлечении текста
            # logging.debug(f"Ошибка при извлечении текста из селектора {selector} на {link}")
            return default

    try:
        # Извлекаем сырые данные
        title_raw = await safe_inner_text('h1[data-qa="vacancy-title"]')
        company_raw = await safe_inner_text('a[data-qa="vacancy-company-name"]')
        # Этот селектор содержит и дату, и город
        location_and_date_raw = await safe_inner_text('p.vacancy-creation-time-redesigned')
        # Селектор для даты публикации, может быть более точным
        published_date_raw = await safe_inner_text("p.vacancy-creation-time-redesigned span")

        salary_raw = await safe_inner_text('span[data-qa="vacancy-salary-compensation-type-net"]')
        description_raw = await safe_inner_text('div[data-qa="vacancy-description"]')
        experience_raw = await safe_inner_text('span[data-qa="vacancy-experience"]')
        employment_type_raw = await safe_inner_text('div[data-qa="common-employment-text"]')
        schedule_raw = await safe_inner_text('p[data-qa="work-schedule-by-days-text"]')
        working_hours_raw = await safe_inner_text('div[data-qa="working-hours-text"]')
        work_format_raw = await safe_inner_text('p[data-qa="work-formats-text"]')

        # Извлечение навыков
        skills_selector = '[data-qa="skills-element"]'
        skills_elements = page.locator(skills_selector)
        raw_skills_list = (
            await skills_elements.all_inner_texts()
            if await skills_elements.count() > 0
            else []
        )
        # skills_raw_joined = ",".join([s.strip() for s in raw_skills_list]) # Или просто передать список?
        # Решаем передавать список в clean_skills, чтобы она его объединила

        # Нормализация URL: оставляем только базовый путь
        # Уже есть утилита canonical_link, можно использовать ее для консистентности
        # normalized_url = link.split("?")[0] # Убираем параметры

        # Применяем чистку и парсинг
        published_at_parsed: date | None = parse_russian_date(published_date_raw) # Парсим в дату или None
        city_extracted = extract_city(location_and_date_raw)
        location_cleaned = normalize_city_name(city_extracted)

        data = {
            "title": clean_text_safe(title_raw),
            "company": clean_text_safe(company_raw),
            # Сохраняем очищенный город
            "location": location_cleaned,
            # Применяем парсинг зарплаты
            "salary": clean_text_safe(salary_raw), # Сохраняем сырую зарплату тоже, если нужно
            "salary_range": extract_salary_range_with_currency(salary_raw), # Сохраняем спарсенный диапазон
            "description": clean_text_safe(description_raw),
            "experience": clean_text_safe(experience_raw),
            "employment_type": clean_text_safe(employment_type_raw),
            "schedule": clean_schedule(schedule_raw),
            "working_hours": clean_working_hours(working_hours_raw), # Используем чистящую функцию
            "work_format": clean_work_format(work_format_raw), # Используем чистящую функцию
            # Сохраняем спарсенную дату (объект date или None)
            "published_at": published_at_parsed,
            # Сохраняем сырую строку даты, если она нужна для отладки или других целей
            "published_date": clean_text_safe(published_date_raw),
            # Применяем чистку навыков
            "skills": clean_skills(",".join(raw_skills_list)), # clean_skills ожидает строку, объединяем список
            # Используем утилиту для нормализации ссылки
            "url": clean_text_safe(link.split("?", 1)[0]), # Или canonical_link(link) из utils
        }

        # Удаляем ключи с "Не указано", если колонка в БД NULLABLE и вы хотите хранить NULL
        # В текущей схеме все поля VARCHAR, так что "Не указано" подходит.
        # Если бы published_at был DATE/TIMESTAMP NOT NULL, нужно было бы проверять None до сохранения.

        logging.info(f"[Vacancy Parsed] ✅ {data.get('title', 'Без названия')} — {data.get('company', 'Без компании')}")
        return data

    except Exception as e:
        # Ловим любые другие ошибки при парсинге данных со страницы
        logging.warning(f"[Vacancy Parse] ❌ Ошибка при парсинге деталей {link}: {e}")
        return None
