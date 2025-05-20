# scraper.py
import logging
from datetime import  date

from src.cleaning import (
    clean_text_safe,
    parse_russian_date,
    extract_city,
    normalize_city_name,
    extract_salary_range_with_currency,
    clean_skills,
    clean_working_hours,
    clean_work_format,
    clean_schedule,
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from src.config import SCRAPER_TIMEOUT_GOTO, SCRAPER_TIMEOUT_SELECTOR


async def get_vacancy_details(link: str, page) -> dict | None:
    """
    Переходит по ссылке вакансии, извлекает и очищает детали.
    """
    try:
        await page.goto(
            link, timeout=SCRAPER_TIMEOUT_GOTO, wait_until="domcontentloaded"
        )
        await page.wait_for_selector(
            'h1[data-qa="vacancy-title"]', timeout=SCRAPER_TIMEOUT_SELECTOR
        )
    except PlaywrightTimeoutError:
        logging.warning(
            f"[Vacancy Load] ⏳ Таймаут при загрузке или ожидании селектора для {link}"
        )
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
            return default
        except Exception:
            return default

    try:
        title_raw = await safe_inner_text('h1[data-qa="vacancy-title"]')
        company_raw = await safe_inner_text('a[data-qa="vacancy-company-name"]')
        location_and_date_raw = await safe_inner_text(
            "p.vacancy-creation-time-redesigned"
        )
        published_date_raw = await safe_inner_text(
            "p.vacancy-creation-time-redesigned span"
        )

        salary_raw = await safe_inner_text(
            'span[data-qa="vacancy-salary-compensation-type-net"]'
        )
        description_raw = await safe_inner_text('div[data-qa="vacancy-description"]')
        experience_raw = await safe_inner_text('span[data-qa="vacancy-experience"]')
        employment_type_raw = await safe_inner_text(
            'div[data-qa="common-employment-text"]'
        )
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
        published_at_parsed: date | None = parse_russian_date(
            published_date_raw
        )  
        city_extracted = extract_city(location_and_date_raw)
        location_cleaned = normalize_city_name(city_extracted)

        data = {
            "title": clean_text_safe(title_raw),
            "company": clean_text_safe(company_raw),
            "location": location_cleaned,
            "salary": clean_text_safe(
                salary_raw
            ),  
            "salary_range": extract_salary_range_with_currency(
                salary_raw
            ),  
            "description": clean_text_safe(description_raw),
            "experience": clean_text_safe(experience_raw),
            "employment_type": clean_text_safe(employment_type_raw),
            "schedule": clean_schedule(schedule_raw),
            "working_hours": clean_working_hours(
                working_hours_raw
            ),  
            "work_format": clean_work_format(
                work_format_raw
            ),  
            
            "published_at": published_at_parsed,
            "published_date": clean_text_safe(published_date_raw),
            "skills": clean_skills(
                ",".join(raw_skills_list)
            ),  
            "url": clean_text_safe(
                link.split("?", 1)[0]
            ),
        }
        logging.info(
            f"[Vacancy Parsed] ✅ {data.get('title', 'Без названия')} — {data.get('company', 'Без компании')}"
        )
        return data

    except Exception as e:
        logging.warning(f"[Vacancy Parse] ❌ Ошибка при парсинге деталей {link}: {e}")
        return None
