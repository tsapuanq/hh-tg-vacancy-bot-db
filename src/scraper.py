import logging
import re
from datetime import date

from src.cleaning import (
    clean_text_safe,
    parse_russian_date,
    normalize_city_name,
    extract_salary_range_with_currency,
    clean_skills,
    clean_working_hours,
    clean_work_format,
    clean_schedule
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from src.config import SCRAPER_TIMEOUT_GOTO, SCRAPER_TIMEOUT_SELECTOR


async def get_vacancy_details(link: str, page) -> dict | None:
    try:
        await page.goto(link, timeout=SCRAPER_TIMEOUT_GOTO, wait_until="domcontentloaded")
        await page.wait_for_selector('h1[data-qa="vacancy-title"]', timeout=SCRAPER_TIMEOUT_SELECTOR)
    except PlaywrightTimeoutError:
        logging.warning(f"[Vacancy Load] ⏳ Таймаут при загрузке или ожидании селектора для {link}")
        return None
    except Exception as e:
        logging.warning(f"[Vacancy Load] ❌ Не удалось загрузить {link}: {e}")
        return None

    async def safe_inner_text(selector: str, default="Не указано") -> str:
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
            "div[class*='magritte-text'][class*='typography-label']"
        )

        try:
            elem = await page.query_selector("div:has-text('Вакансия опубликована')")
            if elem:
                text = await elem.inner_text()
                match = re.search(r"(\d{1,2}\s+[а-я]+\s+\d{4})", text.lower())
                published_date_raw = match.group(1) if match else None
            else:
                published_date_raw = None
            published_at_parsed = parse_russian_date(published_date_raw)
            print(f"🕓 RAW date: {published_date_raw} → parsed: {published_at_parsed}")
        except Exception as e:
            logging.warning(f"[Date Parse] ❌ Ошибка при извлечении даты: {e}")
            published_date_raw, published_at_parsed = None, None

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

        # ✅ Заменили ТОЛЬКО извлечение города
        city_raw = None
        try:
            sidebar_elems = await page.query_selector_all("[class*='magritte-text']")
            for elem in sidebar_elems:
                raw_text = (await elem.inner_text()).strip()
                if "опубликована" in raw_text.lower():
                    cleaned = re.sub(r"<!--.*?-->", "", raw_text)
                    cleaned = re.sub(r"\s+", " ", cleaned).strip()
                    parts = cleaned.split(" ")
                    city_raw = parts[-1] if parts else None
                    break
        except:
            city_raw = None

        location_cleaned = normalize_city_name(city_raw or "Не указано")

        data = {
            "title": clean_text_safe(title_raw),
            "company": clean_text_safe(company_raw),
            "location": location_cleaned,
            "salary": clean_text_safe(salary_raw),
            "salary_range": extract_salary_range_with_currency(salary_raw),
            "description": clean_text_safe(description_raw),
            "experience": clean_text_safe(experience_raw),
            "employment_type": clean_text_safe(employment_type_raw),
            "schedule": clean_schedule(schedule_raw),
            "working_hours": clean_working_hours(working_hours_raw),
            "work_format": clean_work_format(work_format_raw),
            "published_at": published_at_parsed,
            "published_date": clean_text_safe(published_date_raw),
            "skills": clean_skills(",".join(raw_skills_list)),
            "url": clean_text_safe(link.split("?", 1)[0]),
        }

        logging.info(f"[Vacancy Parsed] ✅ {data.get('title', 'Без названия')} — {data.get('company', 'Без компании')}")
        return data

    except Exception as e:
        logging.warning(f"[Vacancy Parse] ❌ Ошибка при парсинге деталей {link}: {e}")
        return None