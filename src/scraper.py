import logging
import re

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


def _is_missing(value: str | None) -> bool:
    if not isinstance(value, str):
        return True
    value = value.strip()
    return not value or value.lower() == "не указано"


def _clean_hh_title(value: str | None) -> str:
    title = clean_text_safe(value)
    if not title:
        return ""

    title = re.sub(r"^вакансия\s+", "", title, flags=re.IGNORECASE).strip()
    title = re.split(r"\s+[—-]\s+hh\.", title, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    title = re.split(r"\s+\|\s+hh\.", title, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    return title


async def _first_visible_text(page, selectors: tuple[str, ...], timeout: int = 3000) -> str:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            text = await locator.inner_text(timeout=timeout)
            text = clean_text_safe(text)
            if text:
                return text
        except Exception:
            continue
    return ""


async def _first_meta_content(page, selectors: tuple[str, ...]) -> str:
    for selector in selectors:
        try:
            content = await page.locator(selector).first.get_attribute("content", timeout=1000)
            content = clean_text_safe(content)
            if content:
                return content
        except Exception:
            continue
    return ""


async def extract_vacancy_title(page) -> str:
    title = await _first_visible_text(
        page,
        (
            'h1[data-qa="vacancy-title"]',
            '[data-qa="vacancy-title"]',
            "main h1",
            "h1",
        ),
    )
    if title:
        return _clean_hh_title(title)

    title = await _first_meta_content(
        page,
        (
            'meta[property="og:title"]',
            'meta[name="twitter:title"]',
            'meta[name="title"]',
        ),
    )
    if title:
        return _clean_hh_title(title)

    try:
        return _clean_hh_title(await page.title())
    except Exception:
        return ""


async def get_vacancy_details(link: str, page) -> dict | None:
    try:
        await page.goto(link, timeout=SCRAPER_TIMEOUT_GOTO, wait_until="domcontentloaded")
    except PlaywrightTimeoutError:
        logging.warning(f"[Vacancy Load] ⏳ Таймаут при загрузке страницы {link}")
        return None
    except Exception as e:
        logging.warning(f"[Vacancy Load] ❌ Не удалось загрузить {link}: {e}")
        return None

    try:
        await page.wait_for_selector('h1[data-qa="vacancy-title"], h1', timeout=SCRAPER_TIMEOUT_SELECTOR)
    except PlaywrightTimeoutError:
        logging.warning(f"[Vacancy Load] ⏳ Таймаут ожидания h1, пробуем извлечь title из метаданных: {link}")

    async def safe_inner_text(selector: str, default="Не указано") -> str:
        try:
            text = await page.inner_text(selector)
            return clean_text_safe(text)
        except PlaywrightTimeoutError:
            return default
        except Exception:
            return default

    try:
        title_raw = await extract_vacancy_title(page)
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

        salary_raw = "Не указано"
        salary_selectors = (
            'span[data-qa="vacancy-salary-compensation-type-net"]',
            'span[data-qa="vacancy-salary-compensation-type-gross"]',
        )
        for selector in salary_selectors:
            candidate = await safe_inner_text(selector, default=None)
            if candidate:
                salary_raw = candidate
                break
        description_raw = await safe_inner_text('div[data-qa="vacancy-description"]')
        experience_raw = await safe_inner_text('span[data-qa="vacancy-experience"]')
        employment_type_raw = await safe_inner_text('div[data-qa="common-employment-text"]')
        schedule_raw = await safe_inner_text('p[data-qa="work-schedule-by-days-text"]')
        working_hours_raw = await safe_inner_text('div[data-qa="working-hours-text"]')
        work_format_raw = await safe_inner_text('p[data-qa="work-formats-text"]')

        skills_selector = '[data-qa="skills-element"]'
        skills_elements = page.locator(skills_selector)
        raw_skills_list = (
            await skills_elements.all_inner_texts()
            if await skills_elements.count() > 0
            else []
        )

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

        title_cleaned = clean_text_safe(title_raw)
        description_cleaned = clean_text_safe(description_raw)

        if _is_missing(title_cleaned):
            title_cleaned = "Не указано"
            logging.warning(f"[Vacancy Parse] ⚠️ Не удалось спарсить реальное название вакансии: {link}")

        if _is_missing(description_cleaned):
            logging.warning(f"[Vacancy Parse] ⚠️ Пропуск вакансии без описания: {link}")
            return None

        data = {
            "title": title_cleaned,
            "company": clean_text_safe(company_raw),
            "location": location_cleaned,
            "salary": clean_text_safe(salary_raw),
            "salary_range": extract_salary_range_with_currency(salary_raw),
            "description": description_cleaned,
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
