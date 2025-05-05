import logging
from src.utils import clean_text_safe

async def get_vacancy_details(link: str, page) -> dict | None:
    try:
        await page.goto(link, timeout=20000, wait_until="domcontentloaded")
        await page.wait_for_selector('h1[data-qa="vacancy-title"]', timeout=15000)
    except Exception as e:
        logging.warning(f"[Vacancy Load] ❌ Не удалось загрузить {link}: {e}")
        return None

    async def clean(selector: str, default="Не указано"):
        try:
            text = await page.inner_text(selector)
            return clean_text_safe(text)
        except Exception:
            return default

    try:
        data = {
            "title": await clean('h1[data-qa="vacancy-title"]'),
            "company": await clean('a[data-qa="vacancy-company-name"]'),
            "location": await clean('span[data-qa="vacancy-view-raw-address"]'),
            "salary": await clean('span[data-qa="vacancy-salary-compensation-type-net"]'),
            "description": await clean('div[data-qa="vacancy-description"]'),
            "experience": await clean('span[data-qa="vacancy-experience"]'),
            "employment_type": await clean('div[data-qa="common-employment-text"]'),
            "schedule": await clean('p[data-qa="work-schedule-by-days-text"]'),
            "working_hours": await clean('div[data-qa="working-hours-text"]'),
            "work_format": await clean('p[data-qa="work-formats-text"]'),
            "published_date": await clean("p.vacancy-creation-time-redesigned span"),
            "link": clean_text_safe(link),
        }

        # Чтение скиллов
        skills_selector = '[data-qa="skills-element"]'
        skills_elements = page.locator(skills_selector)
        raw_skills = (
            await skills_elements.all_inner_texts()
            if await skills_elements.count() > 0
            else []
        )
        data["skills"] = (
            clean_text_safe(",".join([s.strip() for s in raw_skills]))
            if raw_skills
            else "Не указано"
        )

        logging.info(f"[Vacancy Parsed] ✅ {data['title']} — {data['company']}")
        return data

    except Exception as e:
        logging.warning(f"[Vacancy Parse] ❌ Ошибка при парсинге {link}: {e}")
        return None