import logging 

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)

async def get_vacancy_details(link: str, page) -> dict:
    await page.goto(link)
    await page.wait_for_selector('h1[data-qa="vacancy-title"]', timeout=5000)

    async def clean(selector: str, default="Не указано"):
        try:
            return (await page.inner_text(selector)).replace('\xa0', ' ').strip()
        except:
            return default

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
        "link": link,
        "published_date": await clean('p.vacancy-creation-time-redesigned span')
    }

    skills_selector = '[data-qa="skills-element"]'
    skills_elements = page.locator(skills_selector)
    data["skills"] = await skills_elements.all_inner_texts() if await skills_elements.count() > 0 else []

    return data
