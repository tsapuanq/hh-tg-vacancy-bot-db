from playwright.async_api import async_playwright
import logging

async def get_vacancy_details(link: str) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(link)

        await page.wait_for_selector('h1[data-qa="vacancy-title"]')
        salary_raw = await page.inner_text('span[data-qa="vacancy-salary-compensation-type-net"]', timeout=3000) if await page.query_selector('span[data-qa="vacancy-salary-compensation-type-net"]') else "Не указано"
        salary = salary_raw.replace('\xa0', ' ') 

        title_raw = await page.inner_text('h1[data-qa="vacancy-title"]')
        title = title_raw.replace('\xa0', ' ') 

        company_raw = await page.inner_text('a[data-qa="vacancy-company-name"]', timeout=3000) if await page.query_selector('a[data-qa="vacancy-company-name"]') else "Не указано"
        company = company_raw.replace('\xa0', ' ') 

        location_raw = await page.inner_text('span[data-qa="vacancy-view-raw-address"]', timeout=3000) if await page.query_selector('span[data-qa="vacancy-view-raw-address"]') else "Не указано"
        location = location_raw.replace('\xa0', ' ') 

        description_raw = await page.inner_text('div[data-qa="vacancy-description"]', timeout=5000)
        description = description_raw.replace('\xa0', ' ') 

        experience_raw = await page.inner_text('span[data-qa="vacancy-experience"]', timeout=3000) if await page.query_selector('span[data-qa="vacancy-experience"]') else "Не указано"
        experience = experience_raw.replace('\xa0', ' ') 

        employment_type_raw = await page.inner_text('div[data-qa="common-employment-text"]', timeout=3000) if await page.query_selector('div[data-qa="common-employment-text"]') else "Не указано"
        employment_type = employment_type_raw.replace('\xa0', ' ') 

        schedule_raw = await page.inner_text('p[data-qa="work-schedule-by-days-text"]', timeout=3000) if await page.query_selector('p[data-qa="work-schedule-by-days-text"]') else "Не указано"
        schedule = schedule_raw.replace('\xa0', ' ') 

        working_hours_raw = await page.inner_text('div[data-qa="working-hours-text"]', timeout=3000) if await page.query_selector('div[data-qa="working-hours-text"]') else "Не указано"
        working_hours = working_hours_raw.replace('\xa0', ' ') 

        work_format_raw = await page.inner_text('p[data-qa="work-formats-text"]', timeout=3000) if await page.query_selector('p[data-qa="work-formats-text"]') else "Не указано"
        work_format = work_format_raw.replace('\xa0', ' ') 

        skills_selector = '[data-qa="skills-element"]'
        skills_elements = page.locator(skills_selector)

        if await skills_elements.count() > 0:
            
            skills_raw = await skills_elements.all_inner_texts() 
            skills = [skill.replace('\xa0', ' ') for skill in skills_raw]

        else:

            skills = []
      

        await browser.close()

        return {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "description": description,
            "link": link,
            "experience": experience,
            "employment_type": employment_type,
            "schedule": schedule,
            "working_hours": working_hours,
            "work_format": work_format,
            'skills': skills
        }
