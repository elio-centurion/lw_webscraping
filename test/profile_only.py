from playwright.sync_api import sync_playwright
import json
from src.ingestion.page_configurations import custom_headers

start_url='https://www.lw.com/en/news/ex-treasury-dept-deputy-asst-gc-joins-latham-in-dc'

def get_list_text_from_profile(page,parameter)-> list[str]:
    variable_heading = page.get_by_role("heading", name=parameter)
    variable_list = variable_heading.locator(
        "xpath=following-sibling::ul[contains(@class, 'qualifications__list') or contains(@class, 'qualifications__list-inline')]//li"
    )
    results = []
    count = variable_list.count()
    for i in range(count):
        li = variable_list.nth(i)
        try:
            text = li.inner_text().strip()
            results.append(text)
        except Exception as e:
            print(f"Error extracting text from list item #{i}: {e}")
            results.append("")
    return results

def get_profile_information(playwright,start_url):
    browser=playwright.chromium.launch(headless=False)
    context=browser.new_context(extra_http_headers=custom_headers)
    page=context.new_page()
    response=page.goto(start_url)
    print(response.headers())

    data=page.locator("script[type='application/ld+json']").text_content()
    json_data=json.loads(data)
    mainEntity = json_data.get("mainEntity", [{}])
    name = mainEntity.get("name")
    jobTitle = mainEntity.get("jobTitle")
    email = mainEntity.get("email")
    workLocation = mainEntity.get("workLocation")
    locations=[]
    telephones=[]
    for i in workLocation:
        location=i.get("name")
        telephone=i.get("telephone")
        locations.append(location)
        telephones.append(telephone)
    education=get_list_text_from_profile(page,parameter='Education')
    practices=get_list_text_from_profile(page,parameter='Practices')
    industries=get_list_text_from_profile(page,parameter='Industries')
    vcard_link = page.locator('a[href*="vcardapi"]')
    vcard_url = vcard_link.get_attribute("href")

    all_profile = {
        "name": name,
        "jobTitle": jobTitle,
        "email": email,
        "locations": locations,
        "telephones":telephones,
        "education": education,
        "practices": practices,
        "industries": industries,
        "vcard_link":vcard_url,
        "url": start_url,
        "mainEntity":mainEntity,
    }
    print(practices)

    page.close()
    context.close()
    browser.close()
    return all_profile

with sync_playwright() as playwright:
    get_profile_information(playwright, start_url)