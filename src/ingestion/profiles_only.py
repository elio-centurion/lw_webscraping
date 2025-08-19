import json
from asyncio import timeout

from .page_configurations import custom_headers
from .playwright_utils import intercept_route

async def get_list_text_from_profile(page,parameter)-> list[str]:
    variable_heading = page.get_by_role("heading", name=parameter)
    variable_list = variable_heading.locator(
        "xpath=following-sibling::ul[contains(@class, 'qualifications__list') or contains(@class, 'qualifications__list-inline')]//li"
    )
    results = []
    count = await variable_list.count()
    for i in range(count):
        li =variable_list.nth(i)
        try:
            text =await li.inner_text()
            text=text.strip()
            results.append(text)
        except Exception as e:
            print(f"Error extracting text from list item #{i}: {e}")
            results.append("")
    return results

async def get_profiles(playwright, browser, start_url):
    context=await browser.new_context(extra_http_headers=custom_headers)
    page=await context.new_page()
    await page.route("**/*",intercept_route)
    await page.goto(start_url)
    await page.wait_for_selector("script[type='application/ld+json']",state="attached",timeout=10000)
    data= await page.locator("script[type='application/ld+json']").text_content()
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
    education=await get_list_text_from_profile(page,parameter='Education')
    practices=await get_list_text_from_profile(page,parameter='Practices')
    industries=await get_list_text_from_profile(page,parameter='Industries')
    try:
        vcard_link = await page.wait_for_selector('a[href*="vcardapi"]', timeout=5000)
        vcard_url = await vcard_link.get_attribute("href")
    except Exception:
        vcard_url = None

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

    await page.close()
    await context.close()
    return all_profile