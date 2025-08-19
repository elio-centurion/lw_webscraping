import json
from .page_configurations import custom_headers
from .playwright_utils import intercept_route

async def get_news(playwright, browser, start_url,meta=None):
    context = await browser.new_context(extra_http_headers=custom_headers)
    page = await context.new_page()
    await page.route("**/*", intercept_route)
    await page.goto(start_url)
    lawyer_link, lawyer_names = [], []
    #The only data that is application/ld+json is the contacts information, else i would have extracted more
    try:
        await page.wait_for_selector("script[type='application/ld+json']", state="attached", timeout=30000)
        data = await page.locator("script[type='application/ld+json']").text_content()
        json_data = json.loads(data)
        persons_urls= json_data.get('mainEntity', {}).get('contactPoint', [])
        lawyer_link = [p.get('email') for p in persons_urls if p.get('email')]
        lawyer_names=[p.get('name') for p in persons_urls if p.get('name')]
    except Exception:
        pass
    top_info = page.locator("div.article-hero__container")
    headline = await top_info.locator("h1").inner_text(timeout=60000)
    news_type = await top_info.locator("span.article-hero__info-type").inner_text()
    date_article = await top_info.locator("span.article-hero__info-date").inner_text()

    article_text_list = await page.locator("div.component.rich-text > div.component-content").all_text_contents()
    article_text = "\n\n".join(article_text_list).strip()

    all_news = {
        "headline": headline.strip(),
        "news_type": news_type.strip(),
        "datePublished": date_article.strip(),
        "text": article_text,
        "lawyer_link": lawyer_link,
        "lawyer_names": lawyer_names,
        "url": start_url,
    }
    #This is because there is some information in the meta not in the url (like location or the id for some reason)
    if meta:
        all_news.update(meta)

    await page.close()
    await context.close()
    return all_news
