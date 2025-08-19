from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)  # see browser for debugging
    page = browser.new_page()
    page.goto("https://www.lw.com/en/people/jonathan-katz")
    try:
        our_work_cards = page.locator(
            "section#news-and-insights div.news-and-insights__cards article.content-card:has(div.content-card__info span:text('Our Work'))"
        )

        count = our_work_cards.count()
        if count == 0:
            print("No 'Our Work' card found.")
        else:
            for i in range(count):
                card = our_work_cards.nth(i)
                link_el = card.locator("a.content-card__link.field-title")
                href = link_el.get_attribute("href")
                title = link_el.inner_text()
                print(f"Title: {title}")
                print(f"URL: {href}")

    except Exception as e:
        print(e)

    browser.close()
