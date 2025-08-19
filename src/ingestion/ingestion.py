from playwright.async_api import async_playwright
from asyncio import Semaphore
import asyncio
from .concurrency_utils import process_with_semaphore
import json
from .get_profile_ingestion import get_profile_ingestion,get_news_ingestion
from .profiles_only import get_profiles
from .news_only import get_news
from src.config.settings import client
from .date_functions import get_last_startdate
from .page_configurations import custom_headers

"Set to 1 because usually there arent that many in the same day, for initial loading change it to 5 or 10"
MAX_CONCURRENT_REQUESTS = 5

async def main():
    urls_to_scrape=get_profile_ingestion()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [
            process_with_semaphore(playwright=playwright,browser=browser, url=url, semaphore=semaphore, extractor_function=get_profiles)
            for url in urls_to_scrape
        ]
        all_profiles = []
        for coro in asyncio.as_completed(tasks):
            try:
                profile_data = await coro
                all_profiles.append(profile_data)
            except Exception as e:
                print(f"Failed to extract profile: {e}")
        await browser.close()
        with open("../transformation/lw_profile_missing.json", "w", encoding="utf-8") as f:
            json.dump(all_profiles, f, indent=2, ensure_ascii=False)


async def main_news(last_startdate):
    all_metadata=get_news_ingestion(custom_headers,last_startdate)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
        #The extractor function is our actual function for each url. The rest is to process each request in other tabs
        tasks = [
            process_with_semaphore(playwright=playwright,browser=browser, start_url= meta["url"],
                                   semaphore=semaphore, extractor_function=get_news,meta=meta)
            for meta in all_metadata
        ]

        all_news = []
        for coro in asyncio.as_completed(tasks):
            try:
                profile_data = await coro
                all_news.append(profile_data)
            except Exception as e:
                print(f"Failed to extract new: {e}")
        await browser.close()
        return all_news

def ingestion():
    last_startdate = get_last_startdate(client)
    all_news=asyncio.run(main_news(last_startdate))
    return all_news


