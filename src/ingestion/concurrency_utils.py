from asyncio import Semaphore

async def process_with_semaphore(playwright,browser, start_url: str, semaphore: Semaphore, extractor_function,**kwargs) -> dict:
    async with semaphore:
        print(f"Processing: {start_url}")
        try:
            data = await extractor_function(playwright,browser, start_url,**kwargs)
            return data
        except Exception as e:
            print(f" Exception: {e}")
            return None
