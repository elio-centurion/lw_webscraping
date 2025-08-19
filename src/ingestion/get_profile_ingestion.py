import json
import os
import time
from datetime import datetime

import requests
from .page_configurations import (
    custom_headers,
    build_payloads,
    build_payload_news_with_region,
    build_payload_news_without_region,
)

cities=['Boston','Chicago','Houston','Los Angeles','Los Angeles — Downtown','Los Angeles — GSO',
           'New York','Orange County','San Diego','San Francisco','Silicon Valley','Washington, D.C.']

regions=['Americas','Asia-Pacific','EMEA']

def get_token_requests(custom_headers)-> str:
    ts=int(time.time()*1000)
    token_url=f'https://www.lw.com/coveo/rest/token?t={ts}'
    try:
        response=requests.get(token_url,headers=custom_headers)
        response.raise_for_status()
        token = response.json().get('token')
        return token
    except requests.exceptions.HTTPError as err:
        print(err)

def get_url_requests(token:str) :
    headers = {
        **custom_headers,
        "Authorization": f"Bearer {token}",
    }
    payload_by_cities = build_payloads(cities)
    request_url='https://lathamwatkinsllpproductiondvz7hgwt.org.coveo.com/rest/search/v2?organizationId=lathamwatkinsllpproductiondvz7hgwt'
    all_urls=[]
    for payload in payload_by_cities:
        try:
            response = requests.post(request_url, headers=headers, json=payload_by_cities[payload])
            response.raise_for_status()
            data=response.json()
            total_count=data.get('totalCount')
            data=data.get('results',[])
            for item in data:
                url=item.get("printableUri")
                all_urls.append(url)
        except requests.exceptions.HTTPError as err:
            print(err)
    return all_urls

def page_through(payload_builder, request_url, headers, metadata_start,region=None):
    first_result = 0
    while True:
        if region:
            payload = payload_builder(region, first_result)
        else:
            payload = payload_builder(first_result)
        try:
            response = requests.post(request_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            for item in results:
                url = item.get("printableUri")
                raw = item.get("raw", {})
                if url:
                    metadata_start.append({
                        "url": url,
                        "region": raw.get("taz120xonomyregionstags"),
                        "permanentid": raw.get("permanentid"),
                        "contentdate": raw.get("contentdate"),
                    })
            print(f"{len(results)} results on page {first_result // 1000 + 1}, total so far: {len(metadata_start)}")
            if len(results) < 1000:
                break
            first_result += 1000
            time.sleep(1.5)
        except requests.exceptions.HTTPError as err:
            print(f"Error fetching page {first_result}: {err}")
            break
        except requests.exceptions.RequestException as err:
            print(f"Request failed on page {first_result}: {err}")
            break


def get_url_requests_news(token: str):
    headers = {
        **custom_headers,
        "Authorization": f"Bearer {token}",
    }
    request_url = (
        "https://lathamwatkinsllpproductiondvz7hgwt.org.coveo.com/rest/search/v2"
        "?organizationId=lathamwatkinsllpproductiondvz7hgwt"
    )
    metadata_start = []

    for region in regions:
        print(f"\nFetching news WITH region: {region}")
        page_through(build_payload_news_with_region, request_url, headers, metadata_start, region)

    print("\nFetching news WITHOUT region")
    page_through(build_payload_news_without_region, request_url, headers, metadata_start)

    return metadata_start

def deduplicate_and_find_specific_metadata(metadata_start,last_startdate):
    seen,metada_deduplicate=set(),[]
    for x in metadata_start:
        if x["url"] not in seen and x["contentdate"]>=last_startdate:
            seen.add(x["url"])
            metada_deduplicate.append(x)
    return metada_deduplicate


def get_profile_ingestion(custom_headers:dict):
    token=get_token_requests(custom_headers)
    all_urls=get_url_requests(token)
    return all_urls

def get_news_ingestion(custom_headers:dict,last_startdate):
    token=get_token_requests(custom_headers)
    metadata_start=get_url_requests_news(token)
    metadata=deduplicate_and_find_specific_metadata(metadata_start,last_startdate)
    return metadata