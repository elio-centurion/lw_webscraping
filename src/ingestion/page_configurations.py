custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
}

def build_payloads(cities:list[str])->dict:
        payloads={}
        for city in cities:
                payloads[city] = {
                        "searchHub": "lw-people-listing",
                        "aq": "@z95xlanguage=(en)",
                        "firstResult": 20,
                        "numberOfResults": 1000,
                        "fieldsToInclude": [
                                "peopleformattedname", "peopledirectdialnumber", "PrintableUri"
                        ],
                        "facets": [
                                {
                                        "facetId": "@peopleoffices",
                                        "field": "peopleoffices",
                                        "type": "specific",
                                        "currentValues": [
                                                {"value": city, "state": "selected"},
                                        ],
                                        "injectionDepth": 1000
                                }
                        ],
                        "context": {
                                "referrer": "https://www.lw.com/"
                        }
                }
        return payloads


def build_payload_news_with_region(region: str, first_result: int = 0) -> dict:
    return {
        "searchHub": "lw-news-listing",
        "aq": f'@z95xlanguage=(en) AND @taz120xonomyregionstags=="{region}" AND @newsandinsightstypefacet=="Our Work"',
        "firstResult": first_result,
        "numberOfResults": 1000,
        "sortCriteria": "date descending",
        "fieldsToInclude": ["cardtitle","newsandinsightssource",
                            "pageresultdescription","newsandinsightsez120xternallink",
                            "contentdate"],
        "context": {"referrer": "https://www.lw.com/"}
    }

def build_payload_news_without_region(first_result: int = 0) -> dict:
    return {
        "searchHub": "lw-news-listing",
        "aq": '@z95xlanguage=(en) AND NOT @taz120xonomyregionstags AND @newsandinsightstypefacet=="Our Work"',
        "firstResult": first_result,
        "numberOfResults": 1000,
        "sortCriteria": "date descending",
        "fieldsToInclude": ["cardtitle","newsandinsightssource",
                            "pageresultdescription","newsandinsightsez120xternallink",
                            "contentdate"],
        "context": {"referrer": "https://www.lw.com/"}
    }