# etl_lw

1-First we get the last publishedDate from our cleaned table filtered by the last 7 days(
so we dont analyze the whole table for just 1 row and 7 days in case its monday and we need the ones from friday,
if it gets too bad we have 7 days to analyze what happened
)
2-We go to the website and use the api (first we get the token with the import requests, then we call for the "our work"
in the filter of the request and get the urls)
3-After getting the urls, we filter them for those articles that are equal to or more than our last publishedDate
4-We go to those urls and get the css elements and the json+id 
5-We do some tranformations with polars (pyspark is overkill for this) like trying 
6-We load from polars to a temp table (for both my raw and cleaned df)
7-For the cleaned, we do an upsert for the raw just an insert (i should add like a DELETE duplicates between 30 days ago and the last 3 days
in my syncstartdatetime)
8-If there where any rows updated or inserted in my cleaned, do an upsert with our dwh table

