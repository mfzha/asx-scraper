# ASX Scraper

A scraper and selector for stocks on the ASX. Data pulled from Yahoo AU Finance.

## Usage

Main usage:

    python3 scraper.py

Will produce an Excel spreadsheet with relevant stock tickers and financials for recent financial years.

## Acknowledgements

Ranking method based on the "Magic Formula" from Joel Greenblatt's "The Little Book That Beats The Market", 2006.
Some functions based on code excerpts from Matt Button's "How to scrape Yahoo Finance", 2020.

## Todo

**Long term**:
* Retrieve stock tickers for all mid and high cap stocks

**Janitorial**:
* Clean up `scrape_basics()` function