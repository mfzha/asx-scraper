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
* Retrieve stock tickers for all mid and high cap stocks (there should be a way to scrape this directly but I haven't found a nice source yet. For now, it's easier to export a list of the ASX 100 (all mid and high cap stocks) to a spreadsheet and copy that to a textfile.)

**Janitorial**:
* Clean up `scrape_basics()` function
