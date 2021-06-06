import concurrent.futures
from datetime import date
import random
import time
import timeit

import numpy as np
import pandas as pd
import requests
from lxml import html

def get_page(url):
    '''
    Sets up GET request and sends to specified URL.
    Returns a requests object for parsing.
    '''
    # Set up user agent list
    user_agent_list = [
        'Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/88.0',
    ]
    referer_list = [
        'https://au.finance.yahoo.com',
        'https://duckduckgo.com/',
        'https://www.google.com'
    ]

    # Set up HTTP GET headers
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,\
            */*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Pragma': 'no-cache',
        'Referer': random.choice(referer_list),
        'User-Agent': random.choice(user_agent_list)
    }

    # Fetch page
    return requests.get(url, headers)

def scrape_page(page):
    '''
    Converts requests object to a readable data structure tree.
    tree will contain the entire HTML file of the page in a tree structure
    '''
    tree = html.fromstring(page.content)

    # Sanity check
    # We use XPath to find the heading, to confirm that we've obtained the correct page
    # Good XPath documentation found on w3schools
    # print(tree.xpath('//h1/text()'))


    # The table rows of the balance sheet, income statement, and CFS have class 'D(tbr)'
    # Using the // expresion of XPath we select all divs which contain the D(tbr) @class attribute
    # Note: this may change in the future.
    # If so, you will need to manually inspect page source and find new class if so
    table_rows = tree.xpath("//div[contains(@class, 'D(tbr)')]")

    # Check that we do have rows
    # If not, then we should check if we are are finding with correct classes (see above)
    assert len(table_rows) > 0

    return table_rows

def parse_rows(table_rows):
    '''
    Parses the tables obtained from iterating over the tree data structure.
    Returns a pandas DataFrame.
    '''
    parsed_rows = []
    completed_rows = []

    for table_row in table_rows:
        parsed_row = []
        # grab the current div
        el = table_row.xpath("./div")

        none_count = 0

        for rs in el:
            try:
                # The numbers/values are held in span classes
                # Find the first text element that is a child of the span, append as a 1-tuple
                (text,) = rs.xpath('.//span/text()[1]')

                # Pandas (by design) does not allow duplicate column names
                # Unfortunately financial statements do, e.g. "Deferred revenues"
                # The purpose of the following block is to detect duplicate entries
                #  and mark them as so,
                #   whilst simultaneously casting headers to strings.
                # This is a pretty hacky workaround, but since our analysis does not require it,
                #   we don't need to touch it.

                test = str(text).replace(',','')

                duplicate = 0

                if test in completed_rows: # duplicate entry
                    parsed_row.append(test + ' (duplicate)')
                    duplicate = 1

                try: # if it's an integer, then don't do anything special
                    int(test)
                except ValueError:
                    completed_rows.append(test)

                if duplicate == 0:
                    parsed_row.append(test)

            except ValueError:
                # Some rows have no values: we still want these 
                parsed_row.append(np.NaN)
                none_count += 1

        # Every row has four columns, we still add the row if no data is available
        if none_count < 4:
            parsed_rows.append(parsed_row)

    return pd.DataFrame(parsed_rows)

def clean_data(df):
    '''
    Simple function to clean a DataFrame.
    We transpose it and convert entries to floats where necessary.
    After transposing, drop the first column (the 'Date' column)
    '''

    df = df.set_index(0) # Set the index to the first column: 'Period Ending'.
    df = df.transpose() # Transpose the DataFrame, so that our header contains the account names

    # Rename the "Breakdown" column to "Date" (this is because we transposed the data)
    cols = list(df.columns)
    cols[0] = 'Date'
    df = df.set_axis(cols, axis='columns', inplace=False)

    # Convert the data types of everything (except dates) to numeric (float64)
    numeric_columns = list(df.columns)[1::]

    for column_name in numeric_columns:
        df[column_name] = df[column_name].str.replace(',', '') # Remove the thousands separator
        df[column_name] = df[column_name].astype(np.float64) # Convert the column to float64

    return df

def scrape_table(url):
    '''
    Helper function to return a cleaned DataFrame from a URL.
    '''
    page = get_page(url) # Step 1: Fetch page
    df = parse_rows(scrape_page(page)) # Step 2: Parse data
    df = clean_data(df) # Step 3: Clean data

    return df

def scrape_basics(url):
    '''
    Similar function to scrape_page().
    Purpose: grab Name and Market Cap from ticker.
    We modify the function since the format of the summary page is different.
    '''
    page = get_page(url)
    tree = html.fromstring(page.content)

    # Find name:
    names = tree.xpath("//div[contains(@class, 'D(ib)')]")
    parsed_name_rows = []
    for table_row in names:
        parsed_row = ['Name']
        # grab the current div
        el = table_row.xpath("./div")
        name_found = 0

        if el: 
            for rs in el:
                try:
                    # Find the name
                    (text,) = rs.xpath('.//h1/text()[1]')
                    parsed_row.append(str(text).replace(',',''))
                    name_found = 1
                    break
                except Exception:
                    continue

        if name_found == 1:
            parsed_name_rows.append(parsed_row)
            break

    # Market Cap:
    market_cap = tree.xpath("//tr[contains(@class, 'Bxz(bb)')]")

    parsed_market_cap_rows = []

    for table_row in market_cap:
        parsed_row = []
        # grab the current div
        el = table_row.xpath("./td")

        if el and len(el) == 2:

            # convert EL[0] to text
            (text,) = el[0].xpath('.//span/text()[1]')
            test = str(text).replace(',','')

            parsed_row.append(test)

            if test == 'Market cap':
                (cap,) = el[1].xpath('.//span/text()[1]')
                if str(cap).endswith('B'): # Yahoo Finance stores numbers in thousands
                    mc = int(float(str(cap).strip('B'))*(10**6))
                if str(cap).endswith('M'):
                    mc = int(float(str(cap).strip('B'))*(10**3))
                parsed_row.append(mc)

                break

    parsed_market_cap_rows.append(parsed_row)

    #df_symbol = pd.DataFrame([['Symbol', symbol]])
    df_date = pd.DataFrame([['Date', date.today().strftime('%d/%m/%Y')]])
    df_name = pd.DataFrame(parsed_name_rows)
    df_mc = pd.DataFrame(parsed_market_cap_rows)

    df = pd.concat([df_date, df_name, df_mc], sort=True)

    df = df.set_index(0) # Match indexing of the other DataFrames
    df = df.transpose()

    return df

def scrape_symbol(symbol):
    '''
    Main function to scrape all financials of one stock.
    Returns a pandas DataFrame of all financials and summary.
    '''
    symbol_statements = {
        symbol : None,
        'financials': None,
        'balance-sheet': None,
        'cash-flow': None
    }

    # Get all financial statements
    for key in symbol_statements:      
        if key.endswith('AX'):
            url = 'https://au.finance.yahoo.com/quote/' + symbol + '?p=' + symbol
            symbol_statements[key] = scrape_basics(url).set_index('Date') 
            # require setting index to deal with column overlap on the merge
        else:
            url = 'https://au.finance.yahoo.com/quote/' + symbol + '/' + key + '?p=' + symbol
            symbol_statements[key] = scrape_table(url).set_index('Date')
        print('Scraped URL', url)
    # Make one DataFrame to combine all financials
    df = symbol_statements[symbol] \
        .join(symbol_statements['balance-sheet'], on='Date', \
            how='outer', rsuffix=' - Balance Sheet') \
        .join(symbol_statements['financials'], on='Date', \
            how='outer', rsuffix=' - Income Statement') \
        .join(symbol_statements['cash-flow'], on='Date', \
            how='outer', rsuffix=' - Cash Flow') \
        .dropna(axis=1, how='all') \
        .reset_index()

    # Add identifier
    df.insert(1, 'Symbol', symbol)

    return df

def scrape_multiple(symbols):
    '''
    Multithreaded function to scrape symbols from a list.
    This is relatively conservative in the sleep time.
    Since our jobs are not processor bound (we wait ~0.3 seconds for the GET request),
        multi-threading will speed this up.
    We don't need to deal with any concurrency issues, since
        appending to a list is a thread-safe operation!
    '''
    symbol_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_scrape = {executor.submit(scrape_symbol, symbol): symbol for symbol in symbols}

        for future in concurrent.futures.as_completed(future_to_scrape):
            time.sleep(random.uniform(4,10)) # Don't be too greedy
            scrape = future_to_scrape[future]
            try:
                data = future.result()
                symbol_data.append(data)
            except Exception as e:
                print('%r generated an exception: %s' % (scrape, e))

    return pd.concat(symbol_data, sort=False)

def sanity_check(element):
    '''
    Simple helper function to ensure we always get an integer.
    '''
    if not isinstance(element, int):
        element = 0
    return element

def compute_fundamentals(df):
    '''
    Purpose: Compute fundamentals for the _most recent year_ (exclude ttm)
    input: Pandas DataFrame
    output: Pandas DataFrame
        [symbol, name, Market Cap, Current debt, Fixed debt, Cash, EBIT, EV, EBIT:EV, NWC, NA, RoC]
    '''

    prev_symbol = ''
    increment = 0
    fundamentals = []

    for index in range(len(df.index)):

        symbol = df.iloc[index]['Symbol']

        if symbol == prev_symbol: # same ticker as before
            increment += 1
            if increment == 1: # haven't retrieved data yet
                # Retrieve basics
                total_current_assets = df.iloc[index]['Total current assets']
                total_fixed_assets = df.iloc[index]['Total non-current assets']
                total_current_liabilities = df.iloc[index]['Total current liabilities']
                total_fixed_liabilities = df.iloc[index]['Total non-current liabilities']
                goodwill = sanity_check(df.iloc[index]['Goodwill'])
                intangibles = sanity_check(df.iloc[index]['Intangible assets'])
                cash = df.iloc[index]['Cash and cash equivalents']
                EBIT = df.iloc[index]['Operating income or loss']

                # Compute the following
                EV = MC + total_current_liabilities + total_fixed_liabilities - cash
                EBIT_EV = EBIT/EV
                NWC = total_current_assets - total_current_liabilities
                NA = total_fixed_assets - goodwill - intangibles
                RoC = EBIT/(NWC + NA)

                fundamentals.append([symbol, name, MC, \
                    total_current_liabilities, total_fixed_liabilities, \
                    cash, EBIT, EBIT_EV, NWC, NA, RoC])
            else:
                continue
        else: # reached new ticker
            prev_symbol = symbol # update prev_symbol
            name = df.iloc[index]['Name']
            MC = df.iloc[index]['Market cap']
            increment = 0 # reset counter

    df = pd.DataFrame(fundamentals)
    df.columns = ['Symbol', 'Name', 'Market cap', \
        'Total current liabilities', 'Total fixed liabilities', \
            'Cash', 'EBIT', 'EBIT/EV ratio', \
                'Net working capital', 'Net assets', \
                    'Return on capital']
    return df

def pick_stocks(df):
    '''
    Purpose: computes ranks and sorts the DataFrame
    input: Pandas DataFrame
        [symbol, name, Market Cap, Current debt, Fixed debt, Cash, 
        EBIT, EV, EBIT/EV, NWC, NA, RoC]
    output: Pandas DataFrame
        [aggregate rank, EBIT/EV rank, 
        RoC rank, symbol, name, Market Cap, 
        Current debt, Fixed debt, Cash, NWC, NA] 
        sorted by aggregate rank
    '''
    # sort by EBIT/EV
    df.sort_values(by=['EBIT/EV ratio'], ascending=False, \
        inplace=True, ignore_index=True)
        # inplace=True since we want to preserve this DataFrame
        # ignore_index=True so that the assignment of ranks works correctly in the following loop
    for i in range(len(df.index)): # assign EBIT/EV rank
        df.at[i, 'EBIT/EV rank'] = i + 1

    df.sort_values(by=['Return on capital'], ascending=False, \
        inplace=True, ignore_index=True) # sort by RoC
    for i in range(len(df.index)): # assign RoC rank
        df.at[i, 'RoC rank'] = i + 1

    df['Aggregate rank'] = df['EBIT/EV rank'] + df['RoC rank'] # Build dataframe by vectorising

    df.drop(columns = ['EBIT', 'Return on capital', 'EBIT/EV ratio'])
    cols = ['Aggregate rank', 'EBIT/EV rank', 'RoC rank', \
        'Symbol', 'Name', 'Market cap', \
            'Total current liabilities', 'Total fixed liabilities', 'Cash', \
                'Net working capital', 'Net assets']
    df = df[cols]

    return df.sort_values(by=['Aggregate rank'], ignore_index=True) # Sort by aggregate rank

def get_symbols(text_file):
    '''
    Simple helper function to parse stock tickers from a text file.
    '''
    symbols = []
    with open(text_file) as file:
        for line in file:
            line = line.strip()
            if not line.endswith('.AX'): # Yahoo Finance appends .AX to all ASX stocks
                line += '.AX'
            symbols.append(line)
    return symbols

def main():
    '''
    Driver code
    '''
    start = timeit.default_timer()
    text_file = 'stocks.txt'
    symbols = get_symbols(text_file)

    with pd.ExcelWriter('watchlist.xlsx') as writer:

        # Step 1: Scrape data
        df = scrape_multiple(symbols)
        stop_scrape = timeit.default_timer()
        print('Time to execute scrape:', stop_scrape - start)

        df.to_excel(writer, sheet_name='Raw Data')
        writer.save()

    with pd.ExcelWriter('watchlist.xlsx', mode='a') as writer:
        # Step 2: Compute fundamentals
        fundamentals = compute_fundamentals(df)
        fundamentals.to_excel(writer, sheet_name='Fundamental Analysis')
        writer.save()

        # Step 3: Rank them
        ranks = pick_stocks(fundamentals)
        ranks.to_excel(writer, sheet_name='Ranks')
        writer.save()

    stop_excel = timeit.default_timer()

    return print('Time to run analysis and export to Excel:', stop_excel - stop_scrape)


if __name__ == '__main__':
    main()
