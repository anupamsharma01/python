# python
Stock options utility based on Python to scrap options data using TD Ameritrade APIs and ultimately load to SQLITE3 (open-source in-memory) database for analytics.

#Anupam Sharma - Feb 2020

@stocks_data_pull.py
Python app to fetch stocks list from file and then fetch each stock's current market price, 52 week high, low range to output file. 
Stock data is fetched using TD Ameritrade's APIs based on my trading account.

@options_chain_pull.py
Python app to fetch & compute options data as per below processing steps and load to SQLITE3 (in-memory free license database) table:

1. Fetch stocks list from file
2. Compute options expiry based on current day of week: coming Friday or else next week's Friday if today is Friday.
3. Get Stocks data for each equity - current market price, 52week high, low using TDAmeritrade API call.
4. Get and compute Options data for each equity - equity, symbol, cmp, _52WkRange, strikePrice, last, bid, ask, bidSize, askSize, totalVolume, volatility, putCall, inTheMoney, daysToExpiration, timeValue, theoreticalVolatility using TDAmeritrade API call.
5. Compute premiums from above loaded chain table and loads filtered dataset as needed for desired option trades
6. Write final results from table and calculations to Excel spreadsheet.


@options_excel_pull.py
Python app to fetch options data for ER based stocks. For stock having results After market open of current trading day + Before market open of next trading day .
1. Compute next trading day based on current day of week.
2. Read stocks eligible for options trade from excel sheet.
3. Since zacks ER list defined market time differently than others, remap the market timings as per AMC (after market close) or BMO (before market open).
4. Pull data from whichever sheet has data - nasdaq, chameleon or tdameritrade tables.
5. Load the table results to excel workbook - to specific sheetname identified based on next trading day of week.

