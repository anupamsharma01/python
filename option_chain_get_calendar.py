'''
Yahoo! Earnings Calendar scraper
'''
import datetime
import json
import logging
import requests
import time
import sqlite3

BASE_URL = 'https://finance.yahoo.com/calendar/earnings'
BASE_STOCK_URL = 'https://finance.yahoo.com/quote'
RATE_LIMIT = 2000.0
SLEEP_BETWEEN_REQUESTS_S = 60 * 60 / RATE_LIMIT
OFFSET_STEP = 100

# Logging config
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

#date variables
start_time = datetime.datetime.now()
today = datetime.datetime.strftime(start_time, '%b %d %Y') #Sep 19 2019
print ('today:-', today)
dump_cal_file = 'C:\Anupam\market\option_chain\dumps\dump_ER_calendar_hist.csv'
file_out = open(dump_cal_file,'a')
debug_detailed = 0  # 1 is True; 0 is False

#sqlite connection
connection = sqlite3.connect('C:\Anupam\market\option_chain\options_chain_dump.db')
cursor = connection.cursor()
# Get equities from cboe_data for optionable list
equity_list_cboe = []
equity_list_sql = "SELECT EQUITY FROM CBOE_DATA;"
print("equity_list_sql==", equity_list_sql)
cursor.execute(equity_list_sql)
rows = cursor.fetchall()
if (debug_detailed):
    print ('ROWS:-', rows)
for row in rows:
    equity_list_cboe.append(row[0])
if (debug_detailed):
    print ('final equity list:', equity_list_cboe)


class YahooEarningsCalendar(object):
    """
    This is the class for fetching earnings data from Yahoo! Finance
    """

    def __init__(self, delay=SLEEP_BETWEEN_REQUESTS_S):
        self.delay = delay

    def _get_data_dict(self, url):
        time.sleep(self.delay)
        page = requests.get(url)
        page_content = page.content.decode(encoding='utf-8', errors='strict')
        page_data_string = [row for row in page_content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]
        page_data_string = page_data_string.split('root.App.main = ', 1)[1]
        return json.loads(page_data_string)

    def get_next_earnings_date(self, symbol):
        """Gets the next earnings date of symbol
        Args:
            symbol: A ticker symbol
        Returns:
            Unix timestamp of the next earnings date
        Raises:
            Exception: When symbol is invalid or earnings date is not available
        """
        url = '{0}/{1}'.format(BASE_STOCK_URL, symbol)
        try:
            page_data_dict = self._get_data_dict(url)
            return page_data_dict['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']['earningsDate'][0]['raw']
        except:
            raise Exception('Invalid Symbol or Unavailable Earnings Date')

    def earnings_on(self, date, offset=0, count=1):
        """Gets earnings calendar data from Yahoo! on a specific date.
        Args:
            date: A datetime.date instance representing the date of earnings data to be fetched.
            offset: Position to fetch earnings data from.
            count: Total count of earnings on date.
        Returns:
            An array of earnings calendar data on date given. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            TypeError: When date is not a datetime.date object.
        """
        if offset >= count:
            return []

        if not isinstance(date, datetime.date):
            raise TypeError(
                'Date should be a datetime.date object')
        date_str = date.strftime('%Y-%m-%d')
        logger.debug('Fetching earnings data for %s', date_str)
        dated_url = '{0}?day={1}&offset={2}&size={3}'.format(
            BASE_URL, date_str, offset, OFFSET_STEP)
        page_data_dict = self._get_data_dict(dated_url)
        stores_dict = page_data_dict['context']['dispatcher']['stores']
        earnings_count = stores_dict['ScreenerCriteriaStore']['meta']['total']

        # Recursively fetch more earnings on this date
        new_offset = offset + OFFSET_STEP
        more_earnings = self.earnings_on(date, new_offset, earnings_count)
        curr_offset_earnings = stores_dict['ScreenerResultsStore']['results']['rows']

        return curr_offset_earnings + more_earnings

    def earnings_between(self, from_date, to_date):
        """Gets earnings calendar data from Yahoo! in a date range.
        Args:
            from_date: A datetime.date instance representing the from-date (inclusive).
            to_date: A datetime.date instance representing the to-date (inclusive).
        Returns:
            An array of earnigs calendar data of date range. E.g.,
            [
                {
                    "ticker": "AMS.S",
                    "companyshortname": "Ams AG",
                    "startdatetime": "2017-04-23T20:00:00.000-04:00",
                    "startdatetimetype": "TAS",
                    "epsestimate": null,
                    "epsactual": null,
                    "epssurprisepct": null,
                    "gmtOffsetMilliSeconds": 72000000
                },
                ...
            ]
        Raises:
            ValueError: When from_date is after to_date.
            TypeError: When either from_date or to_date is not a datetime.date object.
        """
        if from_date > to_date:
            raise ValueError(
                'From-date should not be after to-date')
        if not (isinstance(from_date, datetime.date) and
                isinstance(to_date, datetime.date)):
            raise TypeError(
                'From-date and to-date should be datetime.date objects')
        earnings_data = []
        current_date = from_date
        delta = datetime.timedelta(days=1)
        while current_date <= to_date:
            earnings_data += self.earnings_on(current_date)
            current_date += delta
        return earnings_data


if __name__ == '__main__':  # pragma: no cover
    date_from = datetime.datetime.strptime( today+' 12:00AM', '%b %d %Y %I:%M%p')
        #'Sep 18 2019  12:00AM', '%b %d %Y %I:%M%p')
    date_to = datetime.datetime.strptime( today+' 11:59PM', '%b %d %Y %I:%M%p')
        #'Sep 18 2019  11:59PM', '%b %d %Y %I:%M%p')
    print('date_from:-', date_from)
    print('date_to:-', date_to)
    yec = YahooEarningsCalendar()
    yec_dt_range = yec.earnings_between(date_from, date_to)
    #print(yec.earnings_on(date_from))
    #print(yec_dt_range)

    # Returns the next earnings date of BOX in Unix timestamp
    #print('ADBE',yec.get_next_earnings_date('ADBE'))

    #Get and process fn output:
    print ('No. of stocks with earnings today: ', len(yec_dt_range))
    i=0
    while (i<len(yec_dt_range)):
        ticker = yec_dt_range[i]['ticker']
        er_time = yec_dt_range[i]['startdatetimetype']
        if (er_time.upper() not in ['AMC', 'BMO']):
            er_time='NA'
        if (debug_detailed):
            print ('Stocks list from calendar:', ticker, er_time)
        if (ticker in equity_list_cboe):
            string = [ ticker, er_time, datetime.datetime.strftime(date_from, '%Y-%m-%d'), date_from.strftime("%A") ]
            print (','.join(string))
            print(','.join(string) , file=file_out)
        i=i+1

    file_out.close()
    #End
