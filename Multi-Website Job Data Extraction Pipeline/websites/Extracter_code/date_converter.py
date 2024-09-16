import re

from datetime import date , timedelta

def date_convert(given_date):
    re_expression = r'(\d+)[+]?'
    days = re.findall(re_expression , given_date)
    if len(days) != 0:
        final_date = date.today() - timedelta(days=int(days[0]))
    else:
        final_date = date.today()
    return final_date

