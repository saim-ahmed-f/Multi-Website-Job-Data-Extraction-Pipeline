import re
import json
from importlib.machinery import SourceFileLoader

from sql_connector import postgres_connector_get

DIR_LOCATION = "C:/Users/Dell/Desktop/final project/Selenium Project/websites/Extracter_code/main_code.py"

def extraction_controller_(url_tag , url_id , url , driver):
    re_exprasion = r'[/][/]([w]{3})?[.]?([^/]+)'#[^/]+[/][/][w]?[w]?[w]?[.]?([^/]+)'
    main_url = re.findall(re_exprasion , url.strip())[0][-1]
    
    url_info = None

    if url_tag != 'UDU':
        query = 'select sleep , title , company , posted_on , salary , urls.location , description , urls.link from urls where url_id = '
        query += str(url_id)
        query += " ;"
        url_info = postgres_connector_get('postgres' , '1407' , query)
    else:
        query = 'select sleep , title , company , posted_on , salary , uui.location , description , uui.link from user_defined_url uui where user_def_id = '
        query += str(url_id)
        query += " ;"
        url_info = postgres_connector_get('postgres' , '1407' , query)

    
    print("loading....")
    if url_info != None:
        url = SourceFileLoader('extract_driver' , DIR_LOCATION).load_module()
        fetched_data = url.extract_driver(url_info[0] , driver , main_url)
        return fetched_data
    else:
        return False
    
