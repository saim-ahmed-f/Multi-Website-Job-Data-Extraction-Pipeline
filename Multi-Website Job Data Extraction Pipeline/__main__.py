import threading
from selenium import webdriver
import json
import  pandas as pd

from sql_connector import postgres_connector_get , connector_for_pandas

#import undetected_chromedriver as uc

from all_website_detail import extraction_controller_

final_result = {
		'job_id' : [] ,
        'title' : [],
        'company' : [],
        'posted_on' :[] ,
        'date_of_extract' : [],
        'salary' : [],
        'location' : [],
        'description' : [],
        'link' : [],
		'url_name' : []
}

def run_ectarct(url_tag , url_id , url):
        driver = webdriver.Chrome("C:/Users/Dell/Desktop/final project/ChromeDriver/chromedriver.exe")
        driver.get(url)
        global final_result 
        f_result = extraction_controller_(url_tag , url_id , url , driver)
        final_result['job_id'] = final_result['job_id'] + f_result['job_id']
        final_result['title'] = final_result['title'] + f_result['title']
        final_result['company'] = final_result['company'] + f_result['company']
        final_result['posted_on'] = final_result['posted_on'] + f_result['posted_on']
        final_result['date_of_extract'] = final_result['date_of_extract'] + f_result['date_of_extract']
        final_result['salary'] = final_result['salary'] + f_result['salary']
        final_result['location'] = final_result['location'] + f_result['location']
        final_result['description'] = final_result['description'] + f_result['description']
        final_result['link'] = final_result['link'] + f_result['link']
        final_result['url_name'] = final_result['url_name'] + f_result['url_name']
    #print(url)


	#driver.close()


def url_generator(urls , action = None):
	final_urls = []
	if action != None:
		for i in urls:
			final_urls.append(('UDU' , i[0] , i[1]))
	else:
		for i in urls:
			url = str(i[1]).format(job_search_title = str(i[2]) , experience=str(i[3]))
			final_urls.append(('UI' , int(i[0]) , url))
	return final_urls


def getting_user_info(user_id , etl_id):
	query = 'select urls.url_id , urls.url , etl_data.job_search_title , etl_data.experience from user_url_info uui join urls on uui.urls_id = urls.url_id join etl_data  on uui.etl_id = etl_data.etl_id where uui.user_id = '
	query += str(user_id) + ' and uui.etl_id = ' + str(etl_id)
	query += ' ;'
	
	urls = postgres_connector_get('postgres' , '1407' , query)
	
	urls = url_generator(urls)

	query = '''select user_def_id , url from user_defined_url where user_id ='''
	query += str(user_id) + ' and etl_id = ' + str(etl_id)
	query += ' ;'

	user_defined_url = postgres_connector_get('postgres' , '1407' , query)
	user_defined_url = url_generator(user_defined_url , action=True)
	urls = urls + user_defined_url
	return urls


def thread_creater_and_extractor(user_id , etl_id):
    global final_result
    final_result = {
        'job_id' : [] ,
        'title' : [],
        'company' : [],
        'posted_on' :[] ,
        'date_of_extract' : [],
        'salary' : [],
        'location' : [],
        'description' : [],
        'link' : [],
        'url_name' : []
    }

    urls = getting_user_info(user_id , etl_id)
    
    threads = []
    for url in urls:
        t = threading.Thread(target=run_ectarct , args=(url[0],url[1],url[2],))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()

    print(final_result)
    df = pd.DataFrame(final_result , columns=['job_id' , 'title' , 'company' , 'posted_on' , 'date_of_extract' , 'salary' , 'location' , 'description' , 'link' , 'user_id' , 'url_name'])
    df['user_id'] = user_id
    print(df)
    conn = connector_for_pandas('postgres' , '1407' , df , 'job_data')
    print(conn)
    return conn


if __name__ == "__main__":
		
	final_result = {}
	thread_creater_and_extractor(2 , 4)
    