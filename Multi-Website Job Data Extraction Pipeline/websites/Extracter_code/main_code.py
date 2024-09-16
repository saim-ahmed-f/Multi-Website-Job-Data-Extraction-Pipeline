from  time import sleep

from datetime import date , timedelta

import re

from uuid import uuid4


def date_convert(given_date):
    re_expression = r'(\d+)[+]?'
    days = re.findall(re_expression , given_date)
    if len(days) != 0:
        final_date = date.today() - timedelta(days=int(days[0]))
    else:
        final_date = date.today()
    return final_date


def extract_driver(url_info , driver , main_url):
    
    sleep(int(url_info[0]))


    job_title = driver.find_elements('xpath' , url_info[1])
    company_name = driver.find_elements('xpath' , url_info[2])#'//div[@data-at="job-item-company-name"]')
    job_posting_duration = driver.find_elements('xpath' , url_info[3])#'//li[@data-at="job-item-timeago"]')
    job_salary = driver.find_elements('xpath' , url_info[4])#'//dl[@data-at="job-item-salary-info"]')
    job_location = driver.find_elements('xpath' , url_info[5])#'//li[@data-at="job-item-location"]')
    description_extract = driver.find_elements('xpath' , url_info[6])#'//a[@data-offer-meta-text-snippet-link="true"]/span')
    link = driver.find_elements('xpath' , url_info[7])

  
    default_stored = 'None'
    length_of_job_title_list = len(job_title)

    final_result = {
        'job_id' : [],
        'title' : [default_stored]*length_of_job_title_list,
        'company' : [default_stored]*length_of_job_title_list,
        'posted_on' : [default_stored]*length_of_job_title_list,
        'date_of_extract' : [default_stored]*length_of_job_title_list,
        'salary' : [default_stored]*length_of_job_title_list,
        'location' : [default_stored]*length_of_job_title_list,
        'description' : [default_stored]*length_of_job_title_list,
        'link' : [default_stored]*length_of_job_title_list,
        'url_name' : [str(main_url)]*length_of_job_title_list
    }

    
        
    for i in range(length_of_job_title_list):
        try:
            final_result['job_id'].append(str(uuid4()))
            final_result['title'][i] = job_title[i].text #.append(job_title[i].text)
            final_result['company'][i] =  company_name[i].text if len(company_name) != 0 and len(company_name) == len(job_salary) else default_stored #.append(company_name[i].text)
            final_result['posted_on'][i] = date_convert(job_posting_duration[i].text) if len(job_posting_duration) != 0 and len(job_posting_duration) == len(job_salary) else date.today() #.append(date_convertor(job_posting_duration[i].text))
            final_result['date_of_extract'][i] = date.today() #.append(date.today())
            final_result['salary'][i] = job_salary[i].text if len(job_salary) != 0 and len(link) == len(job_salary) else default_stored#.append(job_salary[i].text)
            final_result['location'][i] = job_location[i].text if len(job_location) != 0 and len(link) == len(job_location) else default_stored#.append(job_location[i].text)
            final_result['description'][i] = description_extract[i].text if len(description_extract) and len(description_extract) == len(job_title) else default_stored#.append(description_extract[i].text)
            final_result['link'][i] = link[i].get_attribute('href') if len(link) != 0 and len(link) == len(job_title) else default_stored#.append(link[i].text)
        except Exception as error :
            print("error : " , error)
            exit()
            return final_result
        # result.append([job_title[i].text , partnership_with[i].text , company_name[i].text , 
        #                job_location[i].text ,job_posting_duration[i].text , job_salary[i].text , 
        #                description_extract[i].text])

    

    return final_result
    




