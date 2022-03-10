#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 20:33:40 2022

@author: raj
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from database import postgres as ps
import time
from fake_useragent import UserAgent 

def get_Amazon_products_url(abs_file_path):
    """Function to get the list of amazon products url list"""
    amazon_products_url = []
    amazon_data = pd.read_csv(abs_file_path)
    for index, unamed,  id , asin , country in amazon_data.itertuples():
        amazon_product_url = "https://www.amazon."+country+"/dp/"+asin 
        amazon_products_url.append(amazon_product_url)
    return amazon_products_url

def dump_json_file(abs_file_path, data):
    """Function to dump the product data in the json file"""
    with open(abs_file_path ,'w') as file:
        json.dump(data,file)
        
def get_Amazon_Content(productUrl,arg_ua):
    """Function to get the html content"""
    user_agent = arg_ua.random
    headers = {
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': user_agent,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://www.google.com/',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
    
    res = requests.get(productUrl, headers=headers)
    print("{} status code: {}".format(productUrl,res.status_code))
    if res.status_code == 200:
        return res.content
    else:
        print("{} not available".format(productUrl))
        return None
    
def get_Product_title(soup):
    """Function to get the title of the product"""
    #select the center col
    centre_col = soup.find("div", attrs={"id":"centerCol"})
    try :
        product_title = centre_col.find("span" , attrs={"id":"productTitle"}).string.strip()
    except AttributeError:
        product_title = "NA"
    return product_title

def get_Product_details(soup):
    """Function to get the product details"""
    #Initilaise the product details dictionary
    product_details_dic = {}
    #first find all possible table or bullets for product details
    product_table = soup.find("table", attrs={"class":"a-keyvalue prodDetTable"})
    product_bullets = soup.find("div",attrs={"data-feature-name":"detailBullets"})
    if product_table != None:
        product_table_tr = product_table.findAll('tr')
        for tr in product_table_tr:
            product_detail_key = tr.find('th').text.strip().replace('\n',"").encode('ascii', 'ignore').decode('ascii')
            product_detail_value = tr.find('td').text.strip().replace('\n',"").encode('ascii', 'ignore').decode('ascii')
            product_details_dic[product_detail_key] = product_detail_value
    #if the product table not found check product bullets
    elif product_bullets != None:
        product_details_div = product_bullets.find("div" , attrs={"id":"detailBullets_feature_div"})
        product_details_ul = product_details_div.find("ul")
        for li in product_details_ul.findAll('li'):
            if li.text is not None:
                product_details = li.text.strip().replace("\n","").encode('ascii', 'ignore').decode('ascii')
                product_split = product_details.split(":")
                product_details_dic[product_split[0].strip()] = product_split[1].strip()
    
    return product_details_dic
                
def get_Product_image_url(soup):
    """Function to get the product image url"""
    #select the left col 
    left_col = soup.find("div", attrs={"id":"leftCol"})
    #get the image url
    if left_col != None:
        product_image_url = left_col.find("img").get("src")
    else:
        product_image_url = 'NA'
    return product_image_url
    
def get_Product_price(soup):
    """Function to get the product price"""
    key_name = re.compile('.*price.*')
    #select center col
    centre_col = soup.find("div", attrs={"id":"centerCol"})
    try :
        product_price = centre_col.find("span" , attrs={"class":key_name}).string.strip().replace("\n","").encode('ascii', 'ignore').decode('ascii')
    except AttributeError:
        product_price = 'NA'
    return product_price


def scrap_data(soup):
    """Function to scarp data from the soup"""
    product_title = get_Product_title(soup)
    product_image = get_Product_image_url(soup)
    product_price = get_Product_price(soup)
    product_details = get_Product_details(soup)
    
    return product_title , product_image, product_price , product_details
    
  
def main():
    """Main function"""
    
    #path to csv file
    csv_file_path = 'Amazon Scraping - Sheet1.csv'
    
    #database configuration file
    config_filename = 'database.ini'
    
    #get amazon url list
    amazon_products_url_list = get_Amazon_products_url(csv_file_path)
    
    #proxies
    #proxies={'160.16.77.108:3128', '20.195.17.90:3128', '14.225.5.68:80', '158.46.127.222:52574', '59.192.130.233:8080', '124.106.224.5:8080', '51.79.157.202:443', '161.202.226.194:80'}

    #amazon products data dictionary
    amazon_products_data = {}
    
    #counter varaibles
    success_scrap = 0
    count = 0
    
    #connect to postgres database
    postgres = ps.PostgresConn(config_filename)
    postgres.connect_to_server()
    
    #create amazon product table
    postgres.create_amazon_product_table()
    
    #iterate over the urls
    for url in amazon_products_url_list:
        
        #new user agent
       # user_agent = 'raj/python/'+str(count)
       
        #fake user agent
        ua = UserAgent(use_cache_server=False)
        
        #get website content
        content = get_Amazon_Content(url,ua)
        
        #if content is not none and 100 success scrap is not yet done continue to scrap the data
        if content != None and success_scrap < 100 :
            
            #temproray dictionary to store information of a product
            
            amazon_product = {}
            
            #create soup using html praser "lxml"
            soup = BeautifulSoup(content, "lxml")
            
            #scrap the data
            product_title , product_image , product_price , product_details = scrap_data(soup)

            amazon_product['product_title'] = product_title
            amazon_product['product_image'] = product_image
            amazon_product['product_price'] = product_price
            amazon_product['product_details'] = product_details
            
            
            if product_title != 'NA':
                
                #increament on success scrap
                success_scrap += 1
                
                #product id for each  product data scrape from the web
                product_id = 'Porudct_'+str(success_scrap)
                
                #add product information to the amazon_products_data dictionary
                amazon_products_data[product_id] = amazon_product
                
                #sql insert value variable
                insert_value = (product_id, product_title,product_image,json.dumps(product_details),product_price)
                
                
                #insert value in the database
                postgres.insert_into_table('amazonProducts',insert_value)
            
        #increase count for new user agent
        count  += 1      
        
        #sleep for 2 seconds
        time.sleep(2)
        
    #dump the json file
    dump_json_file('products_data.json', amazon_products_data)
    
    #close the connection 
    postgres.close_connection()
    
if __name__ == "__main__":
    main()
    
