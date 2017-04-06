#!/usr/local/bin/python
# -*- coding: UTF-8 -*-
import re
from lxml import html
import requests
import mysql
import time
import os
import logging
import datetime
import sys
# get the maximum page number
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
page = requests.get(url='http://www.ziroom.com/z/nl/z2.html?qwd=',headers=headers)
tree = html.fromstring(page.content)
pages = tree.xpath('//*[@id="page"]/a')
maxUrl = pages[-1].attrib['href']
for i in pages:
    if i.attrib['href'] >maxUrl:
        maxUrl = i.attrib['href']
pageNum = re.findall(r'p=(\d+)',maxUrl)[0]

print pageNum
date =time.strftime("%Y_%m_%d")


# page = requests.get('http://www.ziroom.com/z/vr/60328018.html')
start = 'http://www.ziroom.com/z/nl/z2.html?qwd=&p=%s'
mainTable = 'ziruMain'
db_env = 'test'
db = 'ziroom'
prefix = '/home/why/ziroom_price/'
dir = prefix+date
subdir = dir + '/rooms'
if not os.path.exists(dir):
    os.makedirs(dir)
if not os.path.exists(subdir):
    os.makedirs(subdir)


logging.basicConfig(level=logging.INFO,
                    filename='%s/log'%dir, # log to this file
                    format='%(asctime)s %(message)s') # include timestamp


# get the current in mysql timestamp format
def current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

test = True
test = False

if test:
    mainRange = range(1,2)

else:
    mainRange = range(1,int(pageNum)+1)


def changeToInt(number):
    if type(number) != int:
        return int(number)
    else:
        return number


sqlSyntax = "select id from %s "%(mainTable)
logging.info(sqlSyntax)
res = mysql.query_db(sqlSyntax, db_env ,db)
all_id =[]
for i in res:
    all_id.append(i['id'])
if all_id== []:
    logging.info('something wrong, all id selected ftrom mysql is empty')
else:
    logging.info('selected %s id from mysql'%str((len(all_id))))


for i in mainRange:
    try:
        page = requests.get(start%str(i),timeout = 10)
    except Exception as e:
        logging.info(e)
        continue
    file = open('%s/page%s'%(dir,str(i)), 'w+')
    file.write(page.content)
    file.close()
    tree = html.fromstring(page.content)
    rooms = tree.xpath('//*[@id="houseList"]/li[@class="clearfix"]')

    if test:
        roomRange = range(1,2)
    else:
        roomRange = range(1,len(rooms)+1)
    logging.info('num of roompage is %s'%str(roomRange))
    for i in roomRange:
        name = tree.xpath('//*[@id="houseList"]/li[%s]/div[2]/h3/a'%str(i))[-1].text
        link = tree.xpath('//*[@id="houseList"]/li[%s]/div[2]/h3/a'%str(i))[-1].attrib['href']
        location = tree.xpath('//*[@id="houseList"]/li[%s]/div[2]/h4/a'%str(i))[-1].text
        priceString = tree.xpath('//*[@id="houseList"]/li[%s]/div[3]/p[1]/text()'%str(i))[0]
        sizeString = tree.xpath('//*[@id="houseList"]/li[%s]/div[2]/div/p[1]/span[1]/text()'%str(i))[0]
        size = re.findall(r'\s?(\d+)\s?', sizeString)[0]
        price = re.findall(r'\s?(\d+)', priceString)[0]
        price = changeToInt(price)
        try:
            room = requests.get('http:%s'%link,headers=headers,timeout = 10)
        except Exception as e:
            logging.info(e)
            continue
        subTree = html.fromstring(room.content)
        id = subTree.xpath('/html/body/div[3]/div[1]/div[3]/h3/text()')[-1]
        id = id.replace(' ', '')
        if id in all_id:
            all_id.remove(id)
            logging.info('%s processed, remove from list '%str(id))
        status = subTree.xpath('/html/body/div[3]/div[2]/div[3]/a[1]/text()')[-1]
        payment = subTree.xpath('/html/body/div[3]/div[2]/div[1]/p/span[2]/span[2]/text()')[-1]
        file = open('%s/page%s' % (subdir, id), 'w+')
        file.write(room.content)
        file.close()


        #print  price,link

        sqlSyntax = "select price,priceDrop,historyPrice,status,payment from %s where id = \"%s\""%(mainTable,id)
        logging.info(sqlSyntax)
        res = mysql.query_db(sqlSyntax, db_env ,db)
        if res ==[]:
            sqlSyntax = 'insert into %s (id,name,link,price,location,size,create_on,status,payment) VALUES (\"%s\",\"%s\",\"%s\",%s,\"%s\",%s,\"%s\",\"%s\",\"%s\")'%(mainTable,id,name,link,price,location,size,current_time(),status,payment)
            logging.info("sql return is empty,insert record with %s"%sqlSyntax)
            mysql.query_db(sqlSyntax, db_env, db)
        else:
            info = res[-1]
            oldPrice = info['price']
            oldPrice = changeToInt(oldPrice)
            historyPrice = info['historyPrice']
            priceDrop = info['priceDrop']
            oldStatus = info['status']
            oldPayment = info['payment']

            if oldPrice > price:
                logging.info('price for %s has dropped from %s to %s,update record'%(id,str(oldPrice),str(price)))
                if historyPrice !=None and str(oldPrice) not in historyPrice:
                    historyPrice = historyPrice+' '+str(oldPrice)
                else:
                    historyPrice = oldPrice
                if priceDrop !=None:
                    priceDrop = priceDrop+' '+date
                else:
                    priceDrop = date
                sqlSyntax = 'update %s set price=%s,PriceDrop=\"%s\",historyPrice=\"%s\",payment=\"%s\",status=\"%s\" where id = \"%s\"'%(mainTable,price,priceDrop,historyPrice,payment,status,id)
                mysql.query_db(sqlSyntax, db_env, db)
                logging.info('update record with %s '%sqlSyntax)
            else:
                if oldPrice != price:
                    logging.info('price changed from %s to %s,update record'%(str(oldPrice),str(price)))
                    if historyPrice !=None :
                        historyPrice = historyPrice+' '+str(oldPrice)
                    else:
                        historyPrice = oldPrice
                    sqlSyntax = 'update %s set price=%s,historyPrice=\"%s\",status=\"%s\",payment=\"%s\" where id = \"%s\"'%(mainTable,price,historyPrice,status,payment,id)
                    mysql.query_db(sqlSyntax, db_env, db)
                    logging.info('update record with %s '%sqlSyntax)
                elif payment!= oldPayment:
                    logging.info('payment changed from %s to %s,update record'%(oldPayment,payment))
                    sqlSyntax = 'update %s set payment=\"%s\",status=\"%s\" where id = \"%s\"'%(mainTable,payment,status,id)
                    mysql.query_db(sqlSyntax, db_env, db)
                    logging.info('update record with %s '%sqlSyntax)
                elif status!=oldStatus:
                    logging.info('status changed from %s to %s,update record'%(oldStatus,status))
                    sqlSyntax = 'update %s set payment=\"%s\",status=\"%s\" where id = \"%s\"'%(mainTable,payment,status,id)
                    mysql.query_db(sqlSyntax, db_env, db)
                    logging.info('update record with %s '%sqlSyntax)
                else:
                    logging.info('price for id %s is the same'%id)


def get_field_by_field(field_wanted,field_condition,field_value):
    sqlSyntax = "select %s  from %s where %s = \"%s\""%(field_wanted,mainTable,field_condition,field_value)
    logging.info(sqlSyntax)
    res = mysql.query_db(sqlSyntax, db_env ,db)
    return res[-1]['link']


def get_tree_spath(addr):
    try:
        room = requests.get('http:%s'%addr,headers=headers,timeout = 10)
        return html.fromstring(room.content)
    except Exception as e:
        logging.info(e)
        return ''

def operate_one_id(id):
    link = get_field_by_field('link','id',id)
    print link
    subtree = get_tree_spath(link)
    if subTree!='':
        try:
            status = subtree.xpath('/html/body/div[3]/div[2]/div[3]/a[1]/text()')[-1]
        except Exception as e:
            logging.info('can not get the status of the page')
            status = 'lost'
        sqlSyntax = 'update ziruMain set update_on= DATE_ADD(update_on,INTERVAL 1 DAY),status=\"%s\" where id=\"%s\"'%(status,id)
        logging.info(sqlSyntax)


        res = mysql.query_db(sqlSyntax, db_env ,db)
        logging.info(res)
    else:
        logging.info('failed to get subtree for page %s'%link)
        return

def after_operate_current_page(all_id):
    logging.info('after query all the pages, still %s id lefted '%str(len(all_id)))
    for i in all_id:
        operate_one_id(i)


after_operate_current_page(all_id)