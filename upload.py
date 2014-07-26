#!/usr/bin/python
#coding: utf8
from lxml import etree
import sys
from os import *
import fdb
import logging
import datetime
import timeit
import time
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startTime)
        st=u"Elapsed time:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._startTime)
        logging.info(st)
def getgenerator(cur,gen):
 sq="SELECT GEN_ID("+gen+", 1) FROM RDB$DATABASE"
 try:
  cur.execute(sq)
 except:
  print "err"
 cur.execute(sq)
 r=cur.fetchall()
 try:
  g=r[0][0]
 except:
  g=-1
 return g
def quoted(a):
 st="'"+a+"'"
 return st
def main():
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',level = logging.DEBUG, filename = './upload.log')
 fileconfig=file('./config.xml')
 xml=etree.parse(fileconfig)
 xmlroot=xml.getroot()
 nd=xmlroot.find('input_path')
 input_path=nd.text
 
 nd=xmlroot.find('input_arc_path')
 input_arc_path=nd.text

 main_database=xmlroot.find('main_database')
 main_dbname=main_database.find('dbname').text
 main_user=main_database.find('user').text 
 main_password=main_database.find('password').text
 main_host=main_database.find('hostname').text
 
 rbd_database=xmlroot.find('rbd')
 rbd_dbname=rbd_database.find('dbname').text
 rbd_user=rbd_database.find('user').text
 rbd_password=rbd_database.find('password').text
 rbd_host=rbd_database.find('hostname').text
 clm=' , '
 
 try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)


 print input_path
 #input_path='/home/chief/work/gai/'
 #ff='for_UFSSP_20131219_184058.txt'
 for ff in listdir(input_path):
  with Profiler() as p:
   print "Начинаем обрабатывать файл:"+ff
   logging.info(u"Начинаем обрабатывать файл:"+ff)
   f=file(input_path+ff)
   l=f.readlines()
   periodstr=l[3].decode('CP1251').partition(u'период (дата технологической операции) :')
   pp=periodstr[2].lstrip(' ')
   date_start=pp.partition(u'Начало -')[2].lstrip(' ').split(' ')[0]
   date_end=pp.partition(u'до окончание -')[2].lstrip(' ').split(' ')[0]
   #print date_start,date_end
   oficerstr=l[4].decode('CP1251').partition(u'Файл сформирован :')
   oficer=oficerstr[2].lstrip(' ')
   oficer=oficer.replace(chr(13),'')
   oficer=oficer.replace(chr(10),'')
   date_act=l[5].decode('CP1251').partition(u'Дата и время формирования :')[2].lstrip(' ').split(' ')[0]
   #print  date_act
   #pp=l[6].decode('CP1251').split(';')
   #print pp[1],pp
   sql='INSERT INTO REESTRS (ID, PACKET_ID, NUM_ID, DATE_ID, LASTNAME, FISTNAME_OGRN, SECONDNAME_INN, SUMM, DATE_ISP, MARK_ISP, PRIM, FILENAME, DATE_ACT, PERIOD_START, PERIOD_END, FIO_OFICER, STATUS, OSP, NUM_IP, OUTFILENAME, DATE_OUT, OUT_PACKET_ID) VALUES ('
   cur = con.cursor()
   #id=getgenerator(cur,'SEC_REESTRS')
   pack_id=getgenerator(cur,'SEC_REESTRS_PACK')
   for i in range(6,len(l)):
    #print l[i].decode('CP1251')
    id=getgenerator(cur,'SEC_REESTRS')
    pp=l[i].decode('CP1251').split(';')
    #print id,'LEN,',len(pp)
    #print l[i]
    #print DD pp[5]
    try:
     dd=datetime.datetime.strptime(pp[7], '%d.%m.%Y')
    except Exception, e :
     date_isp='null'
     logging.error(u'Файл:'+ff+u". Отстутвует дата исполнения, заменил на null "+str(e) )
     logging.info(l[i].decode('CP1251'))
    else:
     date_isp=quoted(pp[7])
    sql2=sql+str(id)+clm+str(pack_id)+clm+quoted(pp[1])+clm+quoted(pp[2])+clm+quoted(pp[3])+clm+quoted(pp[4])+clm+quoted(pp[5])+clm+(pp[6])+clm+date_isp+clm+quoted(pp[8])+clm+quoted(pp[9])+clm+quoted(ff)+clm+quoted(date_act)+clm+quoted(date_start)+clm+quoted(date_end)+clm+quoted(oficer)+',0, null,null,null,null,null)'
    #print sql2
    try:
     cur.execute(sql2)
    except  Exception, e:
     logging.error(u'Файл:'+ff+u'. Ошибка в скрипте:')
     if pp[6]=='':
      logging.error(u'Отсутствует сумма оплаты')
     logging.info(l[i].decode('CP1251'))
     logging.error(sql2)
   con.commit()
   f.close()
  rename(input_path+ff, input_arc_path+ff)
 fileconfig.close()
 con.close() 
if __name__ == "__main__":
    main()
