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
def inform(st):
 logging.info(st)
 print st
 return
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        #print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startTime)
        st=u"Время выполнения:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._startTime)
        print st
        logging.info(st)
def quoted(a):
 st=u"'"+a+u"'"
 return st
def main():
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',level = logging.DEBUG, filename = './processing.log')
 fileconfig=file('./config.xml')
 xml=etree.parse(fileconfig)
 xmlroot=xml.getroot()

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
 try:
   con2 = fdb.connect (host=rbd_host, database=rbd_dbname, user=rbd_user,  password=rbd_password,charset='WIN1251')
 except  Exception, e:
  print("Ошибка при открытии базы данных:\n"+str(e))
  sys.exit(2)
 fileconfig.close()
 cur = con.cursor()
 cur2 = con2.cursor()
 
 sql2="SELECT doc_ip_doc.id, document.doc_number, doc_ip_doc.id_dbtr_name,(REPLACE (doc_ip_doc.id_docno,' ','')) as NUMDOC ,doc_ip.id_debtsum,document.docstatusid FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID "  #  where     upper (REPLACE (doc_ip_doc.id_docno,' ','')) ="
 sql='SELECT * FROM reestrs where status=0'
 sql4="select 'update reestrs set reestrs.num_ip='''||doc_ip_doc.doc_number|| ''', reestrs.osp='''|| substring (doc_ip_doc.id from 1 for 4)|| ''' whrere reestrs.id='|| reestrs.id ||';' from reestrs reestrs join doc_ip_doc on reestrs.num_id=doc_ip_doc.numdoc and doc_ip_doc.docstatusid=9"
 sql5=u"SELECT cast('INSERT INTO docipdoc (ID, DOC_NUMBER, ID_DBTR_NAME, NUMDOC, ID_DEBTSUM, DOCSTATUSID) VALUES ('|| doc_ip_doc.id ||', ''' || document.doc_number||''', '''|| doc_ip_doc.id_dbtr_name||''',''' ||(REPLACE (doc_ip_doc.id_docno,' ','')) ||''','|| doc_ip.id_debtsum||' , '|| document.docstatusid||');' as varchar(1000)) FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID  where "# (doc_ip.ip_risedate<'01.01.2013' and document.docstatusid=9) or (doc_ip.ip_risedate>='01.01.2013')"
 sql6="SELECT  doc_ip_doc.id , document.doc_number, doc_ip_doc.id_dbtr_name,(REPLACE (doc_ip_doc.id_docno,' ','')) ,doc_ip.id_debtsum, document.docstatusid FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID  where"  
 sql7="INSERT INTO docipdoc (ID, DOC_NUMBER, ID_DBTR_NAME, NUMDOC, ID_DEBTSUM, DOCSTATUSID) VALUES (?,?,?,?,?,?)"
 sql8="select docipdoc.doc_number,  substring (docipdoc.id from 1 for 4), reestrs.id from reestrs reestrs join docipdoc on (reestrs.num_id=docipdoc.numdoc and docipdoc.docstatusid=9 and reestrs.status=0)"
 sql9="select docipdoc.doc_number,  substring (docipdoc.id from 1 for 4), reestrs.id from reestrs reestrs join docipdoc on (reestrs.num_id=docipdoc.numdoc and docipdoc.docstatusid<>9 and reestrs.status=0)"
#cur.execute(sql)
 start_date='01.01.2013'
 if sys.argv[1]=='loadrbd':
  sq= sql6+"(doc_ip.ip_risedate<"+quoted(start_date)+" and document.docstatusid=9) or (doc_ip.ip_risedate>="+quoted(start_date)+") or (doc_ip.ip_risedate is null)"
  print sq
  st=u"Генерация скрипта вставки данных из РБД во временную таблицу"
  logging.info(st)
  print st
  with Profiler() as p:
   cur2.execute(sq)
   r=cur2.fetchall()
   print r[0]
  st=u"Выбрано " +str(len(r))+ u"записей"
  logging.info(st)
  print st
  cur.execute("delete from docipdoc")
  con.commit()
  st=u"Вставляем выбранные записи во временную таблицу:"
  logging.info(st)
  print st
  with Profiler() as p:
   for i in range(0,len(r)):
    cur.execute (sql7,r[i])
  st=u"Меряем время commitа :"
  logging.info(st)
  print st
  with Profiler() as p:
   con.commit()
  #st=u"Сохраняем скрипт в файл:"
  #logging.info(st)
  #print st
  #f=file('./rbd.sql','w')
  #for i in r:
  # f.write(rr)
  # f.close()
 if sys.argv[1]=='process':
  inform(u"Начинаем поиск соответствий номеров ИД ГИБДД и РБД ИП на исполнении: ")
  with Profiler() as p:
   cur.execute(sql8)
   r=cur.fetchall()
  inform(u"Найдено "+str(len(r))+u" соответствий, ИП в исполнении")
  inform(u"Герерируем и сразу исполняем скрипт обработки:")
  with Profiler() as p:
   for i in range(0,len(r)):
    sq="update reestrs set status=1, num_ip="+quoted(r[i][0])+", osp="+quoted(r[i][1])+" where id="+str(r[i][2])
    #print sq
    cur.execute(sq)
  inform(u"Меряем время коммита:")
  with Profiler() as p:
   con.commit()
  inform(u"Начинаем поиск соответствий номеров ИД ГИБДД и РБД ИП не в исполнении: ")
  with Profiler() as p:
   cur.execute(sql9)
   r=cur.fetchall()
  inform(u"Найдено "+str(len(r))+u" соответствий, ИП не в исполнении")
  inform(u"Герерируем и сразу исполняем скрипт обработки:")
  with Profiler() as p:
   for i in range(0,len(r)):
    sq="update reestrs set status=3, num_ip="+quoted(r[i][0])+", osp="+quoted(r[i][1])+" where id="+str(r[i][2])
    #print sq
    cur.execute(sq)
  inform(u"Меряем время коммита:")
  with Profiler() as p:
   con.commit()
 if sys.argv[1]=='get':
  sq="select * from reestrs where status=1"
  inform(u"Выбираем готовые к выгрузке платежные документы:")
  with Profiler() as p:
   cur.execute(sq)
   r=cur.fetchaall()
  inform(u"Найдено "+str(len(r))+u" записей")
  
    

 con2.close()
 con.close()
 fileconfig.close()
if __name__ == "__main__":
    main()
