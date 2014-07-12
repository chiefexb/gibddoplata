#!/usr/bin/python
#coding: utf8
def main():
 input_path='/home/gibdd/in'
 f=file(input_path+'/for_UFSSP_20131219_184058.txt')
 l=f.readlines()
 periodstr=l[3].decode('CP1251').partition(u'период (дата технологической операции) :')
 pp=periodstr[2].lstrip(' ')
 date_start=pp.partition(u'Начало -')[2].lstrip(' ').split(' ')[0]
 date_end=pp.partition(u'до окончание -')[2].lstrip(' ').split(' ')[0]
 print date_start,date_end
 oficerstr=l[4].decode('CP1251').partition(u'Файл сформирован :')
 oficer=oficerstr[2].lstrip(' ')
 date_act=l[5].decode('CP1251').partition(u'Дата и время формирования :')[2].lstrip(' ').split(' ')[0]
 print  date_act
if __name__ == "__main__":
    main()
