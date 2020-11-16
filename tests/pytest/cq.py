###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import threading
import taos
import sys
import json
import time
import random
# query sql
query_sql = [
]
func_list=['avg','count','twa','sum','stddev','leastsquares','min',
'max','first','last','top','bottom','percentile','apercentile',
'last_row','diff','spread']
condition_list=[
    "where _c0 > now -10d ",
    'interval(10s)',
    'limit 10',
    'group by',
    'order by',
    'fill(null)'
    
]
class ConcurrentInquiry:
    def initConnection(self):  
        self.numOfTherads = 50
        self.ts=1500000001000
        self.dbname='test'
        self.stb_list=[]
        self.subtb_list=[]
        self.stb_stru_list=[]
        self.subtb_stru_list=[]
        self.stb_tag_list=[]
        self.subtb_tag_list=[]
    def SetThreadsNum(self,num):
        self.numOfTherads=num
    def ret_fcol(self,cl,sql):                     #返回结果的第一列
        cl.execute(sql)
        fcol_list=[]
        for data in cl:
            fcol_list.append(data[0])
        return fcol_list
    def r_stb_list(self,cl):
        sql='show '+self.dbname+'.stables'
        self.stb_list=self.ret_fcol(cl,sql)
    def r_subtb_list(self,cl,stablename):
        sql='select tbname from '+self.dbname+'.'+stablename+' limit 2;'
        self.subtb_list+=self.ret_fcol(cl,sql)
    def cal_struct(self,cl,tbname):
        tb=[]
        tag=[]
        sql='describe '+self.dbname+'.'+tbname+';'
        cl.execute(sql)
        for data in cl:
            if data[3]:
                tag.append(data[0])
            else:
                tb.append(data[0])
        return tb,tag
    def r_stb_stru(self,cl):
        for i in self.stb_list:
            tb,tag=self.cal_struct(cl,i)
            self.stb_stru_list.append(tb)
            self.stb_tag_list.append(tag)
    def r_subtb_stru(self,cl):
        for i in self.subtb_list:
            tb,tag=self.cal_struct(cl,i)
            self.subtb_stru_list.append(tb)
            self.subtb_tag_list.append(tag)
    def get_full(self):
        host = "127.0.0.1"
        user = "root"
        password = "taosdata"
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        self.r_stb_list(cl)
        for i in self.stb_list:
            self.r_subtb_list(cl,i)
        self.r_stb_stru(cl)
        self.r_subtb_stru(cl)
        #print(self.stb_list,self.subtb_list,self.stb_stru_list,self.stb_tag_list,self.subtb_stru_list,self.subtb_tag_list)
        cl.close()
        conn.close()   
    def gen_query_sql(self):
        tbi=random.randint(0,len(self.subtb_list)+len(self.stb_list))
        tbname=''
        col_list=[]
        tag_list=[]
        is_stb=0
        if tbi>len(self.stb_list) :
            tbi=tbi-len(self.stb_list)
            tbname=self.subtb_list[tbi-1]
            col_list=self.subtb_stru_list[tbi-1]
            tag_list=self.subtb_tag_list[tbi-1]
        else:
            tbname=self.stb_list[tbi-1]
            col_list=self.stb_stru_list[tbi-1]
            tag_list=self.stb_tag_list[tbi-1]
            is_stb=1
        tlist=col_list+tag_list
        con_rand=random.randint(0,len(condition_list))
        func_rand=random.randint(0,len(func_list))
        col_rand=random.randint(0,len(col_list))
        tag_rand=random.randint(0,len(tag_list))
        t_rand=random.randint(0,len(tlist))
        sql='select 1'
        random.shuffle(col_list)
        for i in col_list[0:col_rand]:
            if func_list[func_rand-1] == 'leastsquares':
                sql+=','+func_list[func_rand-1]+'('+i+',1,1)'
            elif func_list[func_rand-1] == 'top' or func_list[func_rand-1] == 'bottom' or func_list[func_rand-1] == 'percentile' or func_list[func_rand-1] == 'apercentile':
                sql+=','+func_list[func_rand-1]+'('+i+',1)'
            else:
                sql+=','+func_list[func_rand-1]+'('+i+')'
        sql+=' from '+tbname
        if condition_list[con_rand-1].startswith('w'):
            sql+=' '+condition_list[con_rand-1]+'and '+tlist[t_rand-1]+'>20'
        elif condition_list[con_rand-1].startswith('o'):
            sql+=' '+condition_list[con_rand-1]+' '+tlist[t_rand-1]
        elif condition_list[con_rand-1].startswith('g'):
            sql+=' '+condition_list[con_rand-1]+' '+tlist[t_rand-1]
        else :
            sql+=' '+condition_list[con_rand-1]
        return sql
    def query_thread(self,threadID):
        host = "127.0.0.1"
        user = "root"
        password = "taosdata"
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        cl.execute("use test;")
        
        print("Thread %d: starting" % threadID)
        
        while True:
            
                try:
                    sql=self.gen_query_sql()
                    print("sql is ",sql)
                    start = time.time()
                    cl.execute(sql)
                    cl.fetchall()
                    end = time.time()
                    print("time cost :",end-start)
                except Exception as e:
                    print(
                "Failure thread%d, sql: %s,exception: %s" %
                (threadID, str(sql),str(e)))
                    #exit(-1)
                    
                
        print("Thread %d: finishing" % threadID)
          
        

    def run(self):
            
        threads = []
        for i in range(self.numOfTherads):
            thread = threading.Thread(target=self.query_thread, args=(i,))
            threads.append(thread)
            thread.start()  
        
q = ConcurrentInquiry()
q.initConnection()
q.get_full()
#q.gen_query_sql()
q.run()

