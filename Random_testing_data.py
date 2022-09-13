# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 13:00:26 2022

@author: Dhruv
"""


import pandas as pd
import psycopg2
import pandas
# import time,re
from sklearn.metrics.pairwise import cosine_similarity
# from sklearn.feature_extraction.text import TfidfVectorizer
# import datetime
# from sqlalchemy import create_engine
import numpy as np 
# import io
import re
import itertools
# main()
from datetime import datetime, timedelta
from dateutil import parser
import os


import datetime
import math
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()


os.chdir("D:\Aster\OneDrive_2021-12-07\Detailed_analysis")


##################################################################################################


################################################################################################
"""

Here in this py file 


We have 2 queries -

1. to extarct remote patient ids for 1000 random patients.(# random_pat_remote_id_query)
2. to extract all purchases/transactions  corresponing to these 1000 patients.(random_pat_data_query)
PS: Mentioned queries are commented in order to keep the 1000 patient remote ids same for all analysis
"""

###########################################################################################

##########################################################################################
"""
Pickled files 

In this code we have pickled three files named :
    1.random_pat_data_cnt(In this file we introduced the factor of frequency of an item purchased by patient more the frequency of an item purchased more is the weitage)
    2.random_pat_data_cnt1(In addition to the factor of frequency we have added recency factor for which we have added weitage corresponding to the date of purchase)
    3.random_pat_data_cnt2(same logic as 2. but slight change in weitage values)

"""
#------------weightlist---------------#

weight_list_1 = [75,15,10,5,1]
weight_list_2 = [60,25,10,5,1]
weight_list_3 = [60,25,10,5,0]
#########################################RANDOM PATIENT LIST FETCH QUERY#####################################################

# random_pat_remote_id_query='''

# select remoteid from
# (
# select distinct p.remoteid from
# s_pii.mobileindex m
# inner join s_patient.patientwithsrc p
# on
# m.id=p.mobileid
# inner join s_billing.billingsummary b 
# on
# p.remoteid =b.id 
# ) as q

# ORDER BY RANDOM()
# LIMIT 1000
# '''


random_pat_remote_id=pd.read_pickle('random_pat_remote_id')

#########################################RANDOM PATIENT TRANSACTION DATA FETCH QUERY#####################################################

random_pat_data_query='''


select i.patientremoteid,i.bookingdate,m.brand,t.brandid from(select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid) mb
inner join s_billing.items i on i.patientremoteid=mb.remoteid
inner join s_billing.meta m on m.id = i.billingmetaid
inner join thbbrandmapping t on t.id = m.id
where i.patientremoteid in {}
'''.format(tuple(random_pat_remote_id['patientremoteid']))

random_pat_data = pd.read_pickle('random_pat_data')

def freq_weightage(random_pat_data):
    """
    

    Parameters
    ----------
    random_pat_data-> Random Patient data for a certain duration.
    
    def rndm_patient_shapping : 
    -------------------------
        This function will help us in assigning weitage to every brand
        corresponding to every patient remote id. 
   
    """
    test_data=random_pat_data.copy()
    random_pat_data_cnt=test_data.value_counts(subset=['patientremoteid','brandid'],sort=False).reset_index()
    random_pat_data_cnt.rename(columns={random_pat_data_cnt.columns[2]:'frequency'},inplace=True)
    random_pat_data_cnt_total=random_pat_data_cnt[['patientremoteid','frequency']]
    random_pat_data_cnt_total=random_pat_data_cnt_total.groupby('patientremoteid').sum('frequency')
    random_pat_data_cnt_total.rename(columns={'frequency':'total'},inplace=True)
    random_pat_data_cnt=random_pat_data_cnt.merge(random_pat_data_cnt_total,how='left',on='patientremoteid')
    random_pat_data_cnt['weightage']=(random_pat_data_cnt['frequency']/random_pat_data_cnt['total'])*100
    adj_freq_pat_data=random_pat_data_cnt.drop(['frequency','total'],axis=1)
    return adj_freq_pat_data


def recency_weightage(random_pat_data,adj_freq_pat_data,weight_list):
        
    test_data=random_pat_data.copy()
    test_data_max_date=test_data.groupby(['patientremoteid','brandid'])['bookingdate'].max().reset_index()
    test_data_max_date.rename(columns={'bookingdate':'max_date'},inplace=True)
    test_data=test_data.merge(test_data_max_date,how='left',on=['patientremoteid','brandid'])
    test_data_max_date=test_data_max_date.groupby('patientremoteid')['max_date'].max()
    test_data_max_date=test_data_max_date.to_frame()
    test_data_max_date.rename(columns={'max_date':'max_date_pat'},inplace=True)
    test_data=test_data.merge(test_data_max_date,how='left',on=['patientremoteid'])
    test_data['days']=test_data['max_date_pat']-test_data['max_date']
    test_data['days']=test_data['days']/np.timedelta64(1,'D')
    test_data['weightage_recency']=[weight_list[0] if x <=60 else weight_list[1] if x<=120 else weight_list[2] if x<=180 else weight_list[3] if x<=540 else weight_list[4] for x in test_data['days']]
    test_data=test_data.drop_duplicates(['patientremoteid','brandid'],keep='last')
    test_data1=test_data[['patientremoteid','brandid','weightage_recency']]
    adj_freq_pat_data=adj_freq_pat_data.merge(test_data1,how='left',on=['patientremoteid','brandid'])
    adj_freq_pat_data['final_weightage']=adj_freq_pat_data['weightage_recency']*adj_freq_pat_data['weightage']
    adj_freq_recency_pat_data=adj_freq_pat_data[['patientremoteid','brandid','final_weightage']]
    return adj_freq_recency_pat_data






###################################     MAIN      #############################################
adj_freq_pat_data= freq_weightage(random_pat_data)
adj_freq_recency_pat_data=recency_weightage(random_pat_data,adj_freq_pat_data,weight_list_1)
adj_freq_recency_pat_data_wl2=recency_weightage(random_pat_data,adj_freq_pat_data,weight_list_2)
adj_freq_recency_pat_data_wl3=recency_weightage(random_pat_data,adj_freq_pat_data,weight_list_3)






############################################################################################
###########################################################################################

