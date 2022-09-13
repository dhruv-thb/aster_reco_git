# -*- coding: utf-8 -*-
"""
Created on Fri Dec 31 12:54:07 2021

@author: dhruv
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
import math
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()
      
 







########################################2020-2021#########################################
"""
Data Shapped 
"""


#################################################################################################

data_item_comb_cat_booking=run_query(data_extraction_query(brand_mapping_query,'2019-01-01', '2022-01-01'))
data_item_comb_cat_booking.to_pickle('data_item_com_bkn_id_19_22')
data_item_comb_cat_booking=pd.read_pickle('data_item_com_bkn_id_19_22')

item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
cosine_sim1=cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp)

# tf_idf_products=pd.read_csv('tf_idf_with_mobile.csv')
# tf_idf_products=tf_idf_products[['brandid','tf_idf_1']]
# tf_idf_products['brandid']=tf_idf_products['brandid'].astype('str')
# updated_cosin_sim=cosine_sim1.merge(tf_idf_products,how='left',left_on='secondproductid',right_on='brandid')
# updated_cosin_sim['value']=updated_cosin_sim['value']*updated_cosin_sim['tf_idf_1']
# updated_cosin_sim.drop(['tf_idf_1','brandid'],axis=1,inplace=True)
# updated_cosin_sim = updated_cosin_sim.sort_values(['firstproductid','value'],ascending=False)
# updated_cosin_sim1 = updated_cosin_sim.groupby(['firstproductid']).head(10).reset_index()

updated_cosin_sim1=tfidf(cosine_sim1)
final_result=final_shapping(updated_cosin_sim1,'2019-01-01','2022-01-01',brand_mapping_grp,item_comb)
final_result.to_csv('top_10_model_3.csv')






#######################################################################################################




####################################################################################################
data_item_only_customer_id=run_query(data_extraction_query(brand_mapping_query_customer,'2019-01-01', '2022-01-01'))
data_item_only_customer_id.to_pickle('data_item_only_customer_id')
data_item_only_customer_id=pd.read_pickle('data_item_only_customer_id')

item_comb,brand_mapping_grp=check_func(data_item_only_customer_id, 0)
cosine_sim1=cosin_sim_2(data_item_only_customer_id,100,brand_mapping_grp)
updated_cosin_sim1=tfidf(cosine_sim1)
Year2021_csv_1=final_shapping(updated_cosin_sim1,'2019-01-01','2022-01-01',brand_mapping_grp,item_comb)
Year2021_csv_1.to_csv('top_10_model_3_patient_id.csv')

