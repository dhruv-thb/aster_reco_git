# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 11:48:03 2022

@author: Ankur
"""

import pandas as pd
import psycopg2
# import time,re
# from sklearn.feature_extraction.text import TfidfVectorizer
# import datetime
# from sqlalchemy import create_engine
import numpy as np 
# import io
import os
import re
import itertools
import matplotlib.pyplot as plt




############################################ FILES ########################################################
cosin_customer=pd.read_csv("cosin_sim_all_customer_id_without_filter.csv")
cosin_booking=pd.read_csv('cosin_sim_all_booking_id.csv')
brand_mapping_grp=pd.read_csv('brand_mapping_grp.csv')
brand_mapping_grp['brandid']=brand_mapping_grp['brandid'].astype('int64')
cosin_booking=cosin_booking.merge(brand_mapping_grp,left_on='brandid1',right_on='brandid',how='left')
cosin_booking=cosin_booking.merge(brand_mapping_grp,left_on='brandid2',right_on='brandid',how='left')

cosin_booking.to_csv('cosin_sim_all_booking_id.csv')


tfidf_booking=pd.read_csv('tfidf_booking_all.csv')
tfidf_customer=pd.read_csv('tfidf_customer_all.csv')

bkng_total_count=cosin_booking.copy()
bkng_total_count=bkng_total_count[['brandid2','freq_brand_2_bkng']]
bkng_total_count=bkng_total_count.drop_duplicates(subset=['brandid2', 'freq_brand_2_bkng'], keep='last')


cust_total_count=cosin_customer.copy()
cust_total_count=cust_total_count[['brandid2','freq_brand_2']]
cust_total_count=cust_total_count.drop_duplicates(subset=['brandid2','freq_brand_2'], keep='last')
os.chdir("D:\Aster\OneDrive_2021-12-07\Detailed_analysis")
random_pat_data_unique=pd.read_pickle('random_pat_data_unique')
random_pat_data_unique['brandid']=random_pat_data_unique['brandid'].astype('int64')

########################################################################################################

cosin_customer_analysis=cosin_customer.copy()
cosin_customer_analysis = cosin_customer_analysis[(cosin_customer_analysis['value']>0.0001)&(cosin_customer_analysis['value']<0.99)]
cosin_customer_analysis_group_by= cosin_customer_analysis.groupby('brandid2')['value'].agg(['min', 'max','median', 'count']).reset_index()
cosin_customer_analysis=cosin_customer_analysis_group_by.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
del cosin_customer_analysis['brandid2']
del cosin_customer_analysis['Unnamed: 0']
os.chdir(r"D:\Aster\csv_and_pickle_files\6_model_analysis")
cosin_customer_analysis=cosin_customer_analysis.merge(cust_total_count,how='left',left_on='brandid',right_on='brandid2')
cosin_customer_analysis.to_csv('cosin_customer_analysis.csv')
########################################################################################################




#############################################################################################################
cosin_booking_analysis=cosin_booking.copy()
cosin_booking_analysis=cosin_booking_analysis[['brandid1','brandid2','value_bkng']]
cosin_booking_analysis = cosin_booking_analysis[(cosin_booking_analysis['value_bkng']>0.0001)&(cosin_booking_analysis['value_bkng']<0.99)]
cosin_booking_analysis_group_by=cosin_booking_analysis.groupby('brandid2')['value_bkng'].agg(['min', 'max','median', 'count']).reset_index()
cosin_booking_analysis=cosin_booking_analysis_group_by.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
os.chdir(r"D:\Aster\csv_and_pickle_files\6_model_analysis")
cosin_booking_analysis=cosin_booking_analysis.merge(bkng_total_count,how='left',left_on='brandid',right_on='brandid2')
cosin_booking_analysis.to_csv('cosin_booking_analysis.csv')
######################################################################################################


##########################################################################################################
tfidf_cosin_customer=cosin_customer.copy()
tfidf_cosin_customer=tfidf_cosin_customer[['brandid1','brandid2','value']]
tfidf_cosin_cust_analysis=tfidf_customer.copy()
tfidf_cosin_cust_analysis=tfidf_cosin_cust_analysis[['brandid1','brandid2','tf_idf']]
tfidf_cosin_customer=tfidf_cosin_customer.merge(tfidf_cosin_cust_analysis,how='left',on=['brandid1','brandid2'])
tfidf_cosin_customer['updated_value']=tfidf_cosin_customer['tf_idf']*tfidf_cosin_customer['value']
tfidf_cosin_customer=tfidf_cosin_customer[['brandid1','brandid2','updated_value']]
tfidf_cosin_customer = tfidf_cosin_customer[(tfidf_cosin_customer['updated_value']>0.0001)&(tfidf_cosin_customer['updated_value']<0.99)]
tfidf_cosin_customer_analysis=tfidf_cosin_customer.groupby('brandid2')['updated_value'].agg(['min', 'max','median','mean', 'count']).reset_index()
tfidf_cosin_customer_analysis=tfidf_cosin_customer_analysis.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
os.chdir(r"D:\Aster\csv_and_pickle_files\6_model_analysis")
tfidf_cosin_customer_analysis=tfidf_cosin_customer_analysis.merge(cust_total_count,how='left',left_on='brandid',right_on='brandid2')
tfidf_cosin_customer_analysis.to_csv('tfidf_cosin_customer_analysis.csv')

##########################################################################################################

tfidf_cosin_booking=cosin_booking.copy()
tfidf_cosin_booking=tfidf_cosin_booking[['brandid1','brandid2','value_bkng']]
tfidf_cosin_bkng_analysis=tfidf_booking.copy()
tfidf_cosin_bkng_analysis=tfidf_cosin_bkng_analysis[['brandid1','brandid2','tf_idf']]
tfidf_cosin_booking=tfidf_cosin_booking.merge(tfidf_cosin_bkng_analysis,how='left',on=['brandid1','brandid2'])
tfidf_cosin_booking['updated_value']=tfidf_cosin_booking['tf_idf']*tfidf_cosin_booking['value_bkng']
tfidf_cosin_booking=tfidf_cosin_booking[['brandid1','brandid2','updated_value']]
tfidf_cosin_booking = tfidf_cosin_booking[(tfidf_cosin_booking['updated_value']>0.0001)&(tfidf_cosin_booking['updated_value']<0.99)]
tfidf_cosin_booking_analysis=tfidf_cosin_booking.groupby('brandid2')['updated_value'].agg(['min', 'max','median','mean', 'count']).reset_index()
tfidf_cosin_booking_analysis=tfidf_cosin_booking_analysis.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
os.chdir(r"D:\Aster\csv_and_pickle_files\6_model_analysis")
tfidf_cosin_booking_analysis=tfidf_cosin_booking_analysis.merge(bkng_total_count,how='left',left_on='brandid',right_on='brandid2')
tfidf_cosin_booking_analysis.to_csv('tfidf_cosin_booking_analysis.csv')









#########################################################################################


cosine_sim1=cosin_customer.copy()
top_10_per_pat=random_pat_data_unique.merge(cosine_sim1,how='left',left_on='brandid',right_on='brandid1')
top_10_per_pat_filter=top_10_per_pat.copy()
top_10_per_pat_filter=top_10_per_pat_filter[top_10_per_pat_filter['value']>=0.1]
final_result=top_10_per_pat_filter.groupby(['patientremoteid','brandid2']).sum('value').reset_index()
final_result =final_result.sort_values(['patientremoteid','value'],ascending=False)
final_result=final_result.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
final_result=final_result[final_result['servicetype']!='pharma classification']
final_result1=final_result.groupby(['patientremoteid']).head(10).reset_index()
final_result1["rank_cust_cosin"] = final_result1.groupby("patientremoteid")["value"].rank("dense", ascending=False)
os.chdir("D:\Aster\OneDrive_2021-12-07\Detailed_analysis")
final_result1.to_csv('top_10_cosin_cust_value_max1.csv')



check=cosine_sim1[(cosine_sim1['brandid2']==4764)]
check=check.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
check=check[check['servicetype']!='pharma classification']


check.to_csv('osteocare.csv')

