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
import os
import re
from Common_func import * 
import Model_2_func as md2
from queries import * 
import itertools
from dateutil.relativedelta import relativedelta
# main()
import math
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()
      
 


##########################################   PATHS  #######################################################

#######################################################################################################

path_parmanent_pickle=r"D:\Aster\Parmanent_pickle"

path_secured_data_files=r"D:\Aster\Secured_data_approach"

csv_files=r"D:\Aster\csv_and_pickle_files\Parmanent CSV"

only_ocd=r"D:\Aster\Only_OTC_data"

no_filter=r"D:\Aster\No_filter_cosine_similarities"

########################################################################################################






########################################2020-2021#########################################
"""
Data Shapped 
"""


#################################################################################################

# data_item_comb_cat_booking=run_query(data_extraction_query(brand_mapping_query,'2019-01-01', '2022-01-01'))
# data_item_comb_cat_booking.to_pickle('data_item_com_bkn_id_19_22')

data_item_comb_cat_booking=pd.read_pickle(r'D:\Aster\csv_and_pickle_files\data_item_com_bkn_id_19_22')


item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
cosine_sim1=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,True)

updated_cosin_sim1=tfidf(cosine_sim1,True)

final_result=final_shapping(updated_cosin_sim1,brand_mapping_grp,item_comb)
os.chdir(r"D:\Aster\csv_and_pickle_files")
final_result.to_csv('Tfidf_cosin_Booking_top_10.csv')






#######################################################################################################




####################################################################################################
# data_item_only_customer_id=run_query(data_extraction_query(brand_mapping_query_customer,'2019-01-01', '2022-01-01'))
# data_item_only_customer_id.to_pickle('brand_combo_freq_custmer_19_22')
# data_item_only_customer_id=pd.read_pickle('brand_combo_freq_custmer_19_22')

# item_comb,brand_mapping_grp=check_func(data_item_only_customer_id, 0)
# cosine_sim1=md2.cosin_sim_2(data_item_only_customer_id,100,brand_mapping_grp,False,True)
# updated_cosin_sim1=md2.tfidf(cosine_sim1,True)
# Year2021_csv_1=final_shapping(updated_cosin_sim1,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# Year2021_csv_1.to_csv('Tfidf_cosin_Patient_top_10.csv')






######################################################################################################

# data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_custmer_19_22')
# item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
# cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
# cosine_sim1_all.to_csv('cosin_sim_all_customer_id.csv')
# updated_cosin_sim1_top_25_all=md2.tfidf(cosine_sim1_all,True,25)
# final_result,Result_long_form=final_shapping(updated_cosin_sim1_top_25_all,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('Tfidf_cosin_customer_top_25_all.csv')
# Result_long_form.to_csv('Tfidf_cosin_customer_top_25_all_long.csv')

####################################################################################################








#####################################################################################################

# data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_bkn_id_19_22')
# item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
# cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
# cosine_sim1_all.to_csv('cosin_sim_all_booking_id.csv')
# updated_cosin_sim1_top_25_all=md2.tfidf(cosine_sim1_all,True,25)
# final_result,Result_long_form=final_shapping(updated_cosin_sim1_top_25_all,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('Tfidf_cosin_Booking_top_25_all.csv')
# Result_long_form.to_csv('Tfidf_cosin_Booking_top_25_all_long.csv')
##########################################################################################################





#####################################################################################################

data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_custmer_19_22')
item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
cosine_sim1_all = cosine_sim1_all.sort_values(['firstproductid','value'],ascending=False)
cosine_sim1_all = cosine_sim1_all.groupby(['firstproductid']).head(25).reset_index()
final_result,Result_long_form=final_shapping(cosine_sim1_all,brand_mapping_grp,item_comb)
os.chdir(r"D:\Aster\csv_and_pickle_files")
final_result.to_csv('cosin_customer_top_25_all.csv')
Result_long_form.to_csv('cosin_customer_top_25_all_long.csv')






#####################################################################################################


data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_bkn_id_19_22')
item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)

cosine_sim1_all = cosine_sim1_all.groupby(['firstproductid']).head(25).reset_index()
final_result,Result_long_form=final_shapping(cosine_sim1_all,brand_mapping_grp,item_comb)
os.chdir(r"D:\Aster\csv_and_pickle_files")
final_result.to_csv('cosin_Booking_top_25_all.csv')
Result_long_form.to_csv('cosin_Booking_top_25_all_long.csv')



#####################################################################################################

######################################################################################################

# data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_custmer_19_22')
# item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
# cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
# updated_cosin_sim1_top_25_all=md2.tfidf_all(cosine_sim1_all,True,25)
# final_result,Result_long_form=final_shapping(updated_cosin_sim1_top_25_all,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('Tfidf_all_cosin_customer_top_25_all.csv')
# Result_long_form.to_csv('Tfidf_all_cosin_customer_top_25_all_long.csv')

####################################################################################################








#####################################################################################################

# data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_bkn_id_19_22')
# item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
# cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
# updated_cosin_sim1_top_25_all=md2.tfidf_all(cosine_sim1_all,True,25)
# final_result,Result_long_form=final_shapping(updated_cosin_sim1_top_25_all,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('Tfidf_all_cosin_Booking_top_25_all.csv')
# Result_long_form.to_csv('Tfidf_all_cosin_Booking_top_25_all_long.csv')
##########################################################################################################








data_item_comb_cat_booking=pd.read_pickle(path_parmanent_pickle+r'/brand_combo_freq_custmer_19_22')
item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
cosine_sim1_all = cosine_sim1_all.sort_values(['firstproductid','value'],ascending=False)
# cosine_sim1_all = cosine_sim1_all.groupby(['firstproductid']).head(25).reset_index()
final_result,Result_long_form=final_shapping(cosine_sim1_all,brand_mapping_grp,item_comb)
os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('cosin_customer_top_25_all.csv')
Result_long_form.to_csv('cosin_customer_Top_x_all_long.csv')





#############################################################################################
data_brand_mapping_bkng_with_icd=run_query(data_extraction_query(brand_mapping_bkng_with_icd_query,'2019-01-01', '2022-01-01'))

data_brand_mapping_bkng_with_icd.to_pickle('data_brand_mapping_bkng_with_icd')

data_brand_mapping_cust_with_icd=run_query(data_extraction_query(brand_mapping_cust_with_icd_query,'2019-01-01', '2022-01-01'))

data_brand_mapping_cust_with_icd.to_pickle('data_brand_mapping_cust_with_icd')

data_brand_mapping_bkng_with_icd=pd.read_pickle('data_brand_mapping_bkng_with_icd')





######################################################################################################

# data_item_comb_cat_booking=pd.read_pickle('brand_combo_freq_custmer_19_22')
# item_comb,brand_mapping_grp=check_func(data_item_comb_cat_booking, 0)
# cosine_sim1_all=md2.cosin_sim_2(data_item_comb_cat_booking,100,brand_mapping_grp,False,False)
# updated_cosin_sim1_top_25_all=md2.tfidf(cosine_sim1_all,True,25)
# final_result,Result_long_form=final_shapping(updated_cosin_sim1_top_25_all,brand_mapping_grp,item_comb)
# os.chdir(r"D:\Aster\csv_and_pickle_files")
# final_result.to_csv('NormalisedTfidf_cosin_customer_top_25_all.csv')
# Result_long_form.to_csv('NormalisedTfidf_cosin_customer_top_25_all_long.csv')

####################################################################################################


################################# Brand ICD Mapping #################################################
data_brand_mapping_with_emr=run_query(data_extraction_query(brand_mapping_bkng_with_icd_query_updated,'2019-01-01', '2022-01-01'))
data_brand_mapping_with_emr=pd.read_pickle(r'D:\Aster\Parmanent_pickle\brand_mapping_with_emr_19_22_bkng')
item_comb,brand_mapping_grp=check_func(data_brand_mapping_with_emr, 1)
# item_comb['icd_brand1']=np.where(item_comb['brandid1'].str.isdigit(),'brand','icd')
# item_comb['icd_brand2']=np.where(item_comb['brandid2'].str.isdigit(),'brand','icd')
cosine_sim1=md2.cosin_sim_2(data_brand_mapping_with_emr,0,brand_mapping_grp,True,True)
final_result,Result_long_form=final_shapping(cosine_sim1,brand_mapping_grp,item_comb)
final_result.to_csv('icd_vs_brand_sim_all.csv')
Result_long_form.to_csv('icd_vs_brand_sim_all_long.csv')





# item_comb_only_brands=item_comb[(item_comb['icd_brand1']=='brand') & (item_comb['icd_brand2']=='brand')]
# item_comb_only_icd=item_comb[(item_comb['icd_brand1']=='icd')]
# # tfidf_brand_brand=md2.only_tfidf(item_comb_only_brands)

# tfidf_icd_brand,tfidf_icd_non_pharma=md2.only_tfidf(item_comb_only_icd,brand_mapping_grp)
# tfidf_icd_brand.to_csv('tfidf_icd_brand.csv')
# tfidf_icd_non_pharma.to_csv('tfidf_icd_non_pharma.csv')





# data_brand_mapping_with_emr_customer=run_query(data_extraction_query(brand_mapping_bkng_with_icd_query_updated_customer,'2019-01-01', '2022-01-01'))
# data_brand_mapping_with_emr_customer=pd.read_pickle('brand_mapping_with_emr_19_22_bkng')
# item_comb,brand_mapping_grp=check_func(data_brand_mapping_with_emr, 0)
# item_comb['icd_brand1']=np.where(item_comb['brandid1'].str.isdigit(),'brand','icd')
# item_comb['icd_brand2']=np.where(item_comb['brandid2'].str.isdigit(),'brand','icd')










############################ Top ICD 4 Brands ###################################################
# tfidf_topIcds_4_brand,tfidf_topIcds_4_brand_non_pharma=md2.only_tfidf(item_comb,brand_mapping_grp)
# tfidf_topIcds_4_brand=tfidf_topIcds_4_brand[['brandid1', 'brandid2', 'freq_at_customer', 'total_items', 'tf', 'idf','tf_idf']]
# tfidf_topIcds_4_brand=tfidf_topIcds_4_brand.merge(brand_mapping_grp,how='left',left_on='brandid1',right_on='brandid')
# tfidf_topIcds_4_brand=tfidf_topIcds_4_brand[tfidf_topIcds_4_brand['brandid2'].str.isdigit()==False]
# tfidf_topIcds_4_brand['rank']=tfidf_topIcds_4_brand.groupby("brandid1")["tf_idf"].rank("dense", ascending=False)
# tfidf_topIcds_4_brand.to_csv('tfidf_topIcds_4_brand.csv')

data_brand_mapping_with_emr.rename(columns={'freq_at_customer':'frequency'},inplace=True)
cosin_sim_icd_brand=md2.cosin_sim_2(data_brand_mapping_with_emr,0,brand_mapping_grp,False,True)
cosin_sim_icd_brand=cosin_sim_icd_brand.merge(brand_mapping_grp,how='left',left_on='secondproductid',right_on='brandid')
cosin_sim_icd_brand = cosin_sim_icd_brand.sort_values(by=['firstproductid','value'],ascending=False)
cosin_sim_icd_brand['rank'] = cosin_sim_icd_brand.groupby(['firstproductid']).cumcount()+1
# cosin_sim_icd_brand['rank']=cosin_sim_icd_brand.groupby("firstproductid")["ranker"].rank("dense", ascending=True)
cosin_sim_icd_brand=cosin_sim_icd_brand[cosin_sim_icd_brand['rank']<6]
cosin_sim_icd_brand=cosin_sim_icd_brand[cosin_sim_icd_brand['firstproductid'].str.isdigit()==False]
cosin_sim_icd_brand=cosin_sim_icd_brand.merge(data_brand_mapping_with_emr,left_on=['firstproductid','secondproductid'],right_on=['brandid1','brandid2'])
del cosin_sim_icd_brand['brandid1']
del cosin_sim_icd_brand['brandid2']
cosin_sim_doubtful=~cosin_sim_icd_brand[(~(cosin_sim_icd_brand['freq_at_customer']>10)) & (~(cosin_sim_icd_brand['value']>0.001)) ]
# cosin_sim_icd_brand.to_csv('cosin_sim_icd_brand_no_filter.csv')



################################   Cosine Similarity OTC   #########################


# data_brand_mapping_otc=run_query(brand_mapping_bkng_otc)
# data_brand_mapping_otc=pd.DataFrame(data_brand_mapping_otc)
# data_brand_mapping_otc.to_pickle('data_brand_mapping_otc')
data_brand_mapping_otc=pd.read_pickle(path_parmanent_pickle+r'/data_brand_mapping_otc')
data_brand_mapping_otc.rename(columns={'freq_at_customer':'frequency'},inplace=True)
data_brand_mapping_otc['brandid1']=data_brand_mapping_otc['brandid1'].astype('str')
data_brand_mapping_otc['brandid2']=data_brand_mapping_otc['brandid2'].astype('str')
data_brand_mapping_otc.rename(columns={'freq_at_customer':'frequency'},inplace=True)
item_comb,brand_mapping_grp=check_func(data_brand_mapping_otc, 1)
cosine_sim1=md2.cosin_sim_2(data_brand_mapping_otc,-1,brand_mapping_grp,True,True)
cosine_sim1 = cosine_sim1.sort_values(by=['firstproductid','value'],ascending=False)
cosine_sim1['rank'] = cosine_sim1.groupby(['firstproductid']).cumcount()+1
cosine_sim1=cosine_sim1.merge(brand_mapping_grp,how='left',left_on='secondproductid',right_on='brandid')
cosine_sim1=cosine_sim1[cosine_sim1['rank']<6]
cosine_sim1=cosine_sim1.merge(data_brand_mapping_otc,left_on=['firstproductid','secondproductid'],right_on=['brandid1','brandid2'])
del cosine_sim1['brandid1']
del cosine_sim1['brandid2']
cosine_sim1_filtered=cosine_sim1[((cosine_sim1['value']<0.001) & (cosine_sim1['freq_at_customer']<30))]
# cosine_sim1_filtered=pd.DataFrame(cosine_sim1_filtered)
cosine_sim1.to_csv('cosine_simi_brands_otc_without_filter.csv')

###################################################################################################





################################## Cosine Similarity Updated #########################################

Updated_cosineSimilarity=run_query((dynamic_data_extraction(updated_cosine_similarity_Otc)))
Updated_cosineSimilarity=pd.read_pickle('updated_cosine_similarity_Otc')
Updated_cosineSimilarity.rename(columns={'freq_at_customer':'frequency'},inplace=True)
item_comb,brand_mapping_grp=check_func(Updated_cosineSimilarity, 1)
Update_cosine_similarity_unsym=md2.cosin_sim_2(Updated_cosineSimilarity, 30, brand_mapping_grp, True, True)



############################################################################################

############################### COSINE Similarity Customer OTC #######################


data_cust_brand_mapping_otc=run_query(brand_mapping_customer_otc)
# data_cust_brand_mapping_otc.to_pickle('data_cust_brand_mapping_otc')
data_cust_brand_mapping_otc=pd.read_pickle(path_parmanent_pickle+r'/data_cust_brand_mapping_otc')
data_cust_brand_mapping_otc.rename(columns={'freq_at_customer':'frequency'},inplace=True)
item_comb,brand_mapping_grp=check_func(data_cust_brand_mapping_otc, 0)
cosine_simi_cust_otc=md2.cosin_sim_2(data_cust_brand_mapping_otc,0, brand_mapping_grp, True, True)
cosine_simi_cust_otc = cosine_simi_cust_otc.sort_values(by=['firstproductid','value'],ascending=False)
cosine_simi_cust_otc['rank'] = cosine_simi_cust_otc.groupby(['firstproductid']).cumcount()+1
cosine_simi_cust_otc=cosine_simi_cust_otc[cosine_simi_cust_otc['rank']<6]
cosine_simi_cust_otc=cosine_simi_cust_otc.merge(brand_mapping_grp,how='left',left_on='secondproductid',right_on='brandid')
cosine_simi_cust_otc=cosine_simi_cust_otc.merge(data_cust_brand_mapping_otc,left_on=['firstproductid','secondproductid'],right_on=['brandid1','brandid2'])
# cosine_simi_cust_otc.to_csv('cosine_simi_cust_otc_without_filter.csv')















