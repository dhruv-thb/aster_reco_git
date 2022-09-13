# -*- coding: utf-8 -*-
"""
Created on Fri Dec 31 15:13:31 2021

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
import math
import os
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()




path_permanent_csv=r"D:\Aster\csv_and_pickle_files\Parmanent CSV"
brand_mapping_grp=pd.read_csv(path_permanent_csv+r'\brand_mapping_grp.csv')


#####################################MODEL-2 SPECIFIC FUNCTIONS ###############################




    

def cosin_sim_2(data_item_comb_cat_booking,freq_limit,brand_mapping_grp,top_ten,non_pharma):
    total_items = data_item_comb_cat_booking[data_item_comb_cat_booking['brandid1']==data_item_comb_cat_booking['brandid2']]
    del total_items['brandid2']
    data_item_comb_cat_booking.rename(columns={'frequency':'freq_at_customer'},inplace=True)
    total_items.rename(columns={'brandid1':'brandid'},inplace=True)
    data_item_comb_cat_booking.loc[data_item_comb_cat_booking['freq_at_customer']<freq_limit,'frequency']=0
    data_item_comb_cat_booking.loc[data_item_comb_cat_booking['freq_at_customer']>=freq_limit,'frequency']=data_item_comb_cat_booking['freq_at_customer']
    del data_item_comb_cat_booking['frequency']
    cosine_sim=data_item_comb_cat_booking.merge(total_items,how='left',left_on=('brandid1'),right_on=('brandid'))
    cosine_sim.rename(columns={'frequency':'freq_brand_1'},inplace=True)
    del cosine_sim['brandid']
    cosine_sim=cosine_sim.merge(total_items,how='left',left_on=('brandid2'),right_on=('brandid'))
    cosine_sim.rename(columns={'frequency':'freq_brand_2'},inplace=True)
    del cosine_sim['brandid']
    cosine_sim['cosin_sim_deno']=np.sqrt(cosine_sim['freq_brand_1'])*np.sqrt(cosine_sim['freq_brand_2'])
    cosine_sim['value']=cosine_sim['freq_at_customer']/cosine_sim['cosin_sim_deno']
    del cosine_sim['cosin_sim_deno']
    cosin_sim_mapping=cosine_sim.copy()
    cosine_sim=cosine_sim[['brandid1','brandid2','value']]
    cosine_sim = cosine_sim[(cosine_sim['value']>0)&(cosine_sim['value']<0.99)]
    cosine_sim =cosine_sim.rename(columns = {'brandid1':'firstproductid','brandid2':'secondproductid'})
    brand_mapping_grp = brand_mapping_grp[['servicetype','brandid']]
    cosine_sim = cosine_sim.merge(brand_mapping_grp,left_on = 'firstproductid',right_on = 'brandid',how = 'left')    
    del cosine_sim['brandid']
    cosine_sim = cosine_sim.rename(columns = {'servicetype':'servicetype1'})
    cosine_sim = cosine_sim.merge(brand_mapping_grp,left_on = 'secondproductid',right_on = 'brandid',how = 'left')
    del cosine_sim['brandid']
    cosine_sim = cosine_sim.rename(columns = {'servicetype':'servicetype2'})
    cosine_sim.fillna('ICD information',inplace=True)
    cosine_sim['class1'] = ['ERX' if (x == 'pharma classification') else 'OTC' for x in cosine_sim['servicetype1']]
    cosine_sim['class2'] = ['ERX' if (x == 'pharma classification') else 'OTC' for x in cosine_sim['servicetype2']]
    cosine_sim.loc[cosine_sim['servicetype1']=='ICD information','class1']='ERX'
    cosine_sim.loc[cosine_sim['servicetype2']=='ICD information','class2']='ERX'
    cosine_sim = cosine_sim[['firstproductid', 'secondproductid', 'value', 'class1', 'class2']]
    cosine_sim = cosine_sim.sort_values(['firstproductid','value'],ascending=False)
    
    # cosin_sim_ov=cosine_sim.copy()
    
    # cosin_sim_mapping.to_csv('cosin_sim_all_customer_id.csv')
    
    ###########################################################################
    if (non_pharma==True):
        cosine_sim = cosine_sim[cosine_sim['class2']=='OTC']
    else :
       cosine_sim=cosine_sim
    #########################################################################   
    
    
    ############################################################################
    if (top_ten==True):
        cosine_sim1 = cosine_sim.groupby(['firstproductid']).head(10).reset_index()
    else:
        cosine_sim1=cosine_sim 
    ###########################################################################
    
    
    
    return cosine_sim1


# def tfidf(cosine_sim1,top_filter,x):
    
    
#     os.chdir(r"D:\Aster\csv_and_pickle_files")

#     tf_idf_products=pd.read_csv('tf_idf_with_mobile.csv')
    
#     os.chdir(r"D:\Aster\aster_py_files")
    
    
#     # tf_idf_products=tf_idf_products[['brandid','New_tf_idf']]
#     # tf_idf_products['brandid']=tf_idf_products['brandid'].astype('str')
#     # updated_cosin_sim=cosine_sim1.merge(tf_idf_products,how='left',left_on='secondproductid',right_on='brandid')
#     # updated_cosin_sim['value']=updated_cosin_sim['value']*updated_cosin_sim['New_tf_idf']
#     # updated_cosin_sim.drop(['New_tf_idf','brandid'],axis=1,inplace=True)
#     tf_idf_products=tf_idf_products[['brandid','tf_idf']]
#     tf_idf_products['brandid']=tf_idf_products['brandid'].astype('str')
#     updated_cosin_sim=cosine_sim1.merge(tf_idf_products,how='left',left_on='secondproductid',right_on='brandid')
#     updated_cosin_sim['value']=updated_cosin_sim['value']*updated_cosin_sim['tf_idf']
#     updated_cosin_sim.drop(['tf_idf','brandid'],axis=1,inplace=True)
#     updated_cosin_sim = updated_cosin_sim.sort_values(['firstproductid','value'],ascending=False)
   
#    #############################################################################
#     if(top_filter==True):
#         updated_cosin_sim1 = updated_cosin_sim.groupby(['firstproductid']).head(x).reset_index()
#     else:
#         updated_cosin_sim1=updated_cosin_sim
#    #############################################################################
   
#     return updated_cosin_sim1


# def tfidf_all(cosine_sim1,top_filter,x):
    
    
#     os.chdir(r"D:\Aster\csv_and_pickle_files")

#     tf_idf_products=pd.read_csv('tf_idf_without_mobile.csv')
    
#     os.chdir(r"D:\Aster\aster_py_files")
#     tf_idf_products=tf_idf_products[['brandid','tf_idf']]
#     tf_idf_products['brandid']=tf_idf_products['brandid'].astype('str')
#     updated_cosin_sim=cosine_sim1.merge(tf_idf_products,how='left',left_on='secondproductid',right_on='brandid')
#     updated_cosin_sim['value']=updated_cosin_sim['value']*updated_cosin_sim['tf_idf']
#     updated_cosin_sim.drop(['tf_idf','brandid'],axis=1,inplace=True)
#     updated_cosin_sim = updated_cosin_sim.sort_values(['firstproductid','value'],ascending=False)
   
#    #############################################################################
#     if(top_filter==True):
#         updated_cosin_sim1 = updated_cosin_sim.groupby(['firstproductid']).head(x).reset_index()
#     else:
#         updated_cosin_sim1=updated_cosin_sim
#    #############################################################################
   
#     return updated_cosin_sim1






# def tf_idf_new(item_comb,cosine_sim1,top_filter,x):
    
#     tfidf_table=item_comb[['brandid1','brandid2','freq_at_customer']]
#     total_items = item_comb[item_comb['brandid1']==item_comb['brandid2']]
#     total_items=total_items[['brandid1','freq_at_customer']]
#     total_items.rename(columns={'freq_at_customer':'total_items'},inplace=True)
#     tfidf_table=tfidf_table.merge(total_items,how='left',on='brandid1')
#     tfidf_table['total_brands']=tfidf_table['brandid1'].nunique()
#     uniq_comb=item_comb[['brandid1','brandid2']]
#     uniq_comb=uniq_comb.groupby(['brandid2'])['brandid1'].nunique().reset_index()
#     uniq_comb.rename(columns={'brandid1':'uniq_brands'},inplace=True)
#     tfidf_table=tfidf_table.merge(uniq_comb,how='left',on='brandid2')
#     tfidf_table['tf']=tfidf_table['freq_at_customer']/tfidf_table['total_items']
#     tfidf_table['idf']= np.log(tfidf_table['total_brands']/tfidf_table['uniq_brands'])
#     tfidf_table['tf_idf']=tfidf_table['tf']*tfidf_table['idf']
#     tfidf_table=tfidf_table[['brandid1','brandid2','tf','idf','tf_idf']]
#     tfidf_table1=tfidf_table[['brandid1','brandid2','tf_idf']]
#     updated_cosin_sim=cosine_sim1.merge(tfidf_table1,how='left',left_on=['firstproductid','secondproductid'],right_on=['brandid1','brandid2'])
#     updated_cosin_sim['updated_value']=updated_cosin_sim['value']*updated_cosin_sim['tf_idf']
#     updated_cosin_sim.drop(['tf_idf','brandid1','brandid2'],axis=1,inplace=True)

#    #############################################################################
#     if(top_filter==True):
#         updated_cosin_sim1 = updated_cosin_sim.groupby(['firstproductid']).head(x).reset_index()
#     else:
#         updated_cosin_sim1=updated_cosin_sim
#    #############################################################################
   
#     return updated_cosin_sim1,tfidf_table






# def only_tfidf(item_comb_only_icd,brand_mapping_grp):
#     tfidf_table=item_comb_only_icd[['brandid1','brandid2','freq_at_customer']]
#     total_items = item_comb_only_icd[item_comb_only_icd['brandid1']==item_comb_only_icd['brandid2']]
#     total_items=total_items[['brandid1','freq_at_customer']]
#     total_items.rename(columns={'freq_at_customer':'total_items'},inplace=True)
#     tfidf_table=tfidf_table.merge(total_items,how='left',on='brandid1')
#     tfidf_table['total_brands']=tfidf_table['brandid1'].nunique()
#     uniq_comb=item_comb_only_icd[['brandid1','brandid2']]
#     uniq_comb=uniq_comb.groupby(['brandid2'])['brandid1'].nunique().reset_index()
#     uniq_comb.rename(columns={'brandid1':'uniq_brands'},inplace=True)
#     tfidf_table=tfidf_table.merge(uniq_comb,how='left',on='brandid2')
#     tfidf_table['tf']=tfidf_table['freq_at_customer']/tfidf_table['total_items']
#     tfidf_table['idf']= np.log(tfidf_table['total_brands']/tfidf_table['uniq_brands'])
#     tfidf_table['tf_idf']=tfidf_table['tf']*tfidf_table['idf']
#     tfidf_table=tfidf_table[['brandid1','brandid2','freq_at_customer','total_items','tf','idf','tf_idf']]
#     brand_mapping_grp['brandid']= brand_mapping_grp['brandid'].astype('str')
#     tfidf_table=tfidf_table.merge(brand_mapping_grp,how='left',left_on='brandid2',right_on='brandid')
#     tfidf_table_pharma=tfidf_table[tfidf_table['servicetype']=='non-pharma classification']
#     tfidf_table["rank"] = tfidf_table.groupby("brandid1")["tf_idf"].rank("dense", ascending=False)
#     tfidf_table_pharma["rank"] = tfidf_table_pharma.groupby("brandid1")["tf_idf"].rank("dense", ascending=False)
#     return tfidf_table,tfidf_table_pharma    


