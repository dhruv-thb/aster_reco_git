# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 18:29:03 2022

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
import os
import itertools
from queries import *
from datetime import date
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()

############################# FILE UTILITY #####################################

"""
All the functions which will be used across all the models are written in this py file.
"""




################################################################################# 
def run_query(query):
   cur.execute(query)
   names = [ x[0] for x in cur.description]
   rows = cur.fetchall()
   data = pd.DataFrame( rows, columns=names)
   return data

#################################################################################









###################################################################################

def data_extraction_query(query, startdate,enddate):

    data_query = query.replace("startdate", startdate)
    data_query = data_query.replace("enddate",enddate)
    return data_query
      
################################################################################        






def dynamic_data_extraction(query):
    enddate=date.today()
    startdate=enddate- relativedelta(years=3)
    enddate = enddate.strftime("%Y/%m/%d")
    startdate = startdate.strftime("%Y/%m/%d")
    data_query = query.replace("startdate", startdate)
    data_query = data_query.replace("enddate",enddate)
    return data_query





################################################################################

def check_func(data_item_comb_cat_booking,y):
    """
    

    Parameters
    ----------
    data_item_comb_cat_booking : This parameter will contain frequency count(No of times two brands are bought together)
                                 corresponding to all the combination of brand ids.
    x :
        y=1 in order to convert brandid to str type or object type 
        y=0 keep brandis as it is (int64)
    Returns
    -------
    item_comb : Adding details to the data .
    brand_mapping_grp : Master data this dataferame includes all the details for all the brands available.

    """
    
    brand_mapping_grp  = run_query(query_reco_product)
    brand_mapping_grp['brandid']=brand_mapping_grp['brandid'].astype('int64')
    item_comb = data_item_comb_cat_booking
    if y==1:
        brand_mapping_grp['brandid']= brand_mapping_grp['brandid'].astype('str')
    else :
        brand_mapping_grp['brandid']= brand_mapping_grp['brandid']
    item_comb = item_comb.merge(brand_mapping_grp,how='left',left_on = 'brandid1',right_on = 'brandid')
    item_comb = item_comb.rename(columns ={'brand':'brand1','servicetype':'servicetype1','itemtype':'itemtype1','thb_category':'thb_category1'})
    del item_comb['brandid']
        
    item_comb = item_comb.merge(brand_mapping_grp,how='left',left_on = 'brandid2',right_on = 'brandid')
    item_comb = item_comb.rename(columns ={'brand':'brand2','servicetype':'servicetype2','itemtype':'itemtype2','thb_category':'thb_category2'})
    del item_comb['brandid']

    item_comb['class1'] = ['ERX' if (x == 'pharma classification' or x=='ICD information') else 'OTC' for x in item_comb['servicetype1']]
    item_comb['class2'] = ['ERX' if (x == 'pharma classification' or x=='ICD information') else 'OTC' for x in item_comb['servicetype2']]
    item_comb.loc[item_comb['servicetype1']=='ICD information','class1']='ERX'
    item_comb.loc[item_comb['servicetype2']=='ICD information','class2']='ERX'

    item_comb['first_brand'] = item_comb['brand1']+'*'+item_comb['itemtype1']+'*'+item_comb['thb_category1']+'*'+item_comb['class1']
    item_comb['second_brand'] = item_comb['brand2']+'*'+item_comb['itemtype2']+'*'+item_comb['thb_category2']+'*'+item_comb['class2']
    
    item_comb['status']=1

    return item_comb,brand_mapping_grp





############################################################################################
 




##########################################################################################




def final_shapping(cosine_sim1,brand_mapping_grp,item_comb):
    
    
    """
    DESCRIPTION.
    -----------
        This function takes the input in which every brandid has top 10 items 
        recommendation on the basis of tf-idf and cosine similarity.
        
        But this function then shapes it into a file which has all the detail corresponding to 
        all the recommendation .
    """
    top_prod_new = item_comb[item_comb['brandid1']==item_comb['brandid2']]
    top_prod_new = top_prod_new.sort_values('frequency',ascending=False)
    
    top_prod_cosine  =item_comb[[ 'brandid1','brandid2','frequency']]
    top_prod_merge =top_prod_new[['brandid1','frequency']]
    
    
    
    top_prod = cosine_sim1.merge(top_prod_cosine,left_on = ['firstproductid','secondproductid'],right_on= ['brandid1','brandid2'],how = 'left')
    top_prod = top_prod.sort_values('value',ascending=False)
    
    
    top_prod['value']  = top_prod['value'] .astype(str)
    top_prod['frequency'] = top_prod['frequency'].astype(str)
    del top_prod['brandid1']
    del top_prod['brandid2']

    top_prod = top_prod.drop(['class1','class2'],axis =1)
    reco_product=brand_mapping_grp.copy()
    reco_product['class'] = ['ERX' if x == 'pharma classification' else 'OTC' for x in reco_product['servicetype']]
    
    reco_product['final_brand'] = reco_product['brand']+'*'+reco_product['itemtype']+'*'+reco_product['thb_category']+'*'+reco_product['class']
    reco_prod_brand_info = reco_product[['brandid','final_brand']]
    top_prod = top_prod.merge(reco_prod_brand_info,how='left',left_on = 'firstproductid',right_on = 'brandid')
    del top_prod['brandid']
         
    top_prod = top_prod.rename(columns={'final_brand':'first_brand'})
    
    top_prod = top_prod.merge(reco_prod_brand_info,left_on = 'secondproductid',right_on = 'brandid')
    del top_prod['brandid']    
        
       
    top_prod = top_prod.rename(columns={'final_brand':'second_brand'})
    
    
    top_prod_melt=top_prod.copy()
    
    top_prod['final'] = top_prod['second_brand']+':'+top_prod['value'] +':'+top_prod['frequency']
    top_prod = top_prod.sort_values('value',ascending = False)
    top_prod1 = top_prod.groupby(['first_brand','firstproductid'])['final'].apply('$'.join).reset_index()
    top_prod1 = top_prod1.join(top_prod1['final'].str.split('$',expand=True).rename(columns = lambda x: "top_item_"+str(x+1)))
    spike_cols = [col for col in top_prod1.columns if 'top_item_' in col]
    
    for i in spike_cols:
        top_prod1[i+'_name'] = top_prod1[i].str.split(':').str[0]
        top_prod1[i+'_name'] = top_prod1[i+'_name'].str.replace('\*OTC','')
        top_prod1[i+'_cosine'] = top_prod1[i].str.split(':').str[1]
        top_prod1[i+'_cosine'] = top_prod1[i+'_cosine'].astype(float)
        top_prod1[i+'_count'] = top_prod1[i].str.split(':').str[2]
        top_prod1[i+'_count'] = top_prod1[i+'_count'].astype(float)
    
    top_prod_final_freq_new = top_prod1.filter(regex = 'first\_brand|firstproductid|top\_item\_[0-9]{1,2}\_name|top\_item\_[0-9]{1,2}\_cosine|top\_item\_[0-9]{1,2}\_count')
    top_prod_merge = top_prod_merge.merge(reco_prod_brand_info,left_on = 'brandid1',right_on = 'brandid')
    top_prod_merge = top_prod_merge.drop(['brandid','brandid1'],axis = 1)
    top_prod_merge = top_prod_merge.rename(columns ={'final_brand':'first_brand'})
    top_prod_final_freq_new = top_prod_final_freq_new.merge(top_prod_merge,on ='first_brand',how = 'left')
    #---------------------------------------------------------------------------------------
     
    top_prod_melt[['brand','itemtype','thb_category','class']] = top_prod_melt['second_brand'].str.split('*',expand=True)

    return top_prod_final_freq_new,top_prod_melt

#########################################################################################







