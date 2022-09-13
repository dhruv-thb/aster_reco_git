# -*- coding: utf-8 -*-
"""
Created on Tue Feb 22 15:28:45 2022

@author: Dhruv
"""


import pandas as pd
import psycopg2
import pandas
import numpy as np 
# import io
import re
import os
import itertools
import math
from queries import *
from Common_func import * 
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()






path_permanent_csv=r"D:\Aster\csv_and_pickle_files\Parmanent CSV"
brand_mapping_grp=pd.read_csv(path_permanent_csv+r'\brand_mapping_grp.csv')




'd50','e03','e78','g62','j20','j30','k21','k59','k64','k76','l03','m25','m54','m79','r50','r51','r52','z79'

patientIcd=('d50','e03','e78','g62','j20','j30','k21','k59','k64','k76','l03','m25','m54','m79','r50','r51','r52','z79')
patientIcd=tuple(patientIcd)
icdBrand='''
select firstproductid, secondproductid ,value  from cosinewithicdtable where firstproductid in {}'''.format(patientIcd)

topBrands=run_query(icdBrand)

BrandReco=topBrands.groupby('secondproductid').sum('value').reset_index()
agg_user=pd.pivot_table(topBrands,values='firstproductid',index=['secondproductid'],aggfunc=lambda x: ','.join(x)).reset_index()    
final_reco=BrandReco.merge(agg_user,how='left',on=['secondproductid'])
final_reco=final_reco.sort_values(by=['value'],ascending=False)
final_reco=final_reco.merge(brand_mapping_grp,how='left',left_on='secondproductid',right_on='brandid')
