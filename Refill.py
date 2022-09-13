# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 11:44:35 2022

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


########################           DATA  ############################################################
path_parmanent_pickle=r"D:\Aster\Parmanent_pickle"

path_secured_data_files=r"D:\Aster\Secured_data_approach"

only_ocd=r"D:\Aster\Only_OTC_data"

training_data_brand=pd.read_pickle(path_parmanent_pickle+r'\training_data_brand')

test_data_brand=pd.read_pickle(path_parmanent_pickle+r'\test_data_brand')

training_data_icd=pd.read_pickle(path_parmanent_pickle+r'\training_data_icd')

test_data_icd=pd.read_pickle(path_parmanent_pickle+r'\test_data_icd')

cosin_sim_icd_brand=pd.read_csv(path_secured_data_files+r'\cosin_sim_icd_brand.csv')

random_pat_id_ten_k=pd.read_pickle(path_parmanent_pickle+r'\random_pat_id_ten_k')

test_data_brand_pharma=pd.read_pickle(path_parmanent_pickle+r'\test_data_brand_pharma')



brand_mapping=pd.read_csv('brand_mapping_grp.csv')



cosine_simi_brands_otc=pd.read_csv('cosine_simi_brands_otc.csv')

cosine_simi_brands_otc_updated=cosine_simi_brands_otc.merge(brand_mapping,how='left',left_on='secondproductid',right_on='brandid')







