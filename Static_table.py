# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 14:59:58 2022

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
from Common_func import *
import os
import re
import datetime
import Model_2_func as md2




brand_level_data=pd.read_pickle('brand_combo_freq_bkn_id_19_22')
brand_level_data.rename(columns={'freq_at_customer':'frequency'},inplace=True)
item_comb,brand_mapping_grp=check_func(brand_level_data, 1)
cosine_sim=md2.cosin_sim_2(brand_level_data,100,brand_mapping_grp,False,True)
cosine_sim = cosine_sim.groupby(['firstproductid']).head(3).reset_index()

final_result,final_result_melt=final_shapping(cosine_sim, brand_mapping_grp, item_comb)


