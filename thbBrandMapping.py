# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 14:42:32 2022

@author: Ankur
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




query_brands='''

select m.testname ,m.brand ,t.thb_category,i2.value as itemsubtype ,i3.value  as itemtype, s.value as servicetype,count(distinct i.patientremoteid) as patcount,count(distinct i.bookingid) as bkng_count,sum(i.netamount) from s_billing.items i
inner join s_billing.meta m on i.billingmetaid =m.id 
inner join s_constants.itemsubtype i2 on i2.id =m.itemsubtypeid 
inner join s_constants.itemtype i3 on i3.id=m.itemtypeid 
inner join s_constants.servicetype s on s.id =m.servicetypeid 
left join thbbrandmapping t on t.id =m.id 
group by m.testname,m.brand ,t.thb_category,i2.value ,i3.value , s.value

'''



brands=run_query(query_brands)





fullBrandTable=run_query(thb_brand_mapping)
brandTableWtoutBrandId=fullBrandTable[fullBrandTable['brandid'].isnull()]
brandTableWtoutBrandId.dropna(subset=['brand'],inplace=True)
brandTableWithBrandId=fullBrandTable[~fullBrandTable['brandid'].isnull()]

brandTableWtoutBrandIdUpdated=brandTableWtoutBrandId.copy()
brandTableWtoutBrandId=brandTableWtoutBrandId[['brand','itemtype', 'servicetype','itemsubtype','thb_category']]
brandTableWtoutBrandId=brandTableWtoutBrandId.drop_duplicates()
brandTableWtoutBrandId['category_status']=0
brandTableWtoutBrandId['thb_category']=brandTableWtoutBrandId['itemsubtype']
maxBrandId=fullBrandTable['brandid'].max()
a_list=list(range(1,brandTableWtoutBrandId['brand'].size+1))
brandTableWtoutBrandId['new']=a_list
brandTableWtoutBrandId['brandid']=maxBrandId
brandTableWtoutBrandId['brandid']=brandTableWtoutBrandId['brandid']+brandTableWtoutBrandId['new']
del brandTableWtoutBrandId['new']
brandTableWtoutBrandIdUpdated_1=brandTableWtoutBrandIdUpdated.merge(brandTableWtoutBrandId,how='left',on=['brand','itemtype', 'servicetype','itemsubtype'])
brandTableWtoutBrandIdUpdated_1=brandTableWtoutBrandIdUpdated_1.drop(columns={'brandid_x','category_status_x','thb_category_x'})
brandTableWtoutBrandIdUpdated_1.rename(columns={'thb_category_y':'thb_category','category_status_y':'category_status','brandid_y':'brandid'},inplace=True)
AllBrands = brandTableWithBrandId.append(brandTableWtoutBrandIdUpdated_1, ignore_index=True)
AllBrands.to_csv('AllBrands.csv')

DifferentCategories=AllBrands.groupby(['itemtype','itemsubtype','servicetype','thb_category']).size().reset_index(name='counts')
DifferentCategories.to_csv('DifferentCategories.csv')
