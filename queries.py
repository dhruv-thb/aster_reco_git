# -*- coding: utf-8 -*-
"""
Created on Sun Jan  9 15:06:56 2022

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
import math
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()

########################################QUERIES#########################################

"""
Queries and their explanations:
    1.brand_mapping_query: This query helps in fetching data for the no of times two brands are
                            purchased together the query is cross joined on patient remote id 
                            and booking date.
                            
   2.brand_mapping_query_customer: only difference btw this query and brand_mapping_query is 
                                  the cross join part this one is merged only on patient remote is
                                  so we can say that the counting is done on the baisis of customer or patient.
                                  
   3.query_reco_product: This query helps us n fetching all the details related to the brandid.

                                
"""






#######################################################################################

brand_mapping_query='''select brandid1,brandid2,count(1) as freq_at_customer from
         (select brandid1,brandid2 from
         (select cast(t.brandid as text)as brandid1,i.patientremoteid,date(i.bookingdate) as bookingdate , count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>= 'startdate' and bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid,date(i.bookingdate)) first
          join
         (select i.patientremoteid,date(i.bookingdate) as bookingdate,cast(t.brandid as text) as brandid2, count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>='startdate' and bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid,date(i.bookingdate)) second
         on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate )
         metric
         group by  
         brandid1,brandid2'''


#####################################################################################



########################################################################################
brand_mapping_query_customer='''select brandid1,brandid2,count(1) as freq_at_customer from
         (select brandid1,brandid2 from
         (select cast(t.brandid as text)as brandid1,i.patientremoteid,date(bookingdate) as bookingdate ,count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid, date(bookingdate) )first
          join
         (select i.patientremoteid,cast(t.brandid as text) as brandid2,date(bookingdate) as bookingdate ,  count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>='startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid, date(bookingdate) ) second
         on first.patientremoteid =second.patientremoteid and second.bookingdate > first.bookingdate)
         metric
         group by  
         brandid1,brandid2
         '''
########################################################################################









########################################################################################
brand_mapping_query_customer='''select brandid1,brandid2,count(1) as freq_at_customer from
         (select brandid1,brandid2 from
         (select cast(t.brandid as text)as brandid1,i.patientremoteid, count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid )first
          join
         (select i.patientremoteid,cast(t.brandid as text) as brandid2, count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         where i.bookingdate>='startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid) second
         on first.patientremoteid =second.patientremoteid)
         metric
         group by  
         brandid1,brandid2'''
########################################################################################







####################################################
query_reco_product = '''select * from recoproduct'''
##################################################

















query_tf_idf_with_mobile = '''select t.brandid as brandid, t.brand as brand_name,t.thb_category ,t.itemtype ,t.itemsubtype ,t.servicetype ,count(distinct i.bookingid) as num_trans,count(distinct i.patientremoteid) as num_pats
from (select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid) mb
inner join s_billing.items i on i.patientremoteid=mb.remoteid
inner join s_billing.meta m2 on m2.id = i.billingmetaid 
inner join thbbrandmapping t on t.id  = m2.id
group by  brandid, brand_name,t.thb_category ,t.itemtype ,t.itemsubtype ,t.servicetype
'''











query_tf_idf_withoutmobile = '''select t.brandid as brandid,t.brand as brand_name ,t.thb_category ,t.itemtype ,t.itemsubtype ,t.servicetype ,count(distinct i.bookingid) as num_trans,count(distinct i.patientremoteid) as num_pats
from s_billing.items i 
inner join s_billing.meta m2 on m2.id = i.billingmetaid 
inner join thbbrandmapping t on t.id  = m2.id
group by  brandid,brand_name, t.thb_category ,t.itemtype ,t.itemsubtype ,t.servicetype
'''

       










query_pat_trans_with_mobile = '''select count(distinct i.bookingid) as num_trans,count(distinct i.patientremoteid) as num_pats
from (select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid) mb
inner join s_billing.items i on i.patientremoteid=mb.remoteid
'''




brand_mapping_bkng_with_icd_query='''


select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1,bookingdate from
((select distinct patientremoteid,cast(brandid as text) as brandid1,date(bookingdate) as bookingdate
from
(select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid1, date(bookingdate) as bookingdate
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) first



join



(select patientremoteid,brandid2,bookingdate from
((select distinct patientremoteid,cast(brandid as text) as brandid2,date(bookingdate) as bookingdate
from
(select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid2, date(bookingdate) as bookingdate
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate )
metric
group by
brandid1,brandid2
'''



brand_mapping_bkng_with_icd_query_updated='''


select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1,bookingdate from
((select distinct patientremoteid,cast(brandid as text) as brandid1,date(bookingdate) as bookingdate
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid1, date(bookingdate) as bookingdate
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) first



join



(select patientremoteid,brandid2,bookingdate from
((select distinct patientremoteid,cast(brandid as text) as brandid2,date(bookingdate) as bookingdate
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid2, date(bookingdate) as bookingdate
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate )
metric
group by
brandid1,brandid2




'''








brand_mapping_bkng_with_icd_query_updated_customer='''


select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1 from
((select distinct patientremoteid,cast(brandid as text) as brandid1
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid1
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) first



join



(select patientremoteid,brandid2 from
((select distinct patientremoteid,cast(brandid as text) as brandid2
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid2
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid )
metric
group by
brandid1,brandid2




'''





brand_mapping_bkng_otc='''

select brandid1,brandid2,count(1) as frequency from
(select brandid1,brandid2 from
(select i.patientremoteid ,date(i.bookingdate) as bookingdate,brandid as brandid1 from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
inner join 
(select  distinct patientremoteid ,date(bookingdate) as bookingdate from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
where  servicetype!='pharma classification' and bookingdate>='2019-01-01' and bookingdate<='2022-01-01' 
)y
on i.patientremoteid =y.patientremoteid and date(i.bookingdate) =date(y.bookingdate)
group by t.brandid,i.patientremoteid,date(i.bookingdate)) first 

inner join 

(select i.patientremoteid ,date(i.bookingdate) as bookingdate,brandid as brandid2   from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
inner join 
(select  distinct patientremoteid ,date(bookingdate) as bookingdate from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
where  servicetype!='pharma classification' and bookingdate>='2019-01-01' and bookingdate<='2022-01-01' 
)y
on i.patientremoteid =y.patientremoteid and date(i.bookingdate) =date(y.bookingdate)
group by t.brandid,i.patientremoteid,date(i.bookingdate)) second

on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate)metric
group by  brandid1,brandid2
'''



brand_mapping_customer_otc='''



select brandid1,brandid2,count(1) as frequency from
(select brandid1,brandid2 from
(select i.patientremoteid ,brandid as brandid1 from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
inner join 
(select  distinct patientremoteid  from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
where  servicetype!='pharma classification' and bookingdate>='2019-01-01' and bookingdate<='2022-01-01' 
)y
on i.patientremoteid =y.patientremoteid
group by t.brandid,i.patientremoteid) first 

inner join 

(select i.patientremoteid ,brandid as brandid2   from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
inner join 
(select  distinct patientremoteid from s_billing.items i
inner join thbbrandmapping t 
on i.billingmetaid =t.id
where  servicetype!='pharma classification' and bookingdate>='2019-01-01' and bookingdate<='2022-01-01' 
)y
on i.patientremoteid =y.patientremoteid 
group by t.brandid,i.patientremoteid) second

on first.patientremoteid =second.patientremoteid) metric
group by  brandid1,brandid2
'''



#######################################  Non Symetric Cosine Similarity ############################

updated_cosine_similarity_Otc='''
select brandid1,brandid2,count(1) as freq_at_customer from
         (select brandid1,brandid2 from
         (select cast(t.brandid as text)as brandid1,i.patientremoteid,date(i.bookingdate) as bookingdate ,count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         inner join 
			(select  distinct patientremoteid ,date(i.bookingdate) as bookingdate from s_billing.items i
			inner join thbbrandmapping t 
			on i.billingmetaid =t.id
			where  servicetype!='pharma classification' and i.bookingdate>='startdate' and i.bookingdate<='enddate' 
			)y
			on i.patientremoteid =y.patientremoteid and date(i.bookingdate) =date(y.bookingdate)
         where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid, date(i.bookingdate) )first
          join
         (select i.patientremoteid,cast(t.brandid as text) as brandid2,date(i.bookingdate) as bookingdate ,  count(1) as num_rows from s_billing.items i
         inner join thbbrandmapping t on t.id = i.billingmetaid
         inner join 
			(select  distinct patientremoteid ,date(i.bookingdate) as bookingdate from s_billing.items i
			inner join thbbrandmapping t 
			on i.billingmetaid =t.id
			where  servicetype!='pharma classification' and i.bookingdate>='startdate' and i.bookingdate<='enddate' 
			)y
			on i.patientremoteid =y.patientremoteid and date(i.bookingdate) =date(y.bookingdate) 
         where i.bookingdate>='startdate' and i.bookingdate < 'enddate'
         group by t.brandid,i.patientremoteid, date(i.bookingdate)         
         ) second
         on first.patientremoteid =second.patientremoteid and second.bookingdate > first.bookingdate)
         metric
         group by  
         brandid1,brandid2
'''





























brand_mapping_cust_with_icd_query= '''
select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1 from
((select distinct patientremoteid,cast(brandid as text) as brandid1
from
(select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid1
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) first



join



(select patientremoteid,brandid2 from
((select distinct patientremoteid,cast(brandid as text) as brandid2
from
(select distinct p.remoteid from
s_pii.mobileindex m
inner join s_patient.patientwithsrc p
on
m.id=p.mobileid
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
where i.bookingdate>= 'startdate' and i.bookingdate < 'enddate')
union
(select distinct patientremoteid,substring(code,1,3) as brandid2
from s_emr.emr
where bookingdate>= 'startdate' and bookingdate < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid )
metric
group by
brandid1,brandid2
'''



thb_brand_mapping=''' select m.id,m.testname,m.brand,t.brandid ,s.value as servicetype,i.value as itemtype,i2.value as itemsubtype,t.thb_category ,t.category_status from s_billing.meta m
inner join s_constants.itemtype i on i.id = m.itemtypeid
inner join s_constants.itemsubtype i2 on i2.id =m.itemsubtypeid
inner join s_constants.servicetype s on s.id = m.servicetypeid
left join thbbrandmapping t on t.id =m.id'''