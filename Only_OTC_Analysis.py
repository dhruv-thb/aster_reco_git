# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 13:38:33 2022

@author: Dhruv
"""


import pandas as pd
import psycopg2
import pandas
import numpy as np 
from Common_func import * 
import re
import itertools
# main()
import math
import Model_2_func as md2

import os
conn = psycopg2.connect(host="172.18.221.133",dbname="asterpharmacy", user="asterdhruv", password="al$hanalytics#123")
cur = conn.cursor()









############################################ Booking Data #############################################

brand_vs_otc_booking_data=run_query(data_extraction_query(brand_vs_otc_booking,'2019-01-01', '2022-01-01'))
brand_vs_otc_booking_data=pd.read_pickle('brand_vs_icd_otc_booking')
item_comb,brand_mapping_grp=check_func(brand_vs_otc_booking_data, 0)
# item_comb['icd_brand1']=np.where(item_comb['brandid1'].str.isdigit(),'brand','icd')
# item_comb['icd_brand2']=np.where(item_comb['brandid2'].str.isdigit(),'brand','icd')
cosine_sim1=md2.cosin_sim_2(brand_vs_otc_booking_data,10,brand_mapping_grp,False,True)
final_result,Result_long_form=final_shapping(cosine_sim1,brand_mapping_grp,item_comb)
Result_long_form['value']=Result_long_form['value'].astype('float')
Result_long_form["rank"] = Result_long_form.groupby("firstproductid")["value"].rank("dense", ascending=False)
icd_vs_brand_booking=Result_long_form



icd_vs_brand_booking=pd.read_csv('Booking_icd_vs_brand_otclong.csv')
icd_vs_brand_booking['icd_brand1']=np.where(icd_vs_brand_booking['firstproductid'].str.isdigit(),'brand','icd')
icd_vs_brand_booking=icd_vs_brand_booking[icd_vs_brand_booking['icd_brand1']=='icd']
icd_vs_brand_booking.to_csv('icd_vs_brand_booking.csv')








######################################### Customer data ##########################################

brand_vs_otc_customer_data=run_query(data_extraction_query(brand_vs_otc_customer,'2019-01-01', '2022-01-01'))
brand_vs_otc_customer_data=pd.read_pickle('brand_vs_icd_otc_customer_data')
item_comb,brand_mapping_grp=check_func(brand_vs_otc_customer_data, 0)
# item_comb['icd_brand1']=np.where(item_comb['brandid1'].str.isdigit(),'brand','icd')
# item_comb['icd_brand2']=np.where(item_comb['brandid2'].str.isdigit(),'brand','icd')
cosine_sim1=md2.cosin_sim_2(brand_vs_otc_customer_data,100,brand_mapping_grp,True,True)
final_result,Result_long_form=final_shapping(cosine_sim1,brand_mapping_grp,item_comb)
Result_long_form['value']=Result_long_form['value'].astype('float')
Result_long_form["rank"] = Result_long_form.groupby("firstproductid")["value"].rank("dense", ascending=False)




########################### item to item customer ####################################

brand_vs_otc_customer_data=run_query(data_extraction_query(brand_vs_otc_customer_all,'2019-01-01', '2022-01-01'))
brand_vs_otc_customer_data=pd.read_pickle('brand_vs_brand_otc_customer_data')
item_comb,brand_mapping_grp=check_func(brand_vs_otc_customer_data, 0)
cosine_sim1=md2.cosin_sim_2(brand_vs_otc_customer_data,100,brand_mapping_grp,True,True)
final_result,Result_long_form=final_shapping(cosine_sim1,brand_mapping_grp,item_comb)
Result_long_form['value']=Result_long_form['value'].astype('float')
Result_long_form["rank"] = Result_long_form.groupby("firstproductid")["value"].rank("dense", ascending=False)


Result_long_form.to_csv('item_vs_item_OTC_Customer.csv')












brand_vs_otc_customer='''

select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1  from
((select distinct i.patientremoteid,cast(brandid as text) as brandid1
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)

where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')
union
(select distinct e.patientremoteid,substring(code,1,3) as brandid1
from s_emr.emr e
inner join 
(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=e.patientremoteid  and s.bookingdate=date(e.bookingdate)

where date(s.bookingdate)>= 'startdate' and date(s.bookingdate) < 'enddate'
))x
) first



join



(select patientremoteid,brandid2 from
((select distinct i.patientremoteid,cast(brandid as text) as brandid2
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid

inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)


where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')
union
(select distinct e.patientremoteid,substring(code,1,3) as brandid2
from s_emr.emr e
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=e.patientremoteid  and s.bookingdate=date(e.bookingdate)

where date(s.bookingdate)>= 'startdate' and date(s.bookingdate) < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid )
metric
group by
brandid1,brandid2

'''

















































brand_vs_otc_booking='''

select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1 ,bookingdate  from
((select distinct i.patientremoteid,cast(brandid as text) as brandid1, date(i.bookingdate) as bookingdate
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)

where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')
union
(select distinct e.patientremoteid,substring(code,1,3) as brandid1,date(e.bookingdate) as bookingdate
from s_emr.emr e
inner join 
(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=e.patientremoteid  and s.bookingdate=date(e.bookingdate)

where date(s.bookingdate)>= 'startdate' and date(s.bookingdate) < 'enddate'
))x
) first



join



(select patientremoteid,brandid2 ,bookingdate  from
((select distinct i.patientremoteid,cast(brandid as text) as brandid2, date(i.bookingdate) as bookingdate
from
(select distinct patientremoteid as remoteid from
s_emr.emr e 
)mb
inner join s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)

where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')
union
(select distinct e.patientremoteid,substring(code,1,3) as brandid1,date(e.bookingdate) as bookingdate
from s_emr.emr e
inner join 
(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=e.patientremoteid  and s.bookingdate=date(e.bookingdate)

where date(s.bookingdate)>= 'startdate' and date(s.bookingdate) < 'enddate'
))x
) second
on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate) 
metric
group by
brandid1,brandid2

'''


######################################### NO EMR FILTER #######################################







brand_vs_otc_customer_all='''
select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1  from
(select distinct i.patientremoteid,cast(brandid as text) as brandid1
 from s_billing.items i
   inner join thbbrandmapping t on t.id=i.billingmetaid
      inner join 
        (select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
         inner join thbbrandmapping t 
         on t.id =i.billingmetaid 
         where t.servicetype !='pharma classification')s
   on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)
where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')x
) first

join

(select patientremoteid,brandid2 from
(select distinct i.patientremoteid,cast(brandid as text) as brandid2
from s_billing.items i
    inner join thbbrandmapping t on t.id=i.billingmetaid
        inner join 
            (select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
            inner join thbbrandmapping t 
            on t.id =i.billingmetaid 
            where t.servicetype !='pharma classification')s
        on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)
 where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')x
) second
on first.patientremoteid =second.patientremoteid )
metric
group by
brandid1,brandid2
'''


brand_vs_otc_booking_all='''

select brandid1,brandid2,count(1) as freq_at_customer from
((select patientremoteid,brandid1 ,bookingdate  from
((select distinct i.patientremoteid,cast(brandid as text) as brandid1, date(i.bookingdate) as bookingdate
from
 s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)

where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')

))x
) first



join



(select patientremoteid,brandid2 ,bookingdate  from
((select distinct i.patientremoteid,cast(brandid as text) as brandid2, date(i.bookingdate) as bookingdate
from
s_billing.items i
on i.patientremoteid=mb.remoteid
inner join thbbrandmapping t on t.id=i.billingmetaid
inner join 

(select distinct i.patientremoteid ,date(i.bookingdate) as bookingdate  from s_billing.items i 
inner join thbbrandmapping t 
on t.id =i.billingmetaid 
where t.servicetype !='pharma classification')s
on s.patientremoteid=i.patientremoteid  and s.bookingdate=date(i.bookingdate)

where date(i.bookingdate)>= 'startdate' and date(i.bookingdate) < 'enddate')

))x
) second
on first.patientremoteid =second.patientremoteid and first.bookingdate=second.bookingdate) 
metric
group by
brandid1,brandid2

'''


