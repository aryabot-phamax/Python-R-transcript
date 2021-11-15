#!/usr/bin/env python
# coding: utf-8
# In[133]:

import pandas as pd
from pandasql import sqldf
import csv
from datar.tidyr import pivot_longer
import numpy as np


# In[134]:


countries = pd.read_csv('countries.csv')
countries['mgmt_cluster_1']=np.nan
countries=countries.rename(columns={'Country Code':'Country_Code'})
countries = countries[countries["Country_Code"]!="GB"]
#countries["Country_Code"]=countries["Country_Code"].str.lower()
countries.loc[((countries['Country_Code'].str.lower() == 'uk') | (countries['Country_Code'].str.lower() == 'de') | (countries['Country_Code'].str.lower() == 'fr') | (countries['Country_Code'].str.lower() == 'it') | (countries['Country_Code'].str.lower() == 'es')), 'mgmt_cluster_1'] = 'EU5'
countries.loc[countries['Country_Code'].str.lower() == 'uk', 'Country'] = 'UK'
countries.loc[countries['Country_Code'].str.lower() == 'uk', 'Country_Code'] = 'GB'
countries


# In[135]:

user = pd.read_csv(r'E:\Downloads\11_10_2021\user.csv')
user

# In[136]:

user=user.rename(columns={'Id':'User_Id','Name':'User_Name'})
user

# In[137]:

user1 = sqldf("select * from user where User_Name not like '%c3i%'")
user1 = sqldf("select * from user where User_Name not like '%vacant%'")
user1

# In[138]:

territory = pd.read_csv(r'E:\Downloads\11_10_2021\territory.csv')
territory['ExternalId__c']="NaN"
territory['CurrencyIsoCode']="NaN"
territory=territory.rename(columns={'Id':'Territory_Id','Name':'Territory_Name','DeveloperName':'Territory_Dev_Name'})

# In[139]:

territory=territory[["Territory_Id","Territory_Name","Territory_Dev_Name"]]
territory

# In[140]:

user_territory_map = pd.read_csv(r'E:\Downloads\11_10_2021\User_Territory2Association.csv')
user_territory_map=user_territory_map.rename(columns={'Territory2Id':'Territory_Id'})
user_territory_map

# In[141]:

user_t=user.merge(user_territory_map,how="inner",left_on="User_Id",right_on="UserId")
user_t

# In[142]:

user_terr_name = sqldf("select ut.* , t.Territory_Name, t.Territory_Dev_Name from user_t ut Inner Join territory t On t.Territory_Id = ut.Territory_Id")
user_terr_name

# In[143]:

user_terr_name_country = user_terr_name.merge(countries,how="inner",left_on="QIDC__OK_Available_Countries_ims__c",right_on="Country_Code")

# In[144]:

accounts = pd.read_csv(r'E:\Downloads\11_11_2021\account.csv',engine='python', error_bad_lines=False)
accounts

# In[145]:

account_terr = pd.read_csv('OCE__AccountTerritoryFields__c.csv',engine='python', error_bad_lines=False)
account_terr

# In[146]:

accounts = accounts[accounts["OCE__IsActive__c"]!="FALSE"]
accounts=accounts.rename(columns={'Id':'Account_Id','Santen_Cationorm_Adoption_Ladder__c':'Cationorm-Adoption_Ladder','Santen_Cationorm_Adoption__c':'Cationorm-Adoption','Santen_Cationorm_Limiting_Beliefs__c':'Cationorm-Limiting_Beliefs','Santen_Cationorm_Segment__c':'Cationorm-Segment','Santen_Cosopt_Adoption_Ladder__c':'Cosopt-Adoption_Ladder','Santen_Cosopt_Adoption__c':'Cosopt-Adoption','Santen_Cosopt_Limiting_Beliefs__c':'Cosopt-Limiting_Beliefs','Santen_Cosopt_Segment__c':'Cosopt-Segment','Santen_Ducressa_Adoption_Ladder__c':'Ducressa-Adoption_Ladder','Santen_Ducressa_Adoption__c':'Ducressa-Adoption','Santen_Ducressa_Limiting_Beliefs__c':'Ducressa-Limiting_Beliefs','Santen_Ducressa_Segment__c':'Ducressa-Segment','Santen_Ikervis_Adoption_Ladder__c':'Ikervis-Adoption_Ladder','Santen_Ikervis_Adoption__c':'Ikervis-Adoption','Santen_Ikervis_Limiting_Beliefs__c':'Ikervis-Limiting_Beliefs','Santen_Ikervis_Segment__c':'Ikervis-Segment','Santen_Oftaquix_Adoption__c':'Oftaquix-Adoption','Santen_Oftaquix_Segment__c':'Oftaquix-Segment','Santen_Taflutan_Saflutan_Adoption_Ladder__c':'Taflutan_Saflutan-Adoption_Ladder','Santen_Taflutan_Saflutan_Adoption__c':'Taflutan_Saflutan-Adoption','Santen_Taflutan_Saflutan_Limiting_Be__c':'Taflutan_Saflutan-Limiting_Beliefs','Santen_Taflutan_Saflutan_Segment__c':'Taflutan_Saflutan-Segment','Santen_Taptiqom_Loyada_Adoption_Ladder__c':'Taptiqom_Loyada-Adoption_Ladder','Santen_Taptiqom_Loyada_Adoption__c':'Taptiqom_Loyada-Adoption','Santen_Taptiqom_Loyada_Limiting_Beliefs__c':'Taptiqom_Loyada-Limiting_Beliefs','Santen_Taptiqom_Loyada_Segment__c':'Taptiqom_Loyada-Segment','Santen_Verkazia_Adoption_Ladder__c':'Verkazia-Adoption_Ladder','Santen_Verkazia_Adoption__c':'Verkazia-Adoption','Santen_Verkazia_Limiting_Beliefs__c':'Verkazia-Limiting_Beliefs','Santen_Verkazia_Segment__c':'Verkazia-Segment'})
accounts[['Account_Id','IsPersonAccount','OCE__AccountFullName__c','OCE__CountryCode__c','OCE__IntegrationID__c','OCE__IsActive__c','OCE__ParentAccount__c','OCE__Status__c','OCE__RecordTypeName__c','Cationorm-Adoption_Ladder','Cationorm-Adoption','Cationorm-Limiting_Beliefs','Cationorm-Segment','Cosopt-Adoption_Ladder','Cosopt-Adoption','Cosopt-Limiting_Beliefs','Cosopt-Segment','Ducressa-Adoption_Ladder','Ducressa-Adoption','Ducressa-Limiting_Beliefs','Ducressa-Segment','Ikervis-Adoption_Ladder','Ikervis-Adoption','Ikervis-Limiting_Beliefs','Ikervis-Segment','Oftaquix-Adoption','Oftaquix-Segment','Taflutan_Saflutan-Adoption_Ladder','Taflutan_Saflutan-Adoption','Taflutan_Saflutan-Limiting_Beliefs','Taflutan_Saflutan-Segment','Taptiqom_Loyada-Adoption_Ladder','Taptiqom_Loyada-Adoption','Taptiqom_Loyada-Limiting_Beliefs','Taptiqom_Loyada-Segment','Verkazia-Adoption_Ladder','Verkazia-Adoption','Verkazia-Limiting_Beliefs','Verkazia-Segment']]

# In[147]:

longer_account = pivot_longer(accounts,cols = 10-39,names_sep = '-', names_to = ('products','.value'))
longer_account

# In[148]:

account_terr_user=account_terr.merge(user_terr_name_country,how="inner",left_on="OCE__Territory__c",right_on="Territory_Name")
account_terr_user

# In[149]:

account_terr_user=account_terr.merge(user_terr_name_country,how="inner",left_on="OCE__Territory__c",right_on="Territory_Name")

# In[150]:

account_merged=account_terr_user.merge(longer_account,how="inner",left_on="OCE__Account__c",right_on="Account_Id")
account_merged['OCE__AccountTerritoryUnique__c']='NaN'
account_merged['OCE__UniqueIntegrationID__c']='NaN'
account_merged['QIDC__OK_Available_Countries_ims__c']='NaN'
account_merged['User_Id']='NaN'
account_merged['IsActive']='NaN'
account_merged['Territory_Id']='NaN'
account_merged['Territory_Dev_Name']='NaN'
account_merged['OCE__CountryCode__c']='NaN'
account_merged['OCE__IntegrationID__c']='NaN'
account_merged['OCE__IsActive__c']='NaN'
account_merged['OCE__Status__c']='NaN'

# In[151]:

account_merged=account_merged.rename(columns={'Santen_Customer_Segment__c':'Customer_Segment q','OCE__AccountFullName__c':'OCE__AccountName__c'})
account_merged

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:



