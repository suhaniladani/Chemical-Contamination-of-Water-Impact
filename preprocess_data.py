
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd


# ## Preprocessing Operations 

# In[2]:


def preprocess_chemicals(dataset_name = "chemicals.csv", yearbound=2000):
    df = pd.read_csv(dataset_name, encoding = "utf-8")
    for i, c in enumerate(df["contaminant_level"].unique()):
        df.loc[df["contaminant_level"]==c, "contaminant_level"] = i
    return df


# In[3]:


def preprocess_droughts(dataset_name='droughts', year_bound=2000):
    dataset_path = dataset_name + '.csv'
    df = pd.read_csv(dataset_path, encoding = "utf-8")
    valid_start = list(df['valid_start'])
    valid_start = list(map(lambda x: int(x[0:4]), valid_start))
    df['valid_start'] = valid_start
    valid_end = list(df['valid_end'])
    valid_end = list(map(lambda x: int(x[0:4]), valid_end))
    df['valid_end'] = valid_end
    valid_dates = df['valid_start'] >= year_bound
    df = df[valid_dates]
    df.to_csv(dataset_name + '_processed.csv', index=False)
    df['year'] = list(df['valid_start'])
    return df


# In[4]:


def preprocess_earnings(dataset_name='earnings', year_bound=2000):
    dataset_path = dataset_name + '.csv'
    df = pd.read_csv(dataset_path, encoding = "utf-8")
    df_mod = pd.DataFrame({'fips':df['fips'], 'county':df['county'], 'total_med':df['total_med'], 'year':df['year']})
    df.to_csv(dataset_name + '_processed.csv', index=False)
    possible_years = df['year'] >= year_bound
    df = df[possible_years]
    return df


# In[5]:


def preprocess_education_attainment(dataset_name="education_attainment", yearbound=2000):
    df = pd.read_csv(dataset_name + '.csv', encoding = "utf-8")
    df = df.loc[df["year"]>=yearbound]
    return df


# In[6]:


def preprocess_industry_occupation(dataset_name='industry_occupation', year_bound=2000):
    dataset_path = dataset_name + '.csv'
    df = pd.read_csv(dataset_path, encoding = "latin-1")
    df = df[df['year'] > year_bound]
    df.to_csv(dataset_name + '_processed.csv', index=False)
    return df


# In[7]:


def preprocess_water_usage(dataset_name = "water_usage", yearbound=2000):
    df = pd.read_csv("water_usage.csv")
    df = df.drop(['ps_groundwater', 'ps_surfacewater'], axis=1)
    return df


# In[8]:


preprocess_chemicals().to_csv("chemicals_processed.csv")
preprocess_droughts().to_csv("droughts_processed.csv")
preprocess_earnings().to_csv("earnings_processed.csv")
preprocess_education_attainment().to_csv("education_attainment_processed.csv")
preprocess_industry_occupation().to_csv("industry_occupation_processed.csv")
preprocess_water_usage().to_csv("water_usage_processed.csv")


# ## Joining Operations

# In[9]:


'''
@param df : pandas dataframe
@param other_df : another pandas dataframe
@reurn merge of two dataframes
'''
def join_two_datasets(df, other_df, fields):

    df['fips'] = df['fips'].astype(int)
    other_df['fips'] = other_df['fips'].astype(int)
    return pd.merge(df, other_df, on=fields)

'''
@return dictionary mapping name to pandas data frame
'''
def generate_dfs():
    all_dfs = {}
    all_dfs['water_usage'] = preprocess_water_usage()
    all_dfs['industry_occupation'] = preprocess_industry_occupation()
    all_dfs['chemicals'] = preprocess_chemicals()
    all_dfs['droughts'] = preprocess_droughts()
    all_dfs['earnings'] = preprocess_earnings()
    all_dfs['education_attainment'] = preprocess_education_attainment()
    return all_dfs

