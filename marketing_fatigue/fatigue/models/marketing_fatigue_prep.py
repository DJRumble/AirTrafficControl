import pandas as pd
import numpy as np
from multiprocessing import Pool
from multiprocessing import set_start_method
from os import fork, getpid
import time
import multiprocessing as mp
import pickle
from functools import partial
from aws_tools.athena_tools import AthenaQuerier
from pyathena import connect
from tqdm import tqdm
import sys

conn = connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                   region_name='eu-west-2') 

def customer_event_hist_prep(main_dataset,inputs):
    '''
    customer_event_hist_prep creates a sequential list of...
    customer events (in this case Business Objectives)
    responses (in this case optout)
    ids in prep for the LSTM model
    
    In:
    main_dataset - the original, cleaned data 
    inputs - main_dataset split into several parts for multiple processing
    event - string column header for the chosen events
    
    outputs:
    event_list - list of events over 60 days
    response_list - list of customer responses (i.e. optout)
    customers_ids_list - list of the customers  
    '''
    event = 'business_objective'
    
    event_list = []
    response_list = []
    customers_ids_list = []
    delivery_dates_list = []

    for index, row in inputs.iterrows():
        # for every row in our data, locate the customer it belongs to, and return a dataframe of only thier history
        customer_hist=main_dataset.loc[main_dataset['ee_customer_id'] == row.ee_customer_id]

        #Create a new column in the customer_hist calculating the time difference between an event, and thier most recent (Max) event
        customer_hist["time_to_end"] = (max(customer_hist['date_of_delivery']) - customer_hist['date_of_delivery']).dt.days  

        #Create a list of 60 zeros as a basis of the customer actions across that time 
        customer_events = [0] * 60

        #loop across the customer history and populate customer_events with our event metadata
        for index, row_2 in customer_hist.iterrows():
            #IMPORTANT - here is where is deffine the metadata which will reffine our customer events
            customer_events[-row_2.time_to_end] = row_2[event]    #business_objective

        #Append all info to lists
        event_list.append(customer_events)
        response_list.append(row.optout_flag)
        customers_ids_list.append(row.ee_customer_id)    
        delivery_dates_list.append(row.date_of_delivery)    
        
    return event_list,response_list,customers_ids_list,delivery_dates_list



def main(title,base,event):
    '''
    Load our prepared dataset and run the 'customer_event_hist_prep' function through 'multiprocessing'
    
    Save the prepared data arrays and the run info to pickle files for later use
    
    input
    title - string to attach to the end of the pickle files to specify this run
    event - string column header for the chosen events
    
    Return arrays for use
    '''
    
    program_starts = time.time()
    print("Starting the script .....")
    print("Loading from {}".format(base))
    print("Writing to '{}'".format(title))

    athena = AthenaQuerier()
    it = 500000
    
    sql_code = '''select * from  campaign_data.{} order by ee_customer_id, date_of_delivery'''.format(base)
    iterator= athena.execute_query(sql_code, **{'chunksize':it})
    
    df_cnt = pd.read_sql('''select count(*) as cnt from campaign_data.{}'''.format(base), conn)
    df_mxdt = pd.read_sql('''select max(date_of_delivery) as date from campaign_data.{}'''.format(base), conn)

    print('athena table length = {}'.format(df_cnt['cnt'][0]))
    print('most recent entry = {}'.format(df_mxdt['date'][0]))
    
    my_output_list = []
    my_time_list = []
    
    i = it
    
    set_start_method("spawn")
    
    def mp_func(functional,df_split_2):
        #multiprocessing fucntion
        with Pool(processes=mp.cpu_count()) as pool:
            #print("PID = ", getpid())
            return pool.map(functional, df_split_2)
    
    for main_dataset in tqdm(iterator):
        #Counter
        print('{}% complete'.format(round(100*(i/df_cnt['cnt'][0]),2)))
        i = i+it
        
        #clean data and group by 'size'
        main_dataset['optout_flag'].fillna(0,inplace=True)
        main_dataset['optout_date'].fillna('',inplace=True)
        #main_dataset["date_of_delivery"] = pd.to_datetime(main_dataset["date_of_delivery"])
        mytable = main_dataset.groupby(['ee_customer_id','optout_flag','optout_date']).agg(size=('ee_customer_id', 'size'), date_of_delivery=('date_of_delivery', 'max')).reset_index()
        #mytable = main_dataset.groupby(['ee_customer_id','optout_flag','optout_date']).size().reset_index()
        mytable["optout_date"]= pd.to_datetime(mytable["optout_date"])
        mytable["date_of_delivery"]= pd.to_datetime(mytable["date_of_delivery"])
        
        main_dataset = main_dataset[['ee_customer_id', 'date_of_delivery',event]]

        print("finishing getting the data from athena")
        ##print("Number of cores: ",mp.cpu_count())
       
        #Split data across the number of cores
        df_split = np.array_split(mytable, mp.cpu_count())
        df_split_2 = df_split[0:mp.cpu_count()]
        #print('df_split_2[i] size = {}'.format(len(df_split_2[0])))
        #print('main_dataset rows = {}'.format(len(main_dataset)))  
        #print('main_dataset size = {}'.format(sys.getsizeof(main_dataset)))       

        #Partially load up the 'customer_event_hist_prep' function
        functional = partial(customer_event_hist_prep,main_dataset)
        #run multiprocessing function
        massive_list = mp_func(functional,df_split_2) 
                
        #print("length of my final list: ",len(massive_list))
        my_output_list.append(massive_list)
        print("finishing processing data")
    
    with open('./data/processed/mfmodel_prepdata_{}.pkl'.format(title), 'wb') as f:
        pickle.dump(my_output_list, f)
  
    print("Ending the job........")
    now = time.time()
    timeTrack = now - program_starts
    my_time_list.append(timeTrack)
    with open('./model_objects/timelist/mfmodel_timelist_{}.pkl'.format(title), 'wb') as f:
        pickle.dump(my_time_list, f)
    print("It has been {0} seconds since strating the script".format(now - program_starts))
    
    return my_output_list,my_time_list

if __name__=='__main__':
    main() 
    