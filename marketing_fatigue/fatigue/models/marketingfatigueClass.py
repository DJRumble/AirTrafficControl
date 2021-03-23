import time

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import copy

import datetime

import os
os.environ['NUMEXPR_MAX_THREADS'] = '48'

from pyathena import connect

import pickle

from tqdm import tqdm

from pyspark.sql import SparkSession

import multiprocessing as mp
from multiprocessing import Pool
from multiprocessing import set_start_method

from functools import partial

from aws_tools.athena_tools import AthenaQuerier
from pyathena import connect

from keras.models import Sequential
from keras.models import load_model
from keras.layers import Embedding,LSTM,Dense,Dropout,Bidirectional
from keras.initializers import Constant
from keras.optimizers import Adam

import keras

from sklearn.metrics import confusion_matrix, classification_report
from sklearn.metrics import recall_score, precision_score, accuracy_score, f1_score

import fatigue.models.marketing_fatigue_prep as marketing_fatigue_prep


class MarketingFatigue_sequence_prep:
    '''
    This class represents the data prep steps for the marketing fatigue model
    
    v1.0 - These scripts existed with Alvertos' module - https://cdatasci-ami.notebook.eu-west-2.sagemaker.aws/edit/marketing_fatigue_model/marketing_fatigue_prep.py
    v1.1 - merger with the existing MarketingFatigue Class
    v1.2 - merger with SQL scripts 
    v1.3 - merger with prep scripts (not yet functioning)
    v1.4 - Generate global word2id
    v1.5 - Calculate Eligibility has a final model output
    v1.6 - Update scoring method to increase efficency
    v1.7 - prepare MVP for deployment via cookie cutter
    v1.8 - combine methods into single 'scoring_job' object
    v1.9 - Fixed bugs and completed E2E scoring 
    
    note - when running in notebook, please change the opperating directory to the root
    import os
    os.chdir('../')
    '''
    def __init__(self,base_id,athena_table_name,event= 'business_objective'):
        '''
        When the class is initialised, be sure to specify the 'base_id' with a string a represents to a common identifier 
        that has already been defined as part of the use of the ETL class 'marketing_fatigue_prep'. Examples of initialisation are given below
        
        Global inputs:
            - event = metadata to squence, set to business objective by default
            - base_id = 'string' term to indentify all future model and data artificats to this version
            - athena_table_name = 'string' refers to the has been prepared in athena to hold of the pre pared SCCV


        List of currently available base_ids:
        BO_2020H2
        BO_2020H2_score
        BD
        '''
        self.event = event
        self.base = base_id
        self.table_name = athena_table_name
        
        self.conn = connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                   region_name='eu-west-2') 
        
        return    
    
    def run_marketing_fatigue_prep(self):
        '''
        Because I cannot as yet work out how to run multi processing within the class, for the time being we are going to have to continue with Alvertos' 'marketing_fatigue_prep.py' module that can be found here - https://cdatasci-ami.notebook.eu-west-2.sagemaker.aws/edit/marketing_fatigue_model/marketing_fatigue_prep.py
        
        Code is copied in the 'customer_event_hist_prep' and 'load_prep_save_squence_data'
        
        outputs two arrays - my_output_list,my_time_list
        '''
        my_output_list,my_time_list = marketing_fatigue_prep.main(self.base,self.table_name,self.event)
        return my_output_list,my_time_list

    
    def customer_event_hist_prep(self,main_dataset,inputs):
        '''
        customer_event_hist_prep creates a sequential list of...
        customer events (in this case Business Objectives)
        responses (in this case optout)
        ids in prep for the LSTM model
        
        In:
        main_dataset - the original, cleaned data 
        inputs - main_dataset split into several parts for multiple processing
        event - string column header for the chosen events (deffined globally, default 'business_objective')
        
        outputs:
        event_list - list of events over 60 days
        response_list - list of customer responses (i.e. optout)
        customers_ids_list - list of the customers       
        '''

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
                customer_events[-row_2.time_to_end] = row_2[self.event]    #default = business_objective
    
            #Append all info to lists
            event_list.append(customer_events)
            response_list.append(row.optout_flag)
            customers_ids_list.append(row.ee_customer_id)    
            delivery_dates_list.append(row.date_of_delivery)    
            
        return event_list,response_list,customers_ids_list,delivery_dates_list
    
    def load_prep_save_squence_data(self):
        '''
        CURRENTLY BROKEN - CANNOT BUILD PARTIAL FUNCTION WITHIN A CLASS
        
        Load our prepared dataset and run the 'customer_event_hist_prep' function through 'multiprocessing'

        Save the prepared data arrays and the run info to pickle files for later use

        input
        base_id - string to attach to the end of the pickle files to specify this run
        event - string column header for the chosen events

        Return arrays for use
        '''
                
        program_starts = time.time()
        print("Starting the script .....")

        athena = AthenaQuerier()
        sql_code = '''select * from  campaign_data.{} order by ee_customer_id, date_of_delivery '''.format(self.table_name)
        iterator= athena.execute_query(sql_code, **{'chunksize':1000000})
        
        my_output_list = []
        my_time_list = []

        set_start_method("spawn")

        for main_dataset in tqdm(iterator):

            #clean data and group by 'size'
            main_dataset['optout_flag'].fillna(0,inplace=True)
            main_dataset['optout_date'].fillna('',inplace=True)
            #main_dataset["date_of_delivery"] = pd.to_datetime(main_dataset["date_of_delivery"])
            mytable = main_dataset.groupby(['ee_customer_id','optout_flag','optout_date']).agg(size=('ee_customer_id', 'size'), date_of_delivery=('date_of_delivery', 'max')).reset_index()
            #mytable = main_dataset.groupby(['ee_customer_id','optout_flag','optout_date']).size().reset_index()
            mytable["optout_date"]= pd.to_datetime(mytable["optout_date"])
            mytable["date_of_delivery"]= pd.to_datetime(mytable["date_of_delivery"])

            print("finishing getting the data from athena")
            print("Number of cores: ",mp.cpu_count())

            #Split data across the number of cores
            df_split = np.array_split(mytable, mp.cpu_count())
            df_split_2 = df_split[0:mp.cpu_count()]

            #Partially load up the 'customer_event_hist_prep' function
            functional = partial(self.customer_event_hist_prep,main_dataset)   ### PROBLEM LINE ###

            #Multiprocessing
            pool = Pool(processes=mp.cpu_count())
            massive_list = pool.map(functional, df_split_2)
            print("length of my final list: ",len(massive_list))
            my_output_list.append(massive_list)

        with open('./data/processed/mfmodel_prepdata_{}.pkl'.format(self.base_id), 'wb') as f:
            pickle.dump(my_output_list, f)

        print("Ending the job........")
        now = time.time()
        timeTrack = now - program_starts
        my_time_list.append(timeTrack)
        with open('./model_objects/timelist/mfmodel_timelist_{}.pkl'.format(self.base_id), 'wb') as f:
            pickle.dump(my_time_list, f)
        print("It has been {0} seconds since strating the script".format(now - program_starts))

        return my_output_list,my_time_list    



class MarketingFatigue_model:
    
    '''
    This class represents the Air Traffic Control Marketing Fatigue model - Damian Rumble
    
    v1.0 [13/01/2021] - intial rebuild of Alvertos' code and refactoring into a class. Sucessfully running of the model and and exploration of model output
    
    
    '''
    
    
    def __init__(self,base_id):
        '''
        When the class is initialised, be sure to specify the 'base_id' with a string a represents to a common identifier 
        that has already been defined as part of the use of the ETL class 'marketing_fatigue_prep'. Examples of initialisation are given below
        
        ETL:        
        my_output_list,my_time_list = marketing_fatigue_prep.main(base_id,'atc_mfmodel_training')
        
        Model:
        mfmodel = MarketingFatigue_model('BO_2020H2')
        
        List of currently available base_ids:
        BO_2020H2
        BO_2020H2_score
        BD
        '''
        self.base = base_id
        
        self.conn = connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                   region_name='eu-west-2') 
        
        return
    
    def test_nans_present(self,df_feature):
        '''
        UNIT test
        
        Check if NANs are present in a output dataframe, for example - df_eligibility.days_since_last_comm
        
        input: 
        df_feature - df.column combo, for example  
        '''
        
        if df_feature.isna().sum():
            print('WARNING: {} NANs present'.format(df_feature.isna().sum()))        
        return        
    
    def test_nan_presence_in_event_array(self,event_array,word2idx):
        '''
        suprious 'nan' values in the event_array has been causing the exceptionally large values in our training set (that exceed the LSTM in_dims param)
        
        This test checks if they are are problems
        '''     
        if np.amax(event_array) > len(word2idx):
            print('NAN value in event array ERROR')
        return
    
    def test_event_categories_increase_error(self,word2idx,event):
        '''
        This unit test will return a warning if the event categories change, due to new categories being introduced that weren't known about at the time of model training. 
        
        This has been known to cause the scoring process to fail, and requires model retraining to solve
        '''
        
        #Load 
        archival_word2idx = pd.read_pickle('./model_objects/wordidx/mfmodel_word2idx_{}_global.pkl'.format(event)) 
        
        if len(word2idx) != len(archival_word2idx):
            print('WARNING: {} categories have changed, this may cause the model to fail and will require retraining')
        return 
        
        
    def run_scoring_job(self,brand,channel,event,scoring_table_name,pre_trained_model,score_date,b_factor=0.5):
        '''
        Run all scoring operations from start to finish
        
        WARNING: the estimated run time here is around 7 hours
        
        inputs:
        brand - 'String' of the form 'EE'
        channel - 'tuple' of the form ('S','M')
        event - 'string' of the form 'business_objective'
        scoring_table_name - 'string' name for the scoring base created in the process of the form 'atc_mfmodel_scoring_base'
        pre_trained_model - 'string' identifier for a pre-trained model of the form 'BO_2020H2_old'
        b_factor - 'float' value to provide business weighting for threshold value (typically between 0 and 1, default 0.5)
        score_date - 'fixed scoring date' for testing
        
        outputs:
        df_final - 'df' containing simulated proba's over 30 days per customer
        df_fatigue - 'df' df_final with calculated fatigue windows joined based on the fatigue threshold
        df_eligibility - 'df' deliverable dataset with eligiblity calculate wrt to a sate (should be today's date)
        '''
        
        score_dateft = datetime.datetime.strptime(score_date, "%Y-%m-%d").strftime("%Y%m%d")
        
        #Load SQL Querry class - run scoring object - est 1min
        print('STAGE 1 of 9: SQL queries')
        MarketingFatigue_SQL(brand,channel,event,scoring_table_name).generate_scoring_base(score_date)
        
        #Convert the scoring table into a nested list of python processing and save as a pickle - est 4hours
        print('STAGE 2 of 9: process sqeuences')
        my_output_list,my_time_list= MarketingFatigue_sequence_prep(self.base,scoring_table_name,event).run_marketing_fatigue_prep()
        
        #Load prepared pickle data structures and diseminate as individual arrays - est 3mins
        print('STAGE 3 of 9: load sequences into arrays')        
        event_array,response_array,date_array,customers_ids_list = self.load_prepared_data_as_array()
        
        #Create cleaned test and trained sets based on our global word2idx - est 40mins
        print('STAGE 4 of 9: build global word2idx dictionary')  
        word2idx = self.generate_global_word2idx_dict(event)
        print('STAGE 5 of 9: prepare scoring data')          
        X_null, X_score, Y_null, Y_score,id_null,id_score,date_null,date_score = self.prepare_train_test_as_array(event_array,
                                                                                                             response_array,
                                                                                                             customers_ids_list,
                                                                                                             date_array,
                                                                                                             test_proportion=1,
                                                                                                             global_event=event)
        
        # Score based on a pre trained model - est 2 hrs 30mins
        print('STAGE 6 of 9: score and simulate based on pre trained model')          
        df_final = self.score_fatigue_proba(X_score,Y_score,id_score,date_score,pre_trained_model=pre_trained_model)
        df_final.to_pickle("./data/processed/BO_scored.pkl")
       
        #Simulate output propensity and calculate fatigue window and eligibility - est 5mins
        print('STAGE 7 of 9: Calculate fatigue windows')          
        print('Business factor  = {}'.format(b_factor))
        df_fatigue,fatigue_th = self.marketing_fatigue_window(df_final,df_final['customer_id'].tolist(),factor=b_factor)
        df_fatigue.to_pickle("./data/processed/BO_score_fatigue.pkl")        
        
        print('STAGE 8 of 9: prepare output dataset')  
        df_eligibility = self.eligibility(df_fatigue,fixed_date=score_date)
        df_eligibility.to_pickle(f"./data/processed/BO_score_eligibility_{score_dateft}.pkl")     
        
        print('STAGE 9 of 9: save scored data to athena')  
        self.save_df_eligibility_to_athena(df_eligibility,score_date)
        
        df_eligibility_gp = df_eligibility.groupby('eligibility_flg').agg(customer_cnt=('customer_id', 'count'),
                                                       avg_days_since_last_comm=('days_since_last_comm', 'mean'),
                                                       avg_fatigue_days=('fatigue_days', 'mean')).reset_index()
        
        print(df_eligibility_gp)
     
        return df_final,df_fatigue,df_eligibility
    
    def run_training_job(self,brand,channel,event,scoring_table_name,pre_trained_model,b_factor=0.5):
        '''
        Run all scoring operations from start to finish
        
        WARNING: the estimated run time here is around 7 hours
        
        inputs:
        brand - 'String' of the form 'EE'
        channel - 'tuple' of the form ('S','M')
        event - 'string' of the form 'business_objective'
        training_table_name - 'string' name for the scoring base created in the process of the form 'atc_mfmodel_scoring_base'
        date_min/date_max - 'string' expressions for the min and max date in 'yyyy-mm-dd' format
        '''
        
        #Load SQL Querry class - run scoring object - est 1min
        mfsql = MarketingFatigue_SQL(brand,channel,event,training_table_name)
        
        positive_sample = 90 #%
        negative_sample = 90 #%

        #Run SQL queries to derive data - est 5mins
        mfsql.atc_ee_optout_customers_model(date_min,date_max)
        mfsql.atc_ee_not_optout_customers_model(date_min,date_max)
        mfsql.date_add_and_union(positive_sample,negative_sample)
        mfsql.generate_final_base()

        #Convert the scoring table into a nested list of python processing and save as a pickle - est 5hours
        my_output_list,my_time_list= MarketingFatigue_sequence_prep(self.base,training_table_name,event).run_marketing_fatigue_prep()
        
        #Load prepared pickle data structures and diseminate as individual arrays - est 3 mins
        event_array,response_array,date_array,customers_ids_list = self.load_prepared_data_as_array()
        
        #Create cleaned test and trained sets based on our global word2idx - est 1 hour
        word2idx = self.generate_global_word2idx_dict(event)
        
        X_null, X_score, Y_null, Y_score,id_null,id_score,date_null,date_score = self.prepare_train_test_as_array(event_array,
                                                                                                             response_array,
                                                                                                             customers_ids_list,
                                                                                                             date_array,
                                                                                                             test_proportion=4,
                                                                                                             global_event=event)
        
        #MODEL TRAINING - depends on sample size - est 5 hours at max
        training_sample = 10000000

        X_train_lstm = X_train[:training_sample]
        X_test_lstm = X_test[:training_sample]
        Y_train_lstm = Y_train[:training_sample]
        Y_test_lstm = Y_test[:training_sample]

        mfmodel.train_lstm_model(X_train_lstm,Y_train_lstm,X_test_lstm,Y_test_lstm,epochs=6,embedding_size=10,global_event=event)        
        
        #Evaluate mode performance using cummaltive gains
        eval_sample = 100000

        X_test_sml = X_test[:eval_sample]
        Y_test_sml = Y_test[:eval_sample]
        id_test_sml = id_test[:eval_sample]
        date_test_sml = date_test[:eval_sample]

        mfmodel.evaluate_model(X_test_sml,Y_test_sml)
        
        #
        df_final = mfmodel.score_fatigue_proba(X_test_sml,Y_test_sml,id_test_sml,date_test_sml)
        
        return 
    

    def marketing_fatigue_LSTM_model(self,num_words,max_length,batch_size,embedding_size,learning_rate):
        '''
        This object contain the model architecture. 
        
        Update and change network to explore model performance
        
        Default model params are deffine by updated values can be passed when the object is called
        '''

        # Remember to run the step before re-ruuning the model.
        # This step clears Keras backend and also del the model object

        keras.backend.clear_session()

        try:
            del model
        except NameError:
            var_exists = False    

        ##################
        #TRAIN LSTM MODEL#
        ##################

        #Deffine our model object as sequential (for LSTM)
        model = Sequential()

        #Deffine our Embedding layer - Turns positive integers (indexes) into dense vectors of fixed size.
        model.add(Embedding(num_words,embedding_size,input_length=max_length))

        #Deffine our LSTM layer
        # model.add(LSTM(max_length,dropout=0.1,return_sequences=True))
        model.add(LSTM(max_length,dropout=0.1))
        #Output of embeding layer3D tensor with shape: (batch_size, input_length, output_dim).

        # model.add(LSTM(32,dropout=0.1))
        # model.add(Dense(16,activation="relu"))
        model.add(Dense(1,activation="sigmoid"))
        #Considering using model.add(Bidirectional(LSTM(64)))This helps the LSTM to learn long range dependencies.

        #Compile the model 
        optimizer = Adam(learning_rate=learning_rate) 
        model.compile(loss="binary_crossentropy",optimizer=optimizer,metrics=["accuracy"])

        model.summary()

        return model

    def shuffle(self,matrix, target,customer_id,dates, test_proportion):
        '''
        prepare train and test set. 
        
        Adjust test proportion to change up sample size
                
        '''
        ratio = int(matrix.shape[0]/test_proportion) #should be int
        X_train = matrix[ratio:,:]
        X_test =  matrix[:ratio,:]
        Y_train = target[ratio:]
        Y_test =  target[:ratio:]
        id_train = customer_id[ratio:]
        id_test = customer_id[:ratio:]
        date_train = dates[ratio:]
        date_test = dates[:ratio:]    
        return X_train, X_test, Y_train, Y_test,id_train,id_test,date_train,date_test

    def calculate_cumm_metrics(self,y_pred,y_true):
        '''
        Calculate cummalative metrics, over a series of deciles

        y_pred - array of predicted probas e.g
        bst.predict(validation_matrix)
        df_final.prediction

        y_true - array of true outcomes (classifed) 
    
        '''
        deciles = []
        recalls = []
        precisions = []
        thresholds = []
        for n in tqdm(range(1,11)):

            deciles.append(n)

            threshold = np.percentile(y_pred, np.arange(0,100,10))[-n] 

            thresholds.append(threshold)

            predictions = []
            for value in y_pred:
                if value >= threshold:
                    predictions.append(1.0)
                else:
                    predictions.append(0.0)

            recalls.append(recall_score(y_true, predictions))
            precisions.append(precision_score(y_true, predictions))

        return deciles, recalls, precisions, thresholds
    
    def load_prepared_data_as_array(self):
        '''
        IMPORTANT - requires ETL process to be run in advance
        
        Load the prepared data from Pickle. 
        
        Unpack the object and save to respective arrays/lists       
        '''
        
        #%%time
        with open('./data/processed/mfmodel_prepdata_{}.pkl'.format(self.base), 'rb') as f:
            all_data = pickle.load(f)
            
        event_list = []
        response_list = []
        customers_ids_list = []
        date_list = []        

        #Unpack the prepared data to verify its format and convert into Array's
        for p in range(len(all_data)):
            length_lists = []

            for i in range(len(all_data[p])):
                length_lists.append(len(all_data[p][i][0]))

            for i,k in zip(list(range(len(all_data[p]))),length_lists):
                for j in range(k):
                    #Clean any instances of 'nan' values in the input data
                    events = all_data[p][i][0][j]        
                    events = [0 if x != x else x for x in events]
                    event_list.append(events)
                    response_list.append(all_data[p][i][1][j])
                    customers_ids_list.append(all_data[p][i][2][j])
                    date_list.append(all_data[p][i][3][j])

        event_array = np.array(event_list)
        response_array = np.array(response_list)            
        response_array = np.where(response_array==0, 0, 1)
        date_array = np.array(date_list)        
        
        return event_array,response_array,date_array,customers_ids_list
        
    def generate_new_word2idx_dict(self,event_array):
        '''
        OBSOLUTE???
        
        For a given event array, examine the categorical elements and write these to a data dictionary
        
        Save these to Pickle should these be required for reference
        
        a word3idx dict is unique for a specific base
        '''
        #Generate the 'word2idx' dictionary of all the categories in 'business_objective'
        i = 0
        word2idx = {}
        for sentence in tqdm(event_array):
            for token in sentence:
                if token not in word2idx:
                    word2idx[token] = i         
                    i += 1

        #We have keys that are numbers so arifically adding 1000 avoids any clashes. This is subtracted off later
        for key, value in word2idx.items():
            word2idx[key]=value+1000

        with open('./model_objects/wordidx/mfmodel_word2idx_{}.pkl'.format(self.base), 'wb') as f:
            pickle.dump(word2idx, f)        
        
        return word2idx
    
    def generate_global_word2idx_dict(self,event):
        '''
        For the global list of event categories, examine the categorical elements and write these to a data dictionary
        
        Save these to Pickle should these be required for reference
        
        This word2idx is generic and can be used across all bases
        
        Input 
        event - string for a specific event category (e.g. business_objective)
        '''
    
        # Load a pre-deffined 'global' dataset of all available data
        sql_code = '''select distinct business_objective from campaign_data.atc_sccv_ids '''
        df = pd.read_sql(sql_code, self.conn)
        
        word2idx = {}
        
        #Set our first entry equal to the null send option of '0'
        word2idx['0']=0

        #Build our dictionary (increment + 1)
        for i in np.arange(len(df[event])):
            word2idx[df[event][i]]=i+1
            
        #We have keys that are numbers so arifically adding 1000 avoids any clashes. This is subtracted off later
        for key, value in word2idx.items():
            word2idx[key]=value+1000          
            
        print('length of word2idx = {}'.format(len(word2idx))) 
        
        #UNIT TESTING
        self.test_event_categories_increase_error(word2idx,event)        
            
        #save as the 'global' word2idx
        with open('./model_objects/wordidx/mfmodel_word2idx_{}_global.pkl'.format(event), 'wb') as f:
            pickle.dump(word2idx, f)     
        
        return word2idx    
   
    def prepare_train_test_as_array(self,event_array,response_array,customers_ids_list, date_array,test_proportion=4, **kwargs):
        '''
        Process our prepared datasets into train and test
        
        You can change the proportion with the 'test_proportion' argument - set this to 1 for scoring (TBC) and use the test set
        
        Default propotion size is 4 (adjust this when we want to score the data)
        
        load the relevant word2idx either via:
        - enter the required variables and the word2idx will be created off of the presaved dictionary of your 'base_id'
        - enter "word2idx_base='base_id'" to load a different word2idx dict. Use this when preparing your scoring data for a pre-trained model
        - enter "global_event=event" where event is the same used in 'generate_global_word2idx_dict(event)' (eg. business_objective). This will use the global word2idx
        
        and apply it to our event array to convert to a numerical format
        
        Returns test & train arrays for X,Y,ids and dates        
        '''
        
        if ('word2idx_base' in kwargs):
            word2idx_base = kwargs['word2idx_base']
            print('SCORING - update event array on the {} word2idx'.format(word2idx_base))
        elif ('global_event' in kwargs):
            word2idx_base = '{}_global'.format(kwargs['global_event'])
            print('GLOBAL - update event array on the {} word2idx'.format(word2idx_base))  
        else:
            word2idx_base = self.base
            print('TRAINING - update event array on the {} word2idx'.format(word2idx_base))  
        
        #%%time
        with open('./model_objects/wordidx/mfmodel_word2idx_{}.pkl'.format(word2idx_base), 'rb') as f:
            word2idx = pickle.load(f)

        #Use our word2idx dictionary to replace strings as int
        for key, value in tqdm(word2idx.items()):
                event_array[event_array ==key] = value

        event_array = event_array.astype(np.int)
        event_array = event_array-1000 #remove arbitary 1000 from the value
        
        self.test_nan_presence_in_event_array(event_array,word2idx)

        #prepare train and test
        X_train, X_test, Y_train, Y_test,id_train,id_test,date_train,date_test = self.shuffle(event_array, response_array, customers_ids_list, date_array, test_proportion)
        
        return X_train, X_test, Y_train, Y_test,id_train,id_test,date_train,date_test
    
    def train_lstm_model(self,X_train,Y_train,X_test,Y_test,epochs=6,max_length=60,batch_size=64,embedding_size=10,learning_rate=3e-4, **kwargs):
        '''
        Train the model. WARNING - this step is time consuming
        
        Load the word2ids - this is going to be the basis of the num_words parameter
        
        The remaining parameters have preloaded values but can be altered to experiment with model design        
        '''
        
        if ('global_event' in kwargs):
            word2idx_base = '{}_global'.format(kwargs['global_event'])
            print('Using GLOBAL word2idx - {}'.format(word2idx_base))  
        else:
            word2idx_base = self.base
            print('Using BASE word2idx {}'.format(word2idx_base))          
        
        #%%time
        with open('./model_objetcs/wordidx/mfmodel_word2idx_{}.pkl'.format(word2idx_base), 'rb') as f:
            word2idx = pickle.load(f)        
        
        #LSTM params 
        #===========#
        num_words = len(word2idx)+1
        
        print('num_words = {}'.format(num_words))
        
        #prepare our model architecture and save it as a model object 
        model = self.marketing_fatigue_LSTM_model(num_words,max_length,batch_size,embedding_size,learning_rate)
        
        #Fit the model
        history=model.fit(X_train,Y_train,epochs,validation_data=(X_test,Y_test))
        
        # creates a HDF5 file 'mfmodel_lstm_.h5'
        model.save('./model_objects/models/mfmodel_lstm_{}.h5'.format(self.base))  
        
        # deletes the existing model        
        del model
        
        return
        
    def evaluate_model(self,X_test,Y_test):
        '''
        Load our pre trained model using our base_id
        
        Score the model on the testing data (X_test) and compare to the actuals (Y_test)
        
        Evaluation methods:
        
        Classification report - quickly crunch classification metrics
        
        Confusion matrix - The sum of TP,FP,TN,FN
        
        Cummalative gains - use this function to calculate cummulative recall and precisions for each decile. Produces a plot
        
        MODEL ARCHIVE - Recall of positive class @ 1st decile
        =============
        mfmodel_lstm_jan2021   -- 33%
        mfmodel_lstm_BD_jan2021   -- 31%
        mfmodel_lstm_BO_2020H2 -- 35%
         
        '''
        #%%time
        
        model = keras.models.load_model('./model_objects/models/mfmodel_lstm_{}.h5'.format(self.base))

        #Generate predictions on the test set
        predictions = model.predict(X_test, verbose=0, batch_size=64)
        
        #Cumlative gains plot
        deciles, recalls, precisions, thresholds = self.calculate_cumm_metrics(predictions,Y_test)
        
        #Classification metrics (thresholded at Recall@1st decile)
        print('threshold for recall in 1st decile = {:.1f}%'.format(100*thresholds[0]))

        predictions_flags = np.where(predictions>thresholds[0], 1, 0)
        predictions_flags_reshaped = predictions_flags.reshape((-1,))

        print(classification_report(Y_test, predictions_flags_reshaped))

        print(confusion_matrix(Y_test, predictions_flags_reshaped))

        fig, ax = plt.subplots(figsize=(10,5))
        plt.plot(deciles, [round(x*100,1) for x in recalls], marker='o', c='Indigo', markersize=15, linewidth=3)
        plt.xlabel('Decile', fontsize=15)
        plt.ylabel('Fraction of total conversions recovered [%]', fontsize=15)
        plt.xticks(np.arange(1, 10+1, 1), fontsize=15)
        plt.yticks(np.arange(10, 100+1, 10), fontsize=15)
        plt.grid()
        plt.show()
        
        #Calculate KS statistic
        predictions = predictions.reshape((-1,))
            
        predictions_1 = []
        predictions_0 = []

        for i in np.arange(len(Y_test_sml)):
            if Y_test[i] == 0:
                predictions_0.append(predictions[i])
            elif Y_test[i] == 1:
                predictions_1.append(predictions[i])

        KS = stats.ks_2samp(predictions_0, predictions_1)
        
        print('KS Stat = {}'.format(KS[0]))
        print('Probability that target classes are drawn from the population = {}'.format(KS[1]))
        
        plt.hist(predictions_0,bins=50,range=(0,0.1),alpha=0.5,density=1,color = 'b', label='Does not opt out')
        plt.hist(predictions_1,bins=50,range=(0,0.1),alpha=0.5,density=1,color='r',label='Does opt out')
        plt.legend()
        plt.xlabel('Predicted probability')
        plt.xlabel('count')        
        plt.title('KS Statistic = {}'.format(round(KS[0],2)))
        plt.show()        
        
        return KS[0]
    
    def score_fatigue_proba(self,X_test,Y_test,id_test,date_test, **kwargs):
        '''
        Write our predictions to a dataframe (df_final)
        
        load the relevant model for scoring:
        - be default, the model associated with the named base_id will run
        - by adding the argument "pre_trained_model='base_id'" for a different run you can score on a different model (used for scoring)
        
        IMPORTANT - you should score on the same model you prepared your data with the word2idx
        
        Roll those predicitions forward by 30 days
        
        On each predictions we assume that the customer was NOT sent any comm and record thier new proba when that new sequence is scored
        
        Note that the '0' event for 'no send' is at index 1 in the word2idx - see function f(x)
        '''
        
        if ('pre_trained_model' in kwargs):
            model_base = kwargs['pre_trained_model']
            print('SCORE the dataset on the {} model'.format(model_base))
        else:
            model_base = self.base
        
        model = keras.models.load_model('./model_objects/models/mfmodel_lstm_{}.h5'.format(model_base))
                            
        #Deffine dataframe with final predictions
        df_final = pd.DataFrame(columns = ['opt_out_flag'])
        df_final['opt_out_flag']= Y_test
        df_final["customer_id"] = id_test
        df_final["date_of_delivery"] = date_test
        df_final['prediction_date_1'] = model.predict(X_test, verbose=0, batch_size=1280)  
        
        def f(x):
            #1 is the no comunication if i remember correclty on the cause in teh combined array the first element that we print out has Prospects as value the second value is 0 so in word to 2index 1 index is 0 in other training dataset you will have to change that to use the equivlent index for the "0" which is not receiveing comunication
            # Append a 'null' event to each day that is predicted forward into the future. The Null event is the key as value '0' in word2idx
            return np.append(x[1:], 0)

        def array_map(x):
            return np.array(list(map(f, x)))

        #Extrapolate 30 days into the future, append the key value for '0' to array for each day (in this case 1)
        X_test_new = copy.deepcopy(X_test)
        for i in tqdm(range(1,30)):
            X_test_new = array_map(X_test_new)
            valid_y_pred = model.predict(X_test_new, verbose=0, batch_size=1280)
            df_final['prediction_date_{}'.format(i+1)]=valid_y_pred 
        
        return df_final
    
    
    
    def marketing_fatigue_window(self,df_final,id_test,factor=0.5):
        '''
        Deffines a new data frame to capature the customer fatigue window
        
        This is deffined as the number of days into the future after which thier proba of opting out drops to nominal levels
        
        nominal levels are deffined as the 'average+(std*factor)'
        
        The default factor is given as 0.5. The higher the factor, the shorter the customer windows and the more comms can be sent
        
        The customer's fatigue window is returned as an integer number of days between 0 and 30. 
        Where the customer's window is greater than 30, a value of 99 is placed (though all customers should become available after 30 days)
        
        The the fatigue window values are attached to the original proba's for exploration         
        '''
        #%%time

        #Deffine our fatigue specific df
        df_fatigue = pd.DataFrame(columns = ["fatigue_days"])
        df_fatigue['customer_id']= id_test        
        
        #Deffine our threshold values (based on the mean optout proba)
        avg_all_optout= pd.DataFrame(df_final[df_final.columns[3:]].head(100000).mean(axis=0),columns = ['avg_prob_all']).head()
        std_all_optout= pd.DataFrame(df_final[df_final.columns[3:]].head(100000).std(axis=0),columns = ['std_prob_all']).head()
        
        #Calculate our fatigue threshold - see intro       
        fatigue_th =avg_all_optout.mean()[0]+(std_all_optout.mean()[0]*factor)
        print('Threshold at optout proba={:.4f}'.format(fatigue_th))

        ##Mask over our proba for a given condition (threshold) and return the date of the first instance as a new column 
        df_batch = df_final[df_final.columns[3:34]]
        mask_df = df_batch.le(fatigue_th)
        mask_df["prediction_date_30"] = True
        df_fatigue["fatigue_days"] = mask_df.idxmax(axis=1)   

        #Cleaning
        df_fatigue['fatigue_days'] = df_fatigue['fatigue_days'].str.replace('prediction_date_','')
        df_fatigue['fatigue_days'] = df_fatigue['fatigue_days'].str.replace('predictions_date_','')       
        # Where the fatigue is greater than 30days, NA will be returned. Replace this wtih 99
        df_fatigue['fatigue_days'].fillna('99',inplace=True)         
        df_fatigue['fatigue_days'] = df_fatigue['fatigue_days'].astype(int)
        
        #Calculate faitgue segment
        df_fatigue['fatigue_segment'] = 'Unknown'

        df_fatigue.loc[(df_fatigue['fatigue_days']>=30),'fatigue_segment'] = '4.max'
        df_fatigue.loc[(df_fatigue['fatigue_days']>2) & (df_fatigue['fatigue_days']<30),'fatigue_segment'] = '3.high'
        df_fatigue.loc[(df_fatigue['fatigue_days']>1) & (df_fatigue['fatigue_days']<=2),'fatigue_segment'] = '2.low'
        df_fatigue.loc[(df_fatigue['fatigue_days']==1),'fatigue_segment'] = '1.none'   
        

        df_final.columns = ['opt_out_flag','customer_id','date_of_delivery',
                              '1','2','3','4','5','6','7',
                              '8','9','10','11','12','13',
                              '14','15','16','17','18','19',
                              '20','21','22','23','24','25',
                              '26','27','28','29','30']

        #Join to our df_final dataset
        df_fatigue = df_fatigue.join(df_final[['opt_out_flag','date_of_delivery',
                                                  '1','2','3','4','5','6','7',
                                                  '8','9','10','11','12','13',
                                                  '14','15','16','17','18','19',
                                                  '20','21','22','23','24','25',
                                                  '26','27','28','29','30']], 
                                     lsuffix='customer_id', rsuffix='customer_id')        
          
        return df_fatigue,fatigue_th
    
    def eligibility(self,df_fatigue, **kwargs):
        '''
        This function contains some trivial transformations allowing for formation of the scored eligibility dataset
        
        input:
        df_fatigue - data_frame produced by the 'marketing_fatigue_window' function
        
        output:
        
        '''
        #Note that maximum date in the scoring set relative to today's date will make a big impact. 
        #Scoring old data will results in most customers recieving eligible outcomes
        #print("MAXIMUM date scored = {} wrt today's date = {}".format(df_fatigue['date_of_delivery'].max(), pd.Timestamp('today').strftime("%Y-%m-%d")))
        
        # Calculate the 'days since last comm' and convert it and the fatigue window into a suitable format
        if ('fixed_date' in kwargs):
            df_fatigue['score_date'] =  pd.to_datetime(kwargs['fixed_date'], infer_datetime_format=True) 
            df_fatigue['days_since_last_comm'] = (pd.to_datetime(kwargs['fixed_date'], infer_datetime_format=True) - df_fatigue['date_of_delivery'])/np.timedelta64(1, 'D')
            print('SCORE the dataset on {}'.format(kwargs['fixed_date']))
        else:
            df_fatigue['score_date'] =  pd.to_datetime(pd.Timestamp('today').strftime("%Y-%m-%d"), infer_datetime_format=True)   
            df_fatigue['days_since_last_comm'] = (pd.to_datetime(pd.Timestamp('today').strftime("%Y-%m-%d"), infer_datetime_format=True) - df_fatigue['date_of_delivery'])/np.timedelta64(1, 'D')

        df_fatigue['fatigue_days'] = df_fatigue['fatigue_days'].astype(float)
        df_fatigue['optout_proba'] = df_fatigue['1']

        def eligibility(x):
            # eligibility criteria
            if x['fatigue_days']-x['days_since_last_comm']<=0:
                out = 1
            else:
                out = 0
            return out

        #calculate eligibility
        df_fatigue['eligibility_flg'] = df_fatigue.apply(eligibility,axis=1)

        #deffine customer eligibility
        df_eligibility = pd.DataFrame(columns = ["customer_id"])                       
        df_eligibility = df_fatigue[['customer_id','optout_proba','eligibility_flg',
                                     'fatigue_days','fatigue_segment','days_since_last_comm',
                                     'date_of_delivery','score_date']]
        #Unit testing
        self.test_nans_present(df_eligibility.days_since_last_comm)
        
        return df_eligibility
    
    
    def save_df_eligibility_to_athena(self,df_eligibility,score_date):
        '''
        
        '''
        score_dateft = datetime.datetime.strptime(score_date, "%Y-%m-%d").strftime("%Y%m%d")
        
        df_eligibility['date_of_delivery'] = df_eligibility['date_of_delivery'].astype(str)
        df_eligibility['score_date'] = df_eligibility['score_date'].astype(str)

        df_eligibility = df_eligibility[['customer_id','optout_proba','eligibility_flg',
                                         'fatigue_days','fatigue_segment','days_since_last_comm',
                                         'date_of_delivery','score_date'
                                        ]]        

        # now specify the s3 location to save the magpie data
        s3_location = 's3://bt-data-science-playground/marketing-fatigue/data/score_dt_{}'.format(score_dateft)
        athena_output_table_name=f'djr_atc_marketing_fatigue_eligilibity_{score_dateft}'
        df_eligibility.to_parquet(f'{s3_location}/atc_marketing_fatigue_eligilibity_{score_dateft}')
        
        sql_query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS bt_home_ds.{athena_output_table_name} (
            `customer_id` varchar(18)
            , `optout_proba` float
            , `eligibility_flg` bigint
            , `fatigue_days` double
            , `fatigue_segment` varchar(18)
            , `days_since_last_comm` double
            , `date_of_delivery` string
            , `score_date` string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
          'serialization.format' = '1'
        ) LOCATION '{s3_location}/'
        TBLPROPERTIES ('has_encrypted_data'='false');
        '''

        pd.read_sql(f'drop table bt_home_ds.{athena_output_table_name}', self.conn)
        pd.read_sql(sql_query, self.conn)   
        
        print(f'wrote output to {athena_output_table_name}') 
        return
        
class MarketingFatigue_SQL:
    '''
    This class represents the various SQL querries for deriving the Marketing Fatigue Base
    
    
    '''
    def __init__(self,brand,channel,event,athena_table_name):
        '''
        
        Initate with a number of global variables
        
        Input:
        brand - 'string' or 'tuple' of the form 'EE' !!!CAPS!!!
        channel - 'tuple' of the form ('S', 'M') !!!CAPS!!!
        
        event - 'string' matching a metadata field, for example 'business_objective'
        base_id - 'string' user deffined identifier for the base that will be associated with this run
        table_name - 'string' the name of the athena table coresponding to the base_id

        '''
        self.event = event
        self.table_name = athena_table_name        
        
        self.brand = brand
        self.channel = channel
        
        self.conn = connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                   region_name='eu-west-2') 
        
        return    

    def atc_ee_optout_customers_model(self,date_min,date_max):
        '''        
        Run Athena Querries
        '''

        pd.read_sql('''drop table if exists campaign_data.atc_ee_optout_customers_model;''', self.conn)

        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_ee_optout_customers_model as 

        select 
            -- Build joining dates
            *
            ,DATE_ADD('day',-6,cast(date_format(optout_date,'%Y-%m-%d') as date)) as day_1_week_ago -- CLM
            ,format_datetime(date_add('month',-1,cast(date_format(optout_date,'%Y-%m-%d') as date) ),'yMM') as date_month -- AGG
        from (
            -- Identify opt out base
            select 
                ee_customer_id,
                max(optout_flg) as optout_flag,
                max(click_flg) as click_flag,           
                sum(optout_flg) as optout_cnt,   
                min(date_of_delivery) as optout_date 

            from campaign_data.atc_sccv_ids

            where 
            brand= upper('{}')
            and channel in {}
            and control_grp_flg = 'N'
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  < date_parse('{}','%Y-%m-%d')
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  > date_parse('{}','%Y-%m-%d')  

            and optout_flg=1 

            group by ee_customer_id having sum(optout_flg)>0
            ) A
        '''.format(self.brand,self.channel,date_max,date_min)

        pd.read_sql(sql_code, self.conn)

        #QA
        df = pd.read_sql('''select count(*),max(optout_flag) from campaign_data.atc_ee_optout_customers_model;''', self.conn)
        df.head()        
        
        return
    
    def atc_ee_not_optout_customers_model(self,date_min,date_max):
        '''        
        Run Athena Querries        
        '''

        pd.read_sql('''drop table if exists campaign_data.atc_ee_not_optout_customers_model;''', self.conn)

        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_ee_not_optout_customers_model as 

        select 
            -- Build joining dates
            * 
            ,DATE_ADD('day',-6,cast(date_format(optout_date,'%Y-%m-%d') as date)) as day_1_week_ago
            ,format_datetime(date_add('month',-1,cast(date_format(optout_date,'%Y-%m-%d') as date) ),'yMM') as date_month
        from ( 
            -- sample non-opt out base    
            select 
                ee_customer_id,
                max(optout_flg) as optout_flag,
                max(click_flg) as click_flag,        
                sum(optout_flg) as optout_cnt,   
                max(date_of_delivery) as optout_date

            from campaign_data.atc_sccv_ids 

            where brand=upper('{}')
            and channel in {}
            and control_grp_flg = 'N'
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  < date_parse('{}','%Y-%m-%d')
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  > date_parse('{}','%Y-%m-%d')  

            group by ee_customer_id having sum(optout_flg) is null
        ) A
        '''.format(self.brand,self.channel,date_max,date_min)

        pd.read_sql(sql_code, self.conn)

        #QA
        df = pd.read_sql('''select count(*),max(optout_flag) from campaign_data.atc_ee_not_optout_customers_model;''', self.conn)
        df.head()        
        
        return  
    
    def date_add_and_union(self,positive_training_sample,negative_training_sample):
        '''
        Run Athena Querries        
        '''
        #Clean down existing tables
        pd.read_sql('''drop table if exists campaign_data.atc_ee_optout_customers_60dys;''', self.conn)

        # Extend the opt out and not opt out tables by 60 days
        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_ee_optout_customers_60dys as

        select *,DATE_ADD('day',-60,cast(date_format(optout_date,'%Y-%m-%d') as date)) as starting_date
        from campaign_data.atc_ee_optout_customers_model
        '''
        pd.read_sql(sql_code, self.conn)
        
        pd.read_sql('''drop table if exists campaign_data.atc_ee_not_optout_customers_60dys;''', self.conn)          

        # Extend the opt out and not opt out tables by 60 days
        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_ee_not_optout_customers_60dys as

        select *, DATE_ADD('day',-60,cast(date_format(optout_date,'%Y-%m-%d') as date)) as starting_date
        from campaign_data.atc_ee_not_optout_customers_model;
        '''
        pd.read_sql(sql_code, self.conn)
        
        pd.read_sql('''drop table if exists campaign_data.atc_mfmodel_base;''', self.conn)        

        # Union the opt out and not opt out tables together - contains any replancing
        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_mfmodel_base as 

        select * from (
          select * from campaign_data.atc_ee_not_optout_customers_60dys
          )TABLESAMPLE BERNOULLI({})
        UNION ALL 
        select * from (
          select * from campaign_data.atc_ee_optout_customers_60dys
          )TABLESAMPLE BERNOULLI({})
          '''.format(positive_training_sample,negative_training_sample)
        pd.read_sql(sql_code, self.conn)

        pd.read_sql('select count(*),count(distinct ee_customer_id) from campaign_data.atc_mfmodel_base;', self.conn)                
        return
    
    
    def generate_final_base(self):
        '''
        Run Athena Querries 
        '''
        
        pd.read_sql('''drop table if exists  campaign_data.{};'''.format(self.table_name), self.conn)

        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.{} as

        select 
            a.ee_customer_id
            ,a.date_of_delivery
            ,a.{} -- event by default this will be 'business_objective'
            ,b.optout_flag
            ,b.optout_date
        from campaign_data.atc_sccv_ids a 
        inner join campaign_data.atc_mfmodel_base b
            on a.ee_customer_id = b.ee_customer_id
        where a.date_of_delivery between b.starting_date and b.optout_date 
        and b.ee_customer_id <> '0'
        order by ee_customer_id,date_of_delivery
        '''.format(self.table_name,self.event)
        pd.read_sql(sql_code, self.conn)
  
        #QA
        df_cnt = pd.read_sql('''select count(*) as cnt from campaign_data.{};'''.format(self.table_name), self.conn)
        df_mxdt = pd.read_sql('''select max(date_of_delivery) as date from campaign_data.{};'''.format(self.table_name), self.conn)

        print('athena table length = {}'.format(df_cnt['cnt'][0]))
        print('most recent entry = {}'.format(df_mxdt['date'][0]))        
        
        return
        
    def generate_scoring_base(self,score_date):
        '''
        Run Athena Querries
        '''
        
        pd.read_sql('''drop table if exists campaign_data.atc_mfmodel_scoring_base''', self.conn)

        sql_code = '''
        CREATE TABLE IF NOT EXISTS campaign_data.atc_mfmodel_scoring_base as

        with scoring_base as (

            select  
                 ee_customer_id
                ,date_add('day',-60,current_date) as dt60
                ,date_of_delivery
                ,row_number() OVER(PARTITION BY ee_customer_id ORDER BY date_of_delivery desc) AS RN 
            from campaign_data.atc_sccv_ids 

            where brand=upper('{}')
            and channel in {}
            and control_grp_flg = 'N'
            and ee_customer_id <> '0' -- and ee_customer_id = '1000003220'

            -- Score only those who have received anything in the previous 60 days 
            -- and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  >= date_add('day',-60,current_date) 
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  >= date_add('day',-60,date_parse('{}','%Y-%m-%d')) 
            and cast(date_format(date_of_delivery,'%Y-%m-%d') as date) <= date_parse('{}','%Y-%m-%d')
            )

        select
            c.ee_customer_id
            ,c.{}
            ,c.date_of_delivery
            ,c.prev_date_of_delivery
            ,date_diff('day',c.prev_date_of_delivery,c.date_of_delivery) As days_from_last_comm 
            ,date_diff('day',c.date_of_delivery,c.most_recent_date_of_delivery) As days_from_opout  
            ,c.optout_flag
            ,c.most_recent_date_of_delivery as optout_date
            -- ,c.most_recent_date_of_delivery

        from (
                select 
                    a.ee_customer_id
                    ,a.date_of_delivery 
                    ,cast(
                      substring(
                        cast(
                      case when lag(a.date_of_delivery, 1) OVER(PARTITION BY a.ee_customer_id ORDER BY a.date_of_delivery asc) is null then a.date_of_delivery else lag(a.date_of_delivery, 1) OVER(PARTITION BY a.ee_customer_id ORDER BY a.date_of_delivery asc) end
                          as varchar(25))
                           ,1,10)
                            as date) AS prev_date_of_delivery      
                    ,b.date_of_delivery as most_recent_date_of_delivery
                    ,a.{}
                    ,a.optout_flg as optout_flag
                    ,a.optout_date

                from campaign_data.atc_sccv_ids a 
                -- filter by most recent activity to score
                inner join (select * from scoring_base where RN=1) b
                    on a.ee_customer_id = b.ee_customer_id
                    and a.date_of_delivery >= date_add('day',-60,b.date_of_delivery)
                    and a.date_of_delivery <= date_parse('{}','%Y-%m-%d')

                where 
                a.brand= upper('{}')
                and a.channel in {}
                and a.control_grp_flg = 'N'  

                order by ee_customer_id,date_of_delivery desc
          ) c
          '''.format(self.brand,self.channel,score_date,score_date,self.event,self.event,score_date,self.brand,self.channel)
        
        pd.read_sql(sql_code, self.conn)

        #QA
        df_cnt = pd.read_sql('''select count(*) as cnt from campaign_data.atc_mfmodel_scoring_base''', self.conn)
        df_mxdt = pd.read_sql('''select max(date_of_delivery) as date from campaign_data.atc_mfmodel_scoring_base''', self.conn)

        print('athena table length = {}'.format(df_cnt['cnt'][0]))
        print('most recent entry = {}'.format(df_mxdt['date'][0]))
        return
