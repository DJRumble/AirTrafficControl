"""
Functions present here: 
"""

import pandas as pd
from pyathena import connect

# create a connection to Athena
conn = connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                   region_name='eu-west-2') 

def generate_base(write_to_df='no'):
    print('Starting base build')
    print('Stage 1/3')
    
    #base of customers who have opted out
    sql_code = '''
    drop table if exists  campaign_data.atc_ee_optout_customers_model;
    '''
    pd.read_sql(sql_code, conn)

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
            ee_customer_id_extended as ee_customer_id,
            max(optout_flg) as optout_flag,
            max(click_flg) as click_flag,           
            sum(optout_flg) as optout_cnt,   
            min(date_of_delivery) as optout_date 
        from campaign_data.atc_sccv_ids 

        where brand='EE'
        and channel in ('S', 'M')
        and control_grp_flg = 'N'
        and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  < date_parse('2020-09-01','%Y-%m-%d')
        and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  > date_parse('2019-09-01','%Y-%m-%d')
        and optout_flg=1 

        group by ee_customer_id_extended having sum(optout_flg)>0
        ) A
    '''
    pd.read_sql(sql_code, conn)
    
    print('Stage 2/3')    

    #Base of customers who have NOT opted out
    sql_code = '''
    drop table if exists  campaign_data.atc_ee_not_optout_customers_model;
    '''
    pd.read_sql(sql_code, conn)

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
            ee_customer_id_extended as ee_customer_id,
            max(optout_flg) as optout_flag,
            max(click_flg) as click_flag,        
            sum(optout_flg) as optout_cnt,   
            DATE_ADD('day',random(365),date_parse('2019-09-01','%Y-%m-%d')) as optout_date -- 365 one year -- 240 cause is 9 months after the 20-01-01 until 2020-09-01
        from campaign_data.atc_sccv_ids 
        where brand='EE'
        and channel in ('S', 'M')
        and control_grp_flg = 'N'
        and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  < date_parse('2020-09-01','%Y-%m-%d')
        and cast(date_format(date_of_delivery,'%Y-%m-%d') as date)  > date_parse('2019-09-01','%Y-%m-%d')

        group by ee_customer_id_extended having sum(optout_flg) is null
    ) A
    '''
    pd.read_sql(sql_code, conn)

    print('Stage 3/3')    

    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_2;
    '''
    pd.read_sql(sql_code, conn)

    sql_code = '''
    CREATE TABLE IF NOT EXISTS campaign_data.optout_model_base_2 as 

    select * from (
      select * from campaign_data.atc_ee_not_optout_customers_model 
      )TABLESAMPLE BERNOULLI(5)
    UNION ALL 
    select * from (
      select * from campaign_data.atc_ee_optout_customers_model
      )TABLESAMPLE BERNOULLI(100)  
      '''
    pd.read_sql(sql_code, conn)
    
    if write_to_df == 'yes':
        print('writing to df')
        return pd.read_sql('''select * from campaign_data.optout_model_base_2''', conn)


def generate_features(write_to_df='no'):
    print('Starting feature build')
    print('Stage 1/5')    
    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_features_1;
    '''
    pd.read_sql(sql_code, conn)

    sql_code = '''
    CREATE TABLE IF NOT EXISTS campaign_data.optout_model_base_features_1 as 

    SELECT 
        -- 
        distinct base.*,

        a.REFERENCE_DATE,
        -- CLM DATA
        a.os, 
        a.smartphone, 
        a.device_type, 
        a.volte_flg, 
        a.max_n_tile_release_price, 
        a.max_n_tile_body_weight,
        a.max_n_tile_display_size, 
        a.max_n_tile_display_resolution, 
        a.max_n_tile_cpu_cores, 
        a.max_n_tile_ram,
        a.sid_birth_age_years,
        a.sid_max_days_since_lifetime_start_date,
        a.sub_vo_cnt_2m_sum,
        a.sub_vi_dur_3m_mean, 
        a.sub_do_vol_3m_mean, 
        a.sub_m_reve_2m_max, 
        a.base_type, 
        a.last_text_allowance, 
        a.last_mins_allowance, 
        a.ooc_days, 
        a.data_1yr_vs_now_per,
        a.last_retail_mrc, 
        a.act_accs, 
        a.last_data_allowance, 
        a.wk4_hid_tot_pages, 
        a.SID_COMM_CHAN_EML, 
        a.SID_COMM_CHAN_SMS, 
        a.SID_COMM_TYPE_MARKETING, 
        a.SID_COMM_DD_LC_TYPE_LEGAL,
        a.SID_COMM_TYPE_SERVICE, 
        a.SID_COMM_DD_LC_CATEGORY_INFORMING, 
        a.SID_COMM_DD_LC_CATEGORY_XSELL,
        a.avg_week_wifi_count,
        a.avg_week_data_kb,
        avg_week_4g_data_kb,
        a.avg_week_3g_data_kb, 
        a.avg_week_2g_data_kb,
        a.avg_week_volte_secs, 
        a.avg_week_voice_secs, 
        a.avg_week_sms, 
        a.hid_we_dist_km,
        a.sub_wdwe_dist_km,
        a.sub_wdewe_dist_km, 
        a.number_of_adults,     
        a.wlan_capable,
        a.pid_avg_days_since_lifetime_start_date,
        a.pid_avg_rev_items,
        a.pid_avg_disc_items,
        a.pid_avg_ovechrg,
        a.pid_avg_revs,
        a.pid_avg_discs,
        a.hid_data_allowance,
        a.hid_mrc,
        a.hid_act_accs,
        a.hid_avg_ovechrg,
        a.avg_pid_comm_dd_lc_type_legal_hid,
        a.avg_pid_comm_dd_lc_category_upsell_hid,
        a.child_0to4,
        a.child5to11,
        a.child12to17,
        a.hid_min_days_since_lifetime_start_date,
        a.pid_do_alw_1w_sum,
        a.pid_vo_dur_1m_sum,
        a.pid_do_alw_3m_sum,
        a.pid_do_alw_1m_mean,
        a.last_regular_extra,
        a.number_of_children, 
        a.month1_donated, 
        a.month1_received,
        a.STACK,
        a.account_num  

    from ee_paym_dev.clm_dm_paym a 
    inner join campaign_data.optout_model_base_2 base  -- ORIGINAL BASE
        on cast(base.ee_customer_id as  VARCHAR(25)) = cast(a.SUB_ID as VARCHAR(25)) 
        and base.day_1_week_ago <= date_parse(a.REFERENCE_DATE,'%Y-%m-%d') 
        and date_parse(a.REFERENCE_DATE,'%Y-%m-%d')< base.optout_date
    '''
    pd.read_sql(sql_code, conn)
    
    print('Stage 2/5')  
    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_features_2a;
    '''
    pd.read_sql(sql_code, conn)

    sql_code = '''
    CREATE TABLE IF NOT EXISTS campaign_data.optout_model_base_features_2a as 

    select 
        distinct a.*

        -- bi_ccm_agg_mth features
        ,b.TOT_COST_IC
        ,b.TOT_COST_IC_VOICE
        ,b.TOT_COST_IC_SMS
        ,b.TOT_COST_IC_MMS
        ,b.VOL_UPG
        ,b.TOT_REV_PP_RC
        ,b.TOT_REV_PP_DISC
        ,b.TOT_REV_SOC_RC
        ,b.TOT_REV_SOC_DISC
        ,b.TOT_REV_INS_RC
        ,b.TOT_REV_INS_DISC
        ,b.VO_ALLW_DUR_TOT
        ,b.VO_ALLW_DUR_ROAM
        ,b.VO_ALLW_DUR_HOME
        ,b.VO_ALLW_DUR_ONNET
        ,b.VO_ALLW_DUR_OFFNET
        ,b.VO_ALLW_REV_TOT
        ,b.VO_ALLW_REV_ROAM
        ,b.VO_ALLW_REV_HOME
        ,b.VO_ALLW_REV_ONNET
        ,b.VO_ALLW_REV_OFFNET
        ,b.TOT_REV_IC
        ,b.TOT_REV_IC_VOICE
        ,b.TOT_REV_IC_SMS
        ,b.TOT_REV_IC_MMS
        ,b.D_ALLW_VOL_TOT
        ,b.D_ALLW_VOL_ROAM
        ,b.D_ALLW_VOL_HOME
        ,b.ACTIVE_30D_PAYM
        ,b.ANPU
        ,b.D_VOL_TOT
        ,b.DATA_PASS
        ,b.SI_CNT_TOT
        ,b.SO_CNT_TOT
        ,b.TOT_REV_DD_DISC
        ,b.TOT_REV_DD_ONEOFF
        ,b.TOT_REV_ONEOFF
        ,b.VI_CNT_TOT
        ,b.VI_DUR_TOT
        ,b.VO_CNT_TOT
        ,b.VO_DUR_TOT
    from (select * from ee_paym_dev.bi_ccm_agg_mth where current_brand='EE')  b 
    right join campaign_data.optout_model_base_features_1 a 
    on  cast(a.ee_customer_id as VARCHAR(25)) = cast(b.SUB_ID as VARCHAR(25)) 
    and a.date_month = b.AGG_MTH

    -- where b.current_brand='EE';
    '''

    pd.read_sql(sql_code, conn)

    print('Stage 3/5')  
    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_features_2b;
    '''
    pd.read_sql(sql_code, conn)

    sql_code = '''
    CREATE TABLE campaign_data.optout_model_base_features_2b AS    
        SELECT

                TAB11.*

                FROM
                (
                  -- Complete grand aggregation
                  SELECT 

                      -- group on account number
                      ACCOUNT_NUM,

                      -- aggregate features
                      SUM(JUSTBOUGHT) AS JUSTBOUGHT,
                      SUM(CREDIT_CLASS_1) AS CREDIT_CLASS_1,
                      SUM(CREDIT_CLASS_2) AS CREDIT_CLASS_2,
                      SUM(CREDIT_CLASS_3) AS CREDIT_CLASS_3,
                      SUM(CREDIT_CLASS_4) AS CREDIT_CLASS_4,
                      SUM(CREDIT_CLASS_5) AS CREDIT_CLASS_5,
                      SUM(CREDIT_CLASS_6) AS CREDIT_CLASS_6,
                      SUM(CREDIT_CLASS_7) AS CREDIT_CLASS_7,
                      SUM(CREDIT_CLASS_8) AS CREDIT_CLASS_8,
                      SUM(CREDIT_CLASS_9) AS CREDIT_CLASS_9,
                      SUM(CREDIT_CLASS_OTH) AS CREDIT_CLASS_OTH,

                      SUM(TOPEND_HS) AS TOPEND_HS,
                      COUNT(DISTINCT ACTIVESUB) AS ACTIVESUB,
                      SUM(A_TENURE_LIFETIME_MONTHS) AS A_TENURE_LIFETIME_MONTHS, --SUBID TENURE
                      SUM(A_TENURE_BRAND_MONTHS) AS A_TENURE_BRAND_MONTHS, --SUBID BRAND TENURE
                      SUM(A_INLIFE) AS A_INLIFE,
                      SUM(A_TENURE) AS A_TENURE,
                      SUM(A_OOC) AS A_OOC,
                      SUM(A_OOCTENUE) AS A_OOCTENUE,
                      SUM(A_UPGRADEELEG) AS A_UPGRADEELEG,
                      SUM(A_UPGRADETENURE) AS A_UPGRADETENURE,
                      SUM(A_HANDSETCOUNT) AS A_HANDSETCOUNT,
                      SUM(A_MBBCOUNT) AS A_MBBCOUNT,
                      SUM(A_SIMOCOUNT) AS A_SIMOCOUNT,
                      SUM(A_TABLETCOUNT) AS A_TABLETCOUNT,
                      SUM(A_WATCHCOUNT) AS A_WATCHCOUNT,
                      SUM(A_BASEMBB) AS A_BASEMBB,
                      SUM(A_BASEVOICE) AS A_BASEVOICE,
                      SUM(A_BASESIMO) AS A_BASESIMO,
                      SUM(A_PREV_BASE_TYPEMBB) AS A_PREV_BASE_TYPEMBB,
                      SUM(A_PREV_BASE_TYPEVOICE) AS A_PREV_BASE_TYPEVOICE,
                      SUM(A_PREV_BASE_TYPESIMO) AS A_PREV_BASE_TYPESIMO,
                      SUM(A_MRC_HANDSET) AS A_MRC_HANDSET,
                      SUM(A_MRC_SIMO ) AS A_MRC_SIMO,
                      SUM(A_MRC_SIMO_CONTRACT) AS A_MRC_SIMO_CONTRACT,
                      SUM(A_MRC_SIMO_ROLLING) AS A_MRC_SIMO_ROLLING,
                      SUM(A_MRC_MBB_DEVICE) AS A_MRC_MBB_DEVICE,
                      SUM(A_MRC_TABLET  ) AS A_MRC_TABLET,
                      SUM(A_MRC_MBB_DEVICE_CONTRACT) AS A_MRC_MBB_DEVICE_CONTRACT,
                      SUM(A_MRC_MBB_SIMO) AS A_MRC_MBB_SIMO,
                      SUM(A_MRC_MBB_DEVICE_ROLLING) AS A_MRC_MBB_DEVICE_ROLLING,
                      SUM(A_MRC_WATCH) AS A_MRC_WATCH,
                      SUM(A_MRC_MBB_CONNECTED) AS A_MRC_MBB_CONNECTED,
                      SUM(A_MRC_ADDON) AS A_MRC_ADDON,
                      SUM(A_MRC_UNKNOWN) AS A_MRC_UNKNOWN,
                      SUM(A_NO_OF_UPGRADES) AS  A_NO_OF_UPGRADES, --ACTIVE SUBID NO_OF_UPGRADES

                      -- PRICE PLAN STUFF
                      SUM(A_MRC) AS A_MRC,
                      SUM(A_DURATION) AS A_DURATION,

                      -- BINNING DATA, MINS AND TEXTS
                      SUM(DATA1) AS DATA1,
                      SUM(DATA2) AS DATA2,
                      SUM(DATA3) AS DATA3,
                      SUM(DATA4) AS DATA4,
                      SUM(DATA5) AS DATA5,
                      SUM(DATA6) AS DATA6,
                      SUM(DATA7) AS DATA7,
                      SUM(DATA8) AS DATA8,
                      SUM(DATA9) AS DATA9,
                      SUM(DATA10) AS DATA10,
                      SUM(DATA12) AS DATA12,
                      SUM(DATA14) AS DATA14,
                      SUM(DATA16) AS DATA16,
                      SUM(DATA18) AS DATA18,
                      SUM(DATA20) AS DATA20,
                      SUM(DATA24) AS DATA24,
                      SUM(DATA28) AS DATA28,
                      SUM(DATA30) AS DATA30,
                      SUM(DATA35) AS DATA35,
                      SUM(DATA40) AS DATA40,
                      SUM(DATA45) AS DATA45,
                      SUM(DATA50) AS DATA50,
                      SUM(DATA60) AS DATA60,
                      SUM(DATA70) AS DATA70,
                      SUM(DATA80) AS DATA80,
                      SUM(DATA90) AS DATA90,
                      SUM(DATA100) AS DATA100,
                      SUM(DATAOVER100) AS DATAOVER100,
                      SUM(MINS1000) AS MINS1000,
                      SUM(MINS2000) AS MINS2000,
                      SUM(MINS3000) AS MINS3000,
                      SUM(MINS4000) AS MINS4000,
                      SUM(MINS6000) AS MINS6000,
                      SUM(MINSOVER6000) AS MINSOVER6000,
                      SUM(TEXT100) AS TEXT100,
                      SUM(TEXT500) AS TEXT500,
                      SUM(TEXT1000) AS TEXT1000,
                      SUM(TEXT5000) AS TEXT5000,
                      SUM(TEXTOVER5000) AS TEXTOVER5000,
                      SUM(A_CONTRACT24) AS A_CONTRACT24,
                      SUM(A_CONTRACT12) AS A_CONTRACT12,
                      SUM(A_CONTRACT18) AS A_CONTRACT18,
                      SUM(A_CONTRACT1) AS A_CONTRACT1,

                      -- CHANNEL TYPE
                      SUM(A_INDIRECTCOUNT) AS A_INDIRECTCOUNT,
                      SUM(A_DIRECTCOUNT) AS A_DIRECTCOUNT,

                      -- DEVICE STUFF
                      SUM(A_VOLTECOUNT) AS A_VOLTECOUNT,
                      SUM(A_VOWIFICOUNT) AS A_VOWIFICOUNT,

                      SUM(A_SCOUNT) AS A_SCOUNT,
                      SUM(A_HHCOUNT) AS A_HHCOUNT,
                      SUM(A_TCOUNT) AS A_TCOUNT,
                      SUM(A_WRCOUNT) AS A_WRCOUNT,
                      SUM(A_PCOUNT) AS A_PCOUNT,
                      SUM(A_MPFPCOUNT) AS A_MPFPCOUNT,
                      SUM(A_MODEMCOUNT) AS A_MODEMCOUNT,
                      SUM(A_MODULECOUNT) AS A_MODULECOUNT,
                      SUM(A_CCCOUNT) AS A_CCCOUNT,
                      SUM(A_IOTCOUNT) AS A_IOTCOUNT,
                      SUM(A_VCOUNT) AS A_VCOUNT,
                      SUM(A_DCOUNT) AS A_DCOUNT,

                      SUM(A_ANDROIDHHCOUNT) AS A_ANDROIDHHCOUNT,
                      SUM(A_ANDROIDSPCOUNT) AS A_ANDROIDSPCOUNT,
                      SUM(A_ANDROIDMPFPCOUNT) AS A_ANDROIDMPFPCOUNT,
                      SUM(A_ANDROIDTCOUNT) AS A_ANDROIDTCOUNT,
                      SUM(A_ANDROIDWCOUNT) AS A_ANDROIDWCOUNT,

                      SUM(A_APPHHCOUNT) AS A_APPHHCOUNT,
                      SUM(A_APPSPCOUNT) AS A_APPSPCOUNT,
                      SUM(A_APPMPFPCOUNT) AS A_APPMPFPCOUNT,
                      SUM(A_APPTCOUNT) AS A_APPTCOUNT,
                      SUM(A_APPWCOUNT) AS A_APPWCOUNT,
                      SUM(A_WINHHCOUNT) AS A_WINHHCOUNT,
                      SUM(A_WINSPCOUNT) AS A_WINSPCOUNT,
                      SUM(A_WINMPFPCOUNT) AS A_WINMPFPCOUNT,
                      SUM(A_WINTCOUNT) AS A_WINTCOUNT,
                      SUM(A_WINWCOUNT) AS A_WINWCOUNT,

                      SUM(A_RIMHHCOUNT) AS A_RIMHHCOUNT,
                      SUM(A_RIMSPCOUNT) AS A_RIMSPCOUNT,
                      SUM(A_RIMMPFPCOUNT) AS A_RIMMPFPCOUNT,
                      SUM(A_RIMTCOUNT) AS A_RIMTCOUNT,
                      SUM(A_RIMWCOUNT) AS A_RIMWCOUNT,


                      SUM(A_OTHHHCOUNT) AS A_OTHHHCOUNT,
                      SUM(A_OTHSPCOUNT) AS A_OTHSPCOUNT,
                      SUM(A_OTHMPFPCOUNT) AS A_OTHMPFPCOUNT,
                      SUM(A_OTHTCOUNT) AS A_OTHTCOUNT,
                      SUM(A_OTHWCOUNT) AS A_OTHWCOUNT,

                      -----------------------------------------------CHURN DATA ACCOUNT---------------------------------------------------------
                      COUNT(DISTINCT C_SUB) AS C_SUB,
                      SUM(C_HANDSETCOUNT) AS C_HANDSETCOUNT,
                      SUM(C_MBBCOUNT) AS C_MBBCOUNT,
                      SUM(C_SIMOCOUNT) AS C_SIMOCOUNT,
                      SUM(C_TABLETCOUNT) AS C_TABLETCOUNT,
                      SUM(C_WATCHCOUNT) AS C_WATCHCOUNT,
                      SUM(C_BASEMBB) AS C_BASEMBB,
                      SUM(C_BASEVOICE) AS C_BASEVOICE,
                      SUM(C_BASESIMO	) AS C_BASESIMO,
                      SUM(C_PREV_BASE_TYPEMBB) AS C_PREV_BASE_TYPEMBB,
                      SUM(C_PREV_BASE_TYPEVOICE) AS C_PREV_BASE_TYPEVOICE,
                      SUM(C_PREV_BASE_TYPESIMO) AS C_PREV_BASE_TYPESIMO,
                      SUM(C_MRC_HANDSET) AS C_MRC_HANDSET,
                      SUM(C_MRC_SIMO) AS C_MRC_SIMO,
                      SUM(C_MRC_SIMO_CONTRACT) AS C_MRC_SIMO_CONTRACT,
                      SUM(C_MRC_SIMO_ROLLING) AS C_MRC_SIMO_ROLLING,
                      SUM(C_MRC_MBB_DEVICE) AS C_MRC_MBB_DEVICE,
                      SUM(C_MRC_TABLET) AS C_MRC_TABLET,
                      SUM(C_MRC_MBB_DEVICE_CONTRACT) AS C_MRC_MBB_DEVICE_CONTRACT,
                      SUM(C_MRC_MBB_SIMO) AS C_MRC_MBB_SIMO,
                      SUM(C_MRC_MBB_DEVICE_ROLLING) AS C_MRC_MBB_DEVICE_ROLLING,
                      SUM(C_MRC_WATCH) AS C_MRC_WATCH,
                      SUM(C_MRC_MBB_CONNECTED) AS C_MRC_MBB_CONNECTED,
                      SUM(C_MRC_ADDON) AS C_MRC_ADDON,
                      SUM(C_MRC_UNKNOWN) AS C_MRC_UNKNOWN,
                      SUM(C_NO_OF_UPGRADES) AS C_NO_OF_UPGRADES, --ACTIVE SUBID NO_OF_UPGRADES
                      SUM(C_CONTRACT24) AS C_CONTRACT24,
                      SUM(C_CONTRACT12) AS C_CONTRACT12,
                      SUM(C_CONTRACT18) AS C_CONTRACT18,
                      SUM(C_CONTRACT1) AS C_CONTRACT1,

                      -- CHANNEL TYPE
                      SUM(C_INDIRECTCOUNT) AS C_INDIRECTCOUNT,
                      SUM(C_DIRECTCOUNT) AS C_DIRECTCOUNT,

                      -- DEVICE STUFF,
                      SUM(C_VOLTECOUNT) AS C_VOLTECOUNT,
                      SUM(C_VOWIFICOUNT) AS C_VOWIFICOUNT,

                      SUM(C_SCOUNT) AS C_SCOUNT,
                      SUM(C_HHCOUNT) AS C_HHCOUNT,
                      SUM(C_TCOUNT) AS C_TCOUNT,
                      SUM(C_WRCOUNT) AS C_WRCOUNT,
                      SUM(C_PCOUNT) AS C_PCOUNT,
                      SUM(C_MPFPCOUNT) AS C_MPFPCOUNT,
                      SUM(C_MODEMCOUNT) AS C_MODEMCOUNT,
                      SUM(C_MODULECOUNT) AS C_MODULECOUNT,
                      SUM(C_CCCOUNT) AS C_CCCOUNT,
                      SUM(C_IOTCOUNT) AS C_IOTCOUNT,
                      SUM(C_VCOUNT) AS C_VCOUNT,
                      SUM(C_DCOUNT) AS C_DCOUNT,

                      SUM(C_ANDROIDHHCOUNT) AS C_ANDROIDHHCOUNT,
                      SUM(C_ANDROIDSPCOUNT) AS C_ANDROIDSPCOUNT,
                      SUM(C_ANDROIDMPFPCOUNT) AS C_ANDROIDMPFPCOUNT,
                      SUM(C_ANDROIDTCOUNT) AS C_ANDROIDTCOUNT,
                      SUM(C_ANDROIDWCOUNT) AS C_ANDROIDWCOUNT,

                      SUM(C_APPHHCOUNT) AS C_APPHHCOUNT,
                      SUM(C_APPSPCOUNT) AS C_APPSPCOUNT,
                      SUM(C_APPMPFPCOUNT) AS C_APPMPFPCOUNT,
                      SUM(C_APPTCOUNT) AS C_APPTCOUNT,
                      SUM(C_APPWCOUNT) AS C_APPWCOUNT,
                      SUM(C_WINHHCOUNT) AS C_WINHHCOUNT,
                      SUM(C_WINSPCOUNT) AS C_WINSPCOUNT,
                      SUM(C_WINMPFPCOUNT) AS C_WINMPFPCOUNT,
                      SUM(C_WINTCOUNT) AS C_WINTCOUNT,
                      SUM(C_WINWCOUNT) AS C_WINWCOUNT,

                      SUM(C_RIMHHCOUNT) AS C_RIMHHCOUNT,
                      SUM(C_RIMSPCOUNT) AS C_RIMSPCOUNT,
                      SUM(C_RIMMPFPCOUNT) AS C_RIMMPFPCOUNT,
                      SUM(C_RIMTCOUNT) AS C_RIMTCOUNT,
                      SUM(C_RIMWCOUNT) AS C_RIMWCOUNT,

                      SUM(C_OTHHHCOUNT) AS C_OTHHHCOUNT,
                      SUM(C_OTHSPCOUNT) AS C_OTHSPCOUNT,
                      SUM(C_OTHMPFPCOUNT) AS C_OTHMPFPCOUNT,
                      SUM(C_OTHTCOUNT) AS C_OTHTCOUNT,
                      SUM(C_OTHWCOUNT) AS C_OTHWCOUNT


                  FROM (
                     --- deffine features from E_PAYM_DEV.BI_CCM_BASE, EE_PAYM_SOURCE.DIM_PRODUCT_REF, EE_PAYM_SOURCE.DIM_DEVICE_REF, EE_PAYM_SOURCE.DIM_CHANNEL_REF
                    SELECT 
                        --ONE LINE PER SUB_ID
                        A2.SUB_ID,
                        A2.ACCOUNT_NUM,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.MARKETING_NAME IN
                                              ('APPLE IPHONE 6',
                                              'APPLE IPHONE 6 PLUS',
                                              'APPLE IPHONE 6S',
                                              'APPLE IPHONE 6S PLUS',
                                              'APPLE IPHONE 7',
                                              'APPLE IPHONE 7 PLUS',
                                              'APPLE IPHONE 8',
                                              'APPLE IPHONE 8 PLUS',
                                              'APPLE IPHONE SE',
                                              'APPLE IPHONE X',
                                              'APPLE IPHONE XS',
                                              'APPLE IPHONE XS MAX',
                                              'APPLE IPHONE XR',
                                              'APPLE IPHONE 11',
                                              'APPLE IPHONE 11 PRO',
                                              'APPLE IPHONE 11 PRO MAX',
                                              'SAMSUNG GALAXY A3 2017 LTE',
                                              'SAMSUNG GALAXY A5 2017 LTE',
                                              'SAMSUNG GALAXY A6 LTE',
                                              'SAMSUNG GALAXY A8 A530F LTE',
                                              'SAMSUNG GALAXY J3 2017 LTE',
                                              'SAMSUNG GALAXY J5 2017 LTE',
                                              'SAMSUNG GALAXY J6 LTE',
                                              'SAMSUNG GALAXY XCOVER 4 LTE',
                                              'SAMSUNG GALAXY S7 G930F LTE',
                                              'SAMSUNG GALAXY S7 EDGE G935F LTE',
                                              'SAMSUNG GALAXY S8 G950F LTE',
                                              'SAMSUNG GALAXY S8 PLUS G955F LTE',
                                              'SAMSUNG GALAXY S9 G960F LTE',
                                              'SAMSUNG GALAXY S9 PLUS G965F LTE',
                                              'SAMSUNG GALAXY NOTE 8 LTE',
                                              'SAMSUNG GALAXY NOTE 9 LTE',
                                              'SAMSUNG GALAXY S10E LTE',
                                              'SAMSUNG GALAXY S10 LTE',
                                              'SAMSUNG GALAXY S10 PLUS LTE',
                                              'SAMSUNG GALAXY S10 5G',
                                              'SAMSUNG GALAXY S20 ULTRA',
                                              'SAMSUNG GALAXY S20+'
                                              )
                                  AND A2.BASE_TYPE IN ('Voice', 'SIMO') THEN 1 
                                  ELSE 0 
                                  END AS TOPEND_HS,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN DATE_DIFF('month', A2.LIFETIME_START_DATE, A2.optout_date) ELSE 0 END AS A_TENURE_LIFETIME_MONTHS, --SUBID TENURE
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN DATE_DIFF('month', A2.CURRENT_BRAND_START_DATE, A2.optout_date) ELSE 0 END AS A_TENURE_BRAND_MONTHS, --SUBID BRAND TENURE
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date < (CONTRACT_END_DATE - interval '45' day) THEN 1 ELSE 0 END AS A_INLIFE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN DATE_DIFF('month', A2.CONTRACT_START_DATE, A2.optout_date) ELSE 0 END AS A_TENURE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date >= (CONTRACT_END_DATE + interval '45' day) THEN 1 ELSE 0 END AS A_OOC,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date >= (CONTRACT_END_DATE + interval '45' day) THEN DATE_DIFF('month', CONTRACT_END_DATE, A2.optout_date) ELSE 0 END AS A_OOCTENUE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date >= (CONTRACT_END_DATE - interval '45' day) AND A2.optout_date < (CONTRACT_END_DATE + interval '45' day) THEN 1 ELSE 0 END AS A_UPGRADEELEG,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date >= (CONTRACT_END_DATE - interval '45' day) AND A2.optout_date < (CONTRACT_END_DATE + interval '45' day) THEN DATE_DIFF('month', CONTRACT_END_DATE, A2.optout_date) ELSE 0 END AS A_UPGRADETENURE,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.optout_date <= (CONTRACT_START_DATE + interval '14' day) THEN 1 ELSE 0 END AS JUSTBOUGHT, -- Now just Acquired
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN A2.SUB_ID END AS ACTIVESUB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '1' THEN 1 ELSE 0 END AS CREDIT_CLASS_1,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '2' THEN 1 ELSE 0 END AS CREDIT_CLASS_2,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '3' THEN 1 ELSE 0 END AS CREDIT_CLASS_3,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '4' THEN 1 ELSE 0 END AS CREDIT_CLASS_4,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '5' THEN 1 ELSE 0 END AS CREDIT_CLASS_5,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '6' THEN 1 ELSE 0 END AS CREDIT_CLASS_6,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '7' THEN 1 ELSE 0 END AS CREDIT_CLASS_7,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '8' THEN 1 ELSE 0 END AS CREDIT_CLASS_8,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS = '9' THEN 1 ELSE 0 END AS CREDIT_CLASS_9,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.CREDIT_CLASS not in ('1', '2', '3', '4','5', '6','7','8', '9') THEN 1 ELSE 0 END AS CREDIT_CLASS_OTH,    


                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.PRODUCT_GROUP = 'Handset' THEN 1 ELSE 0 END AS A_HANDSETCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.PRODUCT_GROUP LIKE 'MBB%' THEN 1 ELSE 0 END AS A_MBBCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.PRODUCT_GROUP LIKE 'SIMO%' THEN 1 ELSE 0 END AS A_SIMOCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.PRODUCT_GROUP = 'Tablet' THEN 1 ELSE 0 END AS A_TABLETCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.PRODUCT_GROUP = 'Watch' THEN 1 ELSE 0 END AS A_WATCHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.BASE_TYPE = 'MBB' THEN 1 ELSE 0 END AS A_BASEMBB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.BASE_TYPE = 'Voice' THEN 1 ELSE 0 END AS A_BASEVOICE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.BASE_TYPE = 'SIMO' THEN 1 ELSE 0 END AS A_BASESIMO,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.PREV_BASE_TYPE = 'MBB' THEN 1 ELSE 0 END AS A_PREV_BASE_TYPEMBB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.PREV_BASE_TYPE = 'Voice' THEN 1 ELSE 0 END AS A_PREV_BASE_TYPEVOICE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.PREV_BASE_TYPE = 'SIMO' THEN 1 ELSE 0 END AS A_PREV_BASE_TYPESIMO,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Handset' )             AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_HANDSET,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO' )                AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_SIMO ,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO Contract' )       AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_SIMO_CONTRACT,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO Rolling' )        AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_SIMO_ROLLING,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device' )          AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_MBB_DEVICE,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Tablet' )              AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_TABLET  ,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device Contract' ) AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_MBB_DEVICE_CONTRACT,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB SIMO' )            AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_MBB_SIMO,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device Rolling' )  AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_MBB_DEVICE_ROLLING,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Watch' )               AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_WATCH,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Connected' )       AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_MBB_CONNECTED,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Addon' )               AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_ADDON,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Unknown' )             AND ( A2.SUBSCRIBER_STATUS = 'A' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS A_MRC_UNKNOWN,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A2.NO_OF_UPGRADES > 0 THEN A2.NO_OF_UPGRADES - 1 ELSE 0 END AS A_NO_OF_UPGRADES, --ACTIVE SUBID NO_OF_UPGRADES

                        -- PRICE PLAN STUFF
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN A3.MRC_INCL_VAT END AS A_MRC,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' THEN A3.CONTRACT_DURATION END AS A_DURATION,

                        -- BINNING DATA, MINS AND TEXTS
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 1500 THEN 1 ELSE 0 END AS DATA1,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 2500 AND A3.TOTAL_ALLOWANCE_DATA >= 1500 THEN 1 ELSE 0 END AS  DATA2,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 3500 AND A3.TOTAL_ALLOWANCE_DATA >= 2500 THEN 1 ELSE 0  END AS  DATA3,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 4500 AND A3.TOTAL_ALLOWANCE_DATA >= 3500 THEN 1 ELSE 0  END AS  DATA4,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 5500 AND A3.TOTAL_ALLOWANCE_DATA >= 4500 THEN 1 ELSE 0  END AS  DATA5,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 6500 AND A3.TOTAL_ALLOWANCE_DATA >= 5500 THEN 1 ELSE 0  END AS  DATA6,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 7500 AND A3.TOTAL_ALLOWANCE_DATA >= 6500 THEN 1 ELSE 0  END AS  DATA7,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 8500 AND A3.TOTAL_ALLOWANCE_DATA >= 7500 THEN 1 ELSE 0  END AS  DATA8,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 9500 AND A3.TOTAL_ALLOWANCE_DATA >= 8500 THEN 1 ELSE 0  END AS  DATA9,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 10500 AND A3.TOTAL_ALLOWANCE_DATA >= 9500 THEN 1 ELSE 0  END AS  DATA10,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 12500 AND A3.TOTAL_ALLOWANCE_DATA >= 10500 THEN 1 ELSE 0  END AS  DATA12,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 14500 AND A3.TOTAL_ALLOWANCE_DATA >= 12500 THEN 1 ELSE 0  END AS  DATA14,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 16500 AND A3.TOTAL_ALLOWANCE_DATA >= 14500 THEN 1 ELSE 0  END AS  DATA16,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 18500 AND A3.TOTAL_ALLOWANCE_DATA >= 16500 THEN 1 ELSE 0  END AS  DATA18,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 20500 AND A3.TOTAL_ALLOWANCE_DATA >= 18500 THEN 1 ELSE 0  END AS  DATA20,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 24500 AND A3.TOTAL_ALLOWANCE_DATA >= 20500 THEN 1 ELSE 0  END AS  DATA24,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 28500 AND A3.TOTAL_ALLOWANCE_DATA >= 24500 THEN 1 ELSE 0  END AS  DATA28,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 30500 AND A3.TOTAL_ALLOWANCE_DATA >= 28500 THEN 1 ELSE 0  END AS  DATA30,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 35500 AND A3.TOTAL_ALLOWANCE_DATA >= 30500  THEN 1 ELSE 0  END AS  DATA35,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 40500 AND A3.TOTAL_ALLOWANCE_DATA >= 35500 THEN 1 ELSE 0  END AS  DATA40,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 45500 AND A3.TOTAL_ALLOWANCE_DATA >= 40500  THEN 1 ELSE 0  END AS  DATA45,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 50500 AND A3.TOTAL_ALLOWANCE_DATA >= 45500 THEN 1 ELSE 0  END AS  DATA50,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 60500 AND A3.TOTAL_ALLOWANCE_DATA >= 50500  THEN 1 ELSE 0  END AS  DATA60,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 70500 AND A3.TOTAL_ALLOWANCE_DATA >= 60500 THEN 1 ELSE 0  END AS  DATA70,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 80500 AND A3.TOTAL_ALLOWANCE_DATA >= 70500 THEN 1 ELSE 0  END AS  DATA80,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 90500 AND A3.TOTAL_ALLOWANCE_DATA >= 80500 THEN 1 ELSE 0  END AS  DATA90,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA < 100500 AND A3.TOTAL_ALLOWANCE_DATA >= 90500 THEN 1 ELSE 0  END AS  DATA100,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_DATA >= 100500 THEN 1 ELSE 0  END AS DATAOVER100,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS < 1000 THEN 1 ELSE 0  END AS  MINS1000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS < 2000 AND A3.TOTAL_ALLOWANCE_MINS >= 1000 THEN 1 ELSE 0  END AS  MINS2000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS < 3000 AND A3.TOTAL_ALLOWANCE_MINS >= 2000 THEN 1 ELSE 0  END AS  MINS3000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS < 4000 AND A3.TOTAL_ALLOWANCE_MINS >= 3000 THEN 1 ELSE 0  END AS  MINS4000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS < 6000 AND A3.TOTAL_ALLOWANCE_MINS >= 4000 THEN 1 ELSE 0  END AS  MINS6000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_MINS >= 6000 THEN 1 ELSE 0  END AS  MINSOVER6000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_TEXT < 100 THEN 1 ELSE 0 END AS  TEXT100,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_TEXT < 500 AND A3.TOTAL_ALLOWANCE_TEXT >= 100 THEN 1 ELSE 0  END AS  TEXT500,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_TEXT < 1000 AND A3.TOTAL_ALLOWANCE_TEXT >= 500 THEN 1 ELSE 0  END AS  TEXT1000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_TEXT < 5000 AND A3.TOTAL_ALLOWANCE_TEXT >= 1000 THEN 1 ELSE 0  END AS  TEXT5000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.TOTAL_ALLOWANCE_TEXT >= 5000 THEN 1 ELSE 0 END AS  TEXTOVER5000,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.CONTRACT_DURATION = 24 THEN 1 ELSE 0 END AS A_CONTRACT24,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.CONTRACT_DURATION = 12 THEN 1 ELSE 0 END AS A_CONTRACT12,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.CONTRACT_DURATION = 18 THEN 1 ELSE 0 END AS A_CONTRACT18,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A3.CONTRACT_DURATION = 1 THEN 1 ELSE 0 END AS A_CONTRACT1,

                        -- CHANNEL TYPE
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A5.CHAN_TYPE = 'INDIRECT' THEN 1 ELSE 0 END AS A_INDIRECTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A5.CHAN_TYPE = 'DIRECT' THEN 1 ELSE 0 END AS A_DIRECTCOUNT,

                        -- DEVICE STUFF
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.VOLTE_FLAG = 'Y' THEN 1 ELSE 0 END AS A_VOLTECOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.VOWIFI_FLAG = 'Y' THEN 1 ELSE 0 END AS A_VOWIFICOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_SCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_HHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_TCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'WLAN Router' THEN 1 ELSE 0 END AS A_WRCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Portable(include PDA)' THEN 1 ELSE 0 END AS A_PCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_MPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Modem' THEN 1 ELSE 0 END AS A_MODEMCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Module' THEN 1 ELSE 0 END AS A_MODULECOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Connected Computer' THEN 1 ELSE 0 END AS A_CCCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'IoT Device' THEN 1 ELSE 0 END AS A_IOTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Vehicle' THEN 1 ELSE 0 END AS A_VCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.DEVICE_TYPE = 'Dongle' THEN 1 ELSE 0 END AS A_DCOUNT,


                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_ANDROIDHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_ANDROIDSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_ANDROIDMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_ANDROIDTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS A_ANDROIDWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_APPHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_APPSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_APPMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_APPTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS A_APPWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_WINHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_WINSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_WINMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_WINTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS A_WINWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_RIMHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_RIMSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_RIMMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_RIMTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS A_RIMWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS A_OTHHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS A_OTHSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS A_OTHMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS A_OTHTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'A' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS A_OTHWCOUNT,

                        ------------------------------------------------CHURN DATA ACCOUNT---------------------------------------------------------
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' THEN A2.SUB_ID END AS C_SUB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.PRODUCT_GROUP = 'Handset' THEN 1 ELSE 0 END AS C_HANDSETCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.PRODUCT_GROUP LIKE 'MBB%' THEN 1 ELSE 0 END AS C_MBBCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.PRODUCT_GROUP LIKE 'SIMO%' THEN 1 ELSE 0 END AS C_SIMOCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.PRODUCT_GROUP = 'Tablet' THEN 1 ELSE 0 END AS C_TABLETCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.PRODUCT_GROUP = 'Watch' THEN 1 ELSE 0 END AS C_WATCHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.BASE_TYPE = 'MBB' THEN 1 ELSE 0 END AS C_BASEMBB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.BASE_TYPE = 'Voice' THEN 1 ELSE 0 END AS C_BASEVOICE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.BASE_TYPE = 'SIMO' THEN 1 ELSE 0 END AS C_BASESIMO,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.PREV_BASE_TYPE = 'MBB' THEN 1 ELSE 0 END AS C_PREV_BASE_TYPEMBB,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.PREV_BASE_TYPE = 'Voice' THEN 1 ELSE 0 END AS C_PREV_BASE_TYPEVOICE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.PREV_BASE_TYPE = 'SIMO' THEN 1 ELSE 0 END AS C_PREV_BASE_TYPESIMO,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Handset' )             AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_HANDSET,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO' )                AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_SIMO,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO Contract' )       AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_SIMO_CONTRACT,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'SIMO Rolling' )        AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_SIMO_ROLLING,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device' )          AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_MBB_DEVICE,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Tablet' )              AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_TABLET,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device Contract' ) AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_MBB_DEVICE_CONTRACT,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB SIMO' )            AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_MBB_SIMO,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Device Rolling' )  AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_MBB_DEVICE_ROLLING,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Watch' )               AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_WATCH,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'MBB Connected' )       AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_MBB_CONNECTED,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Addon' )               AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_ADDON,
                        CASE WHEN ( A3.PRODUCT_GROUP = 'Unknown' )             AND ( A2.SUBSCRIBER_STATUS = 'C' )  THEN A3.MRC_INCL_VAT ELSE 0 END        AS C_MRC_UNKNOWN,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A2.NO_OF_UPGRADES > 0 THEN A2.NO_OF_UPGRADES - 1 ELSE 0 END AS C_NO_OF_UPGRADES, --ACTIVE SUBID NO_OF_UPGRADES
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.CONTRACT_DURATION = 24 THEN 1 ELSE 0 END AS C_CONTRACT24,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.CONTRACT_DURATION = 12 THEN 1 ELSE 0 END AS C_CONTRACT12,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.CONTRACT_DURATION = 18 THEN 1 ELSE 0 END AS C_CONTRACT18,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A3.CONTRACT_DURATION = 1 THEN 1 ELSE 0 END AS C_CONTRACT1,

                        -- CHANNEL TYPE,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A5.CHAN_TYPE = 'INDIRECT' THEN 1 ELSE 0 END AS C_INDIRECTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A5.CHAN_TYPE = 'DIRECT' THEN 1 ELSE 0 END AS C_DIRECTCOUNT,

                        -- DEVICE STUFF
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.VOLTE_FLAG = 'Y' THEN 1 ELSE 0 END AS C_VOLTECOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.VOWIFI_FLAG = 'Y' THEN 1 ELSE 0 END AS C_VOWIFICOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_SCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_HHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_TCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'WLAN Router' THEN 1 ELSE 0 END AS C_WRCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Portable(include PDA)' THEN 1 ELSE 0 END AS C_PCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_MPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Modem' THEN 1 ELSE 0 END AS C_MODEMCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Module' THEN 1 ELSE 0 END AS C_MODULECOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Connected Computer' THEN 1 ELSE 0 END AS C_CCCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'IoT Device' THEN 1 ELSE 0 END AS C_IOTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Vehicle' THEN 1 ELSE 0 END AS C_VCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.DEVICE_TYPE = 'Dongle' THEN 1 ELSE 0 END AS C_DCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_ANDROIDHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_ANDROIDSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_ANDROIDMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_ANDROIDTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'ANDROID' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS C_ANDROIDWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_APPHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_APPSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_APPMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_APPTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'APPLE OS' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS C_APPWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_WINHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_WINSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_WINMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_WINTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'WINDOWS MOBILE' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS C_WINWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_RIMHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_RIMSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_RIMMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_RIMTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS = 'RIM' AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS C_RIMWCOUNT,

                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Handheld' THEN 1 ELSE 0 END AS C_OTHHHCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Smartphone' THEN 1 ELSE 0 END AS C_OTHSPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Mobile Phone/Feature phone' THEN 1 ELSE 0 END AS C_OTHMPFPCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Tablet' THEN 1 ELSE 0 END AS C_OTHTCOUNT,
                        CASE WHEN A2.SUBSCRIBER_STATUS = 'C' AND A4.OS NOT IN ('ANDROID', 'APPLE OS', 'WINDOWS MOBILE', 'RIM') AND A4.DEVICE_TYPE = 'Wearable' THEN 1 ELSE 0 END AS C_OTHWCOUNT

                  FROM (
                         SELECT
                             a.ee_customer_id,
                             a.optout_date,
                             b.* 

                         -- Take only matching features from base. Rest will be added later
                         FROM EE_PAYM_DEV.BI_CCM_BASE AS b 

                         -- BASE TABLE
                         right join campaign_data.optout_model_base_features_2a a
                              on cast(a.ee_customer_id as VARCHAR(25)) = cast(b.SUB_ID as VARCHAR(25))

                         WHERE b.STACK = 'TMEE'
                         AND b.BUSINESS_FLAG = 'N'
                         AND b.BILLING_BASE = 'PAYM'
                         AND b.SUBSCRIBER_STATUS = 'A'
                         AND DATE_PARSE(b.FROM_DATE, '%Y-%m-%d') <> b.TO_DATE
                         AND a.optout_date BETWEEN DATE_PARSE(b.FROM_DATE,'%Y-%m-%d') AND (b.TO_DATE - interval '1' day)
                       ) AS A2

                  LEFT JOIN EE_PAYM_SOURCE.DIM_PRODUCT_REF AS A3
                      ON A2.PRICE_PLAN = A3.PRODUCT_CODE
                      AND A2.CONTRACT_START_DATE BETWEEN A3.EFFECTIVE_DATE AND A3.EXPIRATION_DATE
                      AND A3.PRODUCT_TYPE = 'Price Plan'
                      AND A3.STACK = 'TMEE'
                      AND A3.FAMILY_TYPE = 'PAYM'
                      AND A3.SEGMENT_IND = 'C'

                  LEFT JOIN EE_PAYM_SOURCE.DIM_DEVICE_REF AS A4
                      ON COALESCE( A2.RET_TAC, A2.ACQ_TAC ) = A4.TAC

                  LEFT JOIN ( 
                              SELECT 
                                    DISTINCT CHAN_CODE, 
                                    CHAN_TYPE 
                              FROM EE_PAYM_SOURCE.DIM_CHANNEL_REF 
                              WHERE CHAN_TYPE IN ( 'DIRECT', 'INDIRECT' ) 
                              ) AS A5
                      ON COALESCE( A2.RET_CHAN_CODE, A2.ACQ_CHAN_CODE ) = A5.CHAN_CODE

                  ) AS TAB1

                  GROUP BY ACCOUNT_NUM

                ) AS TAB11

                --JOIN default.SD_PAYGM_LEADSUB_TRAIN_FRAME1 AS LEADSUB --PROFILE DATE FOR TRAIN FRAME 1 IN DATAMART
                --ON TAB11.ACCOUNT_NUM = LEADSUB.ACCOUNT_NUM                        
                WHERE TAB11.ACTIVESUB > 0
    '''
    pd.read_sql(sql_code, conn)

    print('Stage 4/5')  
    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_features_2c;
    '''
    pd.read_sql(sql_code, conn)    
    
    sql_code = '''
    CREATE TABLE IF NOT EXISTS campaign_data.optout_model_base_features_2c AS

    with comm_table as (

            select 
                a.ee_customer_id_extended as ee_customer_id,
                a.comm_id,
                a.brand,
                b.optout_date,
                a.date_of_delivery,
                a.channel,
                a.campaign_objective,
                a.days_from_last_EE_comm,
                a.days_from_last_open,
                a.days_from_last_click,
                a.total_EE_comms_to_date,
                a.total_opens_to_date,
                a.total_clicks_to_date,
                date_diff('day',a.date_of_delivery,b.optout_date) as diff,
                row_number() OVER(PARTITION BY a.ee_customer_id_extended ORDER BY a.date_of_delivery desc) as RN
            from campaign_data.atc_sccv_timing  a 
            inner join campaign_data.optout_model_base b
                on a.ee_customer_id_extended= b.ee_customer_id
            where date_of_delivery <=  b.optout_date              
            and control_grp_flg = 'N'   
            and a.ee_customer_id_extended is not null
            -- and a.ee_customer_id_extended = '900003236442' -- will vary everytime optout_model_base is rebuilt
      )


    select
        send.ee_customer_id
        ,send.total_em_comms_lstwk
        ,send.total_em_comms_lstmnth
        ,send.total_em_comms_to_date
        ,send.total_mms_comms_lstwk
        ,send.total_mms_comms_lstmnth
        ,send.total_mms_comms_to_date
        ,send.total_sms_comms_lstwk
        ,send.total_sms_comms_lstmnth
        ,send.total_sms_comms_to_date
        ,send.total_comms_lstwk
        ,send.total_comms_lstmnth
        ,send.total_comms_to_date
        ,send.total_em_opens_lstwk
        ,send.total_em_opens_lstmnth
        ,send.total_em_opens_to_date
        ,send.total_opens_lstwk
        ,send.total_opens_lstmnth
        ,send.total_opens_to_date
        ,send.total_em_clicks_lstwk
        ,send.total_em_clicks_lstmnth
        ,send.total_em_clicks_to_date
        ,send.total_mms_clicks_lstwk
        ,send.total_mms_clicks_lstmnth
        ,send.total_mms_clicks_to_date
        ,send.total_sms_clicks_lstwk
        ,send.total_sms_clicks_lstmnth
        ,send.total_sms_clicks_to_date
        ,send.total_clicks_lstwk
        ,send.total_clicks_lstmnth
        ,send.total_clicks_to_date
        ,send.campaign_mix_lstwk
        ,send.campaign_mix_lstmnth
        ,send.campaign_mix_to_date
        ,send.channel_lstwk
        ,send.channel_lstmnth
        ,send.channel_to_date
        ,send.brand_lstwk
        ,send.brand_lstmnth
        ,send.brand_to_date
        ,send.days_from_last_EE_comm   
        ,open.days_from_last_open
        ,click.days_from_last_click

    from (

        select
            ee_customer_id,

            count(case when diff <= 7 and channel='E' then total_EE_comms_to_date else null end) as total_em_comms_lstwk,
            count(case when diff <= 30 and channel='E'  then total_EE_comms_to_date else null end) as total_em_comms_lstmnth,
            count(case when channel='E' then total_EE_comms_to_date else null end) as total_em_comms_to_date,

            count(case when diff <= 7 and channel='M' then total_EE_comms_to_date else null end) as total_mms_comms_lstwk,
            count(case when diff <= 30 and channel='M'  then total_EE_comms_to_date else null end) as total_mms_comms_lstmnth,
            count(case when channel='M' then total_EE_comms_to_date else null end) as total_mms_comms_to_date,

            count(case when diff <= 7 and channel='S' then total_EE_comms_to_date else null end) as total_sms_comms_lstwk,
            count(case when diff <= 30 and channel='S'  then total_EE_comms_to_date else null end) as total_sms_comms_lstmnth,
            count(case when channel='S' then total_EE_comms_to_date else null end) as total_sms_comms_to_date,  

            count(case when diff <= 7 then total_EE_comms_to_date else null end) as total_comms_lstwk,
            count(case when diff <= 30 then total_EE_comms_to_date else null end) as total_comms_lstmnth,
            count(total_EE_comms_to_date) as total_comms_to_date,  

            count(case when diff <= 7 and channel='E' then total_opens_to_date else null end) as total_em_opens_lstwk,
            count(case when diff <= 30 and channel='E'  then total_opens_to_date else null end) as total_em_opens_lstmnth,
            count(case when channel='E' then total_opens_to_date else null end) as total_em_opens_to_date,  

            count(case when diff <= 7 then total_opens_to_date else null end) as total_opens_lstwk,
            count(case when diff <= 30 then total_opens_to_date else null end) as total_opens_lstmnth,
            count(total_opens_to_date) as total_opens_to_date,

            count(case when diff <= 7 and channel='E' then total_clicks_to_date else null end) as total_em_clicks_lstwk,
            count(case when diff <= 30 and channel='E' then total_clicks_to_date else null end) as total_em_clicks_lstmnth,
            count(case when channel='E' then total_clicks_to_date else null end) as total_em_clicks_to_date,   

            count(case when diff <= 7 and channel='M' then total_clicks_to_date else null end) as total_mms_clicks_lstwk,
            count(case when diff <= 30 and channel='M' then total_clicks_to_date else null end) as total_mms_clicks_lstmnth,
            count(case when channel='M' then total_clicks_to_date else null end) as total_mms_clicks_to_date, 

            count(case when diff <= 7 and channel='S' then total_clicks_to_date else null end) as total_sms_clicks_lstwk,
            count(case when diff <= 30 and channel='S' then total_clicks_to_date else null end) as total_sms_clicks_lstmnth,
            count(case when channel='S' then total_clicks_to_date else null end) as total_sms_clicks_to_date,   

            count(case when diff <= 7 then total_clicks_to_date else null end) as total_clicks_lstwk,
            count(case when diff <= 30 then total_clicks_to_date else null end) as total_clicks_lstmnth,
            count(total_clicks_to_date) as total_clicks_to_date,    

            count(distinct case when diff <= 7 then campaign_objective else null end) as campaign_mix_lstwk,
            count(distinct case when diff <= 30 then campaign_objective else null end) as campaign_mix_lstmnth,
            count(distinct campaign_objective) as campaign_mix_to_date,  

            count(distinct case when diff <= 7 then channel else null end) as channel_lstwk,
            count(distinct case when diff <= 30 then channel else null end) as channel_lstmnth,
            count(distinct channel) as channel_to_date,   

            count(distinct case when diff <= 7 then brand else null end) as brand_lstwk,
            count(distinct case when diff <= 30 then brand else null end) as brand_lstmnth,
            count(distinct brand) as brand_to_date,     

            max(case when RN = 1 then days_from_last_EE_comm else null end) as days_from_last_EE_comm
            -- add seperately
            -- add seperately    

        from (select * from comm_table) group by ee_customer_id

        ) send

    left join ( -- filter out rows with no open history to extract open lag time
        select
            ee_customer_id,
            max(case when RN_OPN = 1 then days_from_last_open else null end) as days_from_last_open
        from (select *,row_number() OVER(PARTITION BY ee_customer_id ORDER BY date_of_delivery desc) as RN_OPN
              from comm_table where days_from_last_open is not null ) group by ee_customer_id  
      ) open
      on send.ee_customer_id=open.ee_customer_id

    left join ( -- filter out rows with no click history to extract open lag time
        select
            ee_customer_id,
            max(case when RN_CLK = 1 then days_from_last_click else null end) as days_from_last_click
        from (select *,row_number() OVER(PARTITION BY ee_customer_id ORDER BY date_of_delivery desc) as RN_CLK
              from comm_table where days_from_last_click is not null ) group by ee_customer_id  
      ) click
      on send.ee_customer_id=click.ee_customer_id  

    '''

    pd.read_sql(sql_code, conn)
    
    print('Stage 5/5')  
    
    sql_code = '''
    drop table if exists  campaign_data.optout_model_base_features_combined;
    '''
    pd.read_sql(sql_code, conn)

    sql_code = '''
    CREATE TABLE IF NOT EXISTS campaign_data.optout_model_base_features_combined AS
    select 
        -- target, bi_ccm_agg_mth & CLM_PAYM features
        a.ee_customer_id
        ,a.optout_flag
        ,a.optout_cnt    
        ,a.optout_date
        ,a.os
        ,a.smartphone
        ,a.device_type
        ,a.volte_flg
        ,a.max_n_tile_release_price
        ,a.max_n_tile_body_weight
        ,a.max_n_tile_display_size
        ,a.max_n_tile_display_resolution
        ,a.max_n_tile_cpu_cores
        ,a.max_n_tile_ram
        ,a.sid_birth_age_years
        ,a.sid_max_days_since_lifetime_start_date
        ,a.sub_vo_cnt_2m_sum
        ,a.sub_vi_dur_3m_mean
        ,a.sub_do_vol_3m_mean
        ,a.sub_m_reve_2m_max
        ,a.base_type
        ,a.last_text_allowance
        ,a.last_mins_allowance
        ,a.ooc_days
        ,a.data_1yr_vs_now_per
        ,a.last_retail_mrc
        ,a.act_accs
        ,a.last_data_allowance
        ,a.wk4_hid_tot_pages
        ,a.SID_COMM_CHAN_EML
        ,a.SID_COMM_CHAN_SMS
        ,a.SID_COMM_TYPE_MARKETING
        ,a.SID_COMM_DD_LC_TYPE_LEGAL
        ,a.SID_COMM_TYPE_SERVICE 
        ,a.SID_COMM_DD_LC_CATEGORY_INFORMING
        ,a.SID_COMM_DD_LC_CATEGORY_XSELL
        ,a.avg_week_wifi_count
        ,a.avg_week_data_kb
        ,a.avg_week_4g_data_kb
        ,a.avg_week_3g_data_kb
        ,a.avg_week_2g_data_kb
        ,a.avg_week_volte_secs
        ,a.avg_week_voice_secs
        ,a.avg_week_sms 
        ,a.hid_we_dist_km
        ,a.sub_wdwe_dist_km
        ,a.sub_wdewe_dist_km
        ,a.number_of_adults
        ,a.wlan_capable
        ,a.pid_avg_days_since_lifetime_start_date
        ,a.pid_avg_rev_items
        ,a.pid_avg_disc_items
        ,a.pid_avg_ovechrg
        ,a.pid_avg_revs
        ,a.pid_avg_discs
        ,a.hid_data_allowance
        ,a.hid_mrc
        ,a.hid_act_accs
        ,a.hid_avg_ovechrg
        ,a.avg_pid_comm_dd_lc_type_legal_hid
        ,a.avg_pid_comm_dd_lc_category_upsell_hid
        ,a.child_0to4
        ,a.child5to11
        ,a.child12to17
        ,a.hid_min_days_since_lifetime_start_date
        ,a.pid_do_alw_1w_sum
        ,a.pid_vo_dur_1m_sum
        ,a.pid_do_alw_3m_sum
        ,a.pid_do_alw_1m_mean
        ,a.last_regular_extra
        ,a.number_of_children
        ,a.month1_donated
        ,a.month1_received
        ,a.STACK
        ,a.TOT_COST_IC
        ,a.TOT_COST_IC_VOICE
        ,a.TOT_COST_IC_SMS
        ,a.TOT_COST_IC_MMS
        ,a.VOL_UPG
        ,a.TOT_REV_PP_RC
        ,a.TOT_REV_PP_DISC
        ,a.TOT_REV_SOC_RC
        ,a.TOT_REV_SOC_DISC
        ,a.TOT_REV_INS_RC
        ,a.TOT_REV_INS_DISC
        ,a.VO_ALLW_DUR_TOT
        ,a.VO_ALLW_DUR_ROAM
        ,a.VO_ALLW_DUR_HOME
        ,a.VO_ALLW_DUR_ONNET
        ,a.VO_ALLW_DUR_OFFNET
        ,a.VO_ALLW_REV_TOT
        ,a.VO_ALLW_REV_ROAM
        ,a.VO_ALLW_REV_HOME
        ,a.VO_ALLW_REV_ONNET
        ,a.VO_ALLW_REV_OFFNET
        ,a.TOT_REV_IC
        ,a.TOT_REV_IC_VOICE
        ,a.TOT_REV_IC_SMS
        ,a.TOT_REV_IC_MMS
        ,a.D_ALLW_VOL_TOT
        ,a.D_ALLW_VOL_ROAM
        ,a.D_ALLW_VOL_HOME
        ,a.ACTIVE_30D_PAYM
        ,a.ANPU
        ,a.D_VOL_TOT
        ,a.DATA_PASS
        ,a.SI_CNT_TOT
        ,a.SO_CNT_TOT
        ,a.TOT_REV_DD_DISC
        ,a.TOT_REV_DD_ONEOFF
        ,a.TOT_REV_ONEOFF
        ,a.VI_CNT_TOT
        ,a.VI_DUR_TOT
        ,a.VO_CNT_TOT
        ,a.VO_DUR_TOT

        -- EE_PAYM_DEV.BI_CCM_BASE, EE_PAYM_SOURCE.DIM_PRODUCT_REF, EE_PAYM_SOURCE.DIM_DEVICE_REF, EE_PAYM_SOURCE.DIM_CHANNEL_REF
        ,b.*

        -- SCCV and timing features
        ,c.total_em_comms_lstwk
        ,c.total_em_comms_lstmnth
        ,c.total_em_comms_to_date
        ,c.total_mms_comms_lstwk
        ,c.total_mms_comms_lstmnth
        ,c.total_mms_comms_to_date
        ,c.total_sms_comms_lstwk
        ,c.total_sms_comms_lstmnth
        ,c.total_sms_comms_to_date
        ,c.total_comms_lstwk
        ,c.total_comms_lstmnth
        ,c.total_comms_to_date
        ,c.total_em_opens_lstwk
        ,c.total_em_opens_lstmnth
        ,c.total_em_opens_to_date
        ,c.total_opens_lstwk
        ,c.total_opens_lstmnth
        ,c.total_opens_to_date
        ,c.total_em_clicks_lstwk
        ,c.total_em_clicks_lstmnth
        ,c.total_em_clicks_to_date
        ,c.total_mms_clicks_lstwk
        ,c.total_mms_clicks_lstmnth
        ,c.total_mms_clicks_to_date
        ,c.total_sms_clicks_lstwk
        ,c.total_sms_clicks_lstmnth
        ,c.total_sms_clicks_to_date
        ,c.total_clicks_lstwk
        ,c.total_clicks_lstmnth
        ,c.total_clicks_to_date
        ,c.campaign_mix_lstwk
        ,c.campaign_mix_lstmnth
        ,c.campaign_mix_to_date
        ,c.channel_lstwk
        ,c.channel_lstmnth
        ,c.channel_to_date
        ,c.brand_lstwk
        ,c.brand_lstmnth
        ,c.brand_to_date
        ,c.days_from_last_EE_comm   
        ,c.days_from_last_open
        ,c.days_from_last_click

    from campaign_data.optout_model_base_features_2a a 

    left join campaign_data.optout_model_base_features_2b b
        on a.ACCOUNT_NUM = b.ACCOUNT_NUM

    left join campaign_data.optout_model_base_features_2c c
        on a.ee_customer_id = c.ee_customer_id        
    '''

    pd.read_sql(sql_code, conn)    

        
    if write_to_df == 'yes':
        print('writing to df')
        return pd.read_sql('''select * from campaign_data.optout_model_base_features_combined''', conn)
