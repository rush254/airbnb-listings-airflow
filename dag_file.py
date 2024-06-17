import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import airflow
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'bde',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='at3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

query_refresh_dim_static_tables = f"""
ALTER EXTERNAL TABLE raw.raw_suburb REFRESH;
ALTER EXTERNAL TABLE raw.raw_lga REFRESH;
ALTER EXTERNAL TABLE raw.raw_census_g01 REFRESH;
ALTER EXTERNAL TABLE raw.raw_census_g02 REFRESH;

CREATE OR REPLACE TABLE staging.staging_suburb as
SELECT 
    value:c1::varchar as lga_name
    , value:c2::varchar as suburb_name
FROM raw.raw_suburb;
INSERT INTO staging.staging_suburb VALUES ('OTHER', 'OTHER');


CREATE OR REPLACE TABLE staging.staging_lga as
SELECT 
    value:c1::int as lga_code
    , value:c2::varchar as lga_name
FROM raw.raw_lga;
INSERT INTO staging.staging_lga VALUES (0, 'Other');


CREATE OR REPLACE TABLE staging.staging_census_g01 as
SELECT 
    value:c1::varchar as LGA_CODE_2016, value:c2::decimal as Tot_P_M, value:c3::decimal as Tot_P_F, value:c4::decimal as Tot_P_P, value:c5::decimal as Age_0_4_yr_M
    , value:c6::decimal as Age_0_4_yr_F, value:c7::decimal as Age_0_4_yr_P, value:c8::decimal as Age_5_14_yr_M, value:c9::decimal as Age_5_14_yr_F
    , value:c10::decimal as Age_5_14_yr_P, value:c11::decimal as Age_15_19_yr_M, value:c12::decimal as Age_15_19_yr_F, value:c13::decimal as Age_15_19_yr_P
    , value:c14::decimal as Age_20_24_yr_M, value:c15::decimal as Age_20_24_yr_F, value:c16::decimal as Age_20_24_yr_P, value:c17::decimal as Age_25_34_yr_M
    , value:c18::decimal as Age_25_34_yr_F, value:c19::decimal as Age_25_34_yr_P, value:c20::decimal as Age_35_44_yr_M, value:c21::decimal as Age_35_44_yr_F
    , value:c22::decimal as Age_35_44_yr_P, value:c23::decimal as Age_45_54_yr_M, value:c24::decimal as Age_45_54_yr_F, value:c25::decimal as Age_45_54_yr_P
    , value:c26::decimal as Age_55_64_yr_M, value:c27::decimal as Age_55_64_yr_F, value:c28::decimal as Age_55_64_yr_P, value:c29::decimal as Age_65_74_yr_M
    , value:c30::decimal as Age_65_74_yr_F, value:c31::decimal as Age_65_74_yr_P, value:c32::decimal as Age_75_84_yr_M, value:c33::decimal as Age_75_84_yr_F
    , value:c34::decimal as Age_75_84_yr_P, value:c35::decimal as Age_85ov_M, value:c36::decimal as Age_85ov_F, value:c37::decimal as Age_85ov_P
    , value:c38::decimal as Counted_Census_Night_home_M, value:c39::decimal as Counted_Census_Night_home_F, value:c40::decimal as Counted_Census_Night_home_P
    , value:c41::decimal as Count_Census_Nt_Ewhere_Aust_M, value:c42::decimal as Count_Census_Nt_Ewhere_Aust_F, value:c43::decimal as Count_Census_Nt_Ewhere_Aust_P
    , value:c44::decimal as Indigenous_psns_Aboriginal_M, value:c45::decimal as Indigenous_psns_Aboriginal_F, value:c46::decimal as Indigenous_psns_Aboriginal_P
    , value:c47::decimal as Indig_psns_Torres_Strait_Is_M, value:c48::decimal as Indig_psns_Torres_Strait_Is_F, value:c49::decimal as Indig_psns_Torres_Strait_Is_P
    , value:c50::decimal as Indig_Bth_Abor_Torres_St_Is_M, value:c51::decimal as Indig_Bth_Abor_Torres_St_Is_F, value:c52::decimal as Indig_Bth_Abor_Torres_St_Is_P
    , value:c53::decimal as Indigenous_P_Tot_M, value:c54::decimal as Indigenous_P_Tot_F, value:c55::decimal as Indigenous_P_Tot_P
    , value:c56::decimal as Birthplace_Australia_M, value:c57::decimal as Birthplace_Australia_F, value:c58::decimal as Birthplace_Australia_P
    , value:c59::decimal as Birthplace_Elsewhere_M, value:c60::decimal as Birthplace_Elsewhere_F, value:c61::decimal as Birthplace_Elsewhere_P
    , value:c62::decimal as Lang_spoken_home_Eng_only_M, value:c63::decimal as Lang_spoken_home_Eng_only_F, value:c64::decimal as Lang_spoken_home_Eng_only_P
    , value:c65::decimal as Lang_spoken_home_Oth_Lang_M, value:c66::decimal as Lang_spoken_home_Oth_Lang_F, value:c67::decimal as Lang_spoken_home_Oth_Lang_P
    , value:c68::decimal as Australian_citizen_M, value:c69::decimal as Australian_citizen_F, value:c70::decimal as Australian_citizen_P
    , value:c71::decimal as Age_psns_att_educ_inst_0_4_M, value:c72::decimal as Age_psns_att_educ_inst_0_4_F, value:c73::decimal as Age_psns_att_educ_inst_0_4_P
    , value:c74::decimal as Age_psns_att_educ_inst_5_14_M, value:c75::decimal as Age_psns_att_educ_inst_5_14_F, value:c76::decimal as Age_psns_att_educ_inst_5_14_P
    , value:c77::decimal as Age_psns_att_edu_inst_15_19_M, value:c78::decimal as Age_psns_att_edu_inst_15_19_F, value:c79::decimal as Age_psns_att_edu_inst_15_19_P
    , value:c80::decimal as Age_psns_att_edu_inst_20_24_M, value:c81::decimal as Age_psns_att_edu_inst_20_24_F, value:c82::decimal as Age_psns_att_edu_inst_20_24_P
    , value:c83::decimal as Age_psns_att_edu_inst_25_ov_M, value:c84::decimal as Age_psns_att_edu_inst_25_ov_F, value:c85::decimal as Age_psns_att_edu_inst_25_ov_P
    , value:c86::decimal as High_yr_schl_comp_Yr_12_eq_M, value:c87::decimal as High_yr_schl_comp_Yr_12_eq_F, value:c88::decimal as High_yr_schl_comp_Yr_12_eq_P
    , value:c89::decimal as High_yr_schl_comp_Yr_11_eq_M, value:c90::decimal as High_yr_schl_comp_Yr_11_eq_F, value:c91::decimal as High_yr_schl_comp_Yr_11_eq_P
    , value:c92::decimal as High_yr_schl_comp_Yr_10_eq_M, value:c93::decimal as High_yr_schl_comp_Yr_10_eq_F, value:c94::decimal as High_yr_schl_comp_Yr_10_eq_P
    , value:c95::decimal as High_yr_schl_comp_Yr_9_eq_M, value:c96::decimal as High_yr_schl_comp_Yr_9_eq_F, value:c97::decimal as High_yr_schl_comp_Yr_9_eq_P
    , value:c98::decimal as High_yr_schl_comp_Yr_8_belw_M, value:c99::decimal as High_yr_schl_comp_Yr_8_belw_F, value:c100::decimal as High_yr_schl_comp_Yr_8_belw_P
    , value:c101::decimal as High_yr_schl_comp_D_n_g_sch_M, value:c102::decimal as High_yr_schl_comp_D_n_g_sch_F, value:c103::decimal as High_yr_schl_comp_D_n_g_sch_P
    , value:c104::decimal as Count_psns_occ_priv_dwgs_M, value:c105::decimal as Count_psns_occ_priv_dwgs_F, value:c106::decimal as Count_psns_occ_priv_dwgs_P
    , value:c107::decimal as Count_Persons_other_dwgs_M, value:c108::decimal as Count_Persons_other_dwgs_F, value:c109::decimal as Count_Persons_other_dwgs_P
FROM raw.raw_census_g01;

CREATE OR REPLACE TABLE staging.staging_census_g02 as
SELECT 
    value:c1::varchar as LGA_CODE_2016
    , value:c2::decimal as Median_age_persons
    , value:c3::decimal as Median_mortgage_repay_monthly
    , value:c4::decimal as Median_tot_prsnl_inc_weekly
    , value:c5::decimal as Median_rent_weekly	
    , value:c6::decimal as Median_tot_fam_inc_weekly	
    , value:c7::decimal as Average_num_psns_per_bedroom	
    , value:c8::decimal as Median_tot_hhd_inc_weekly	
    , value:c9::decimal as Average_household_size
FROM raw.raw_census_g02;



CREATE OR REPLACE TABLE datawarehouse.dim_suburb as
SELECT *
FROM staging.staging_suburb;

CREATE OR REPLACE TABLE datawarehouse.dim_lga as
SELECT *
FROM staging.staging_lga;

CREATE OR REPLACE TABLE datawarehouse.dim_census_g01 as
SELECT cast(right(LGA_CODE_2016, 5) as int) as lga_code, *
FROM staging.staging_census_g01;

CREATE OR REPLACE TABLE datawarehouse.dim_census_g02 as
SELECT cast(right(LGA_CODE_2016, 5) as int) as lga_code, *
FROM staging.staging_census_g02;
"""



query_refresh_airbnb_tables = f"""
ALTER EXTERNAL TABLE raw.raw_listings REFRESH;

CREATE OR REPLACE TABLE staging.staging_listings as
SELECT 
    value:c1::int as listing_id
    , value:c2::int as scrape_id
    , value:c3::date as scraped_date
    , value:c4::int as host_id
    , value:c5::varchar as host_name
    , value:c6::varchar as host_since
    , value:c7::boolean as host_is_superhost
    , value:c8::varchar as host_neighbourhood
    , value:c9::varchar as listing_neighbourhood
    , value:c10::varchar as property_type
    , value:c11::varchar as room_type
    , value:c12::int as accommodates
    , value:c13::int as price
    , value:c14::boolean as has_availability
    , value:c15::int as avalability_30
    , value:c16::int as number_of_reviews
    , value:c17::int as review_scores_rating
    , value:c18::int as review_scores_accuracy
    , value:c19::int as review_scores_cleanliness
    , value:c20::int as review_scores_checkin
    , value:c21::int as review_scores_communication
    , value:c22::int as review_scores_value
    , split_part(split_part(metadata$filename, '/', -1)::varchar,'.',1)::varchar as month_year
FROM raw.raw_listings;



CREATE OR REPLACE TABLE datawarehouse.fact as
SELECT 
    listing_id
    , scrape_id
    , scraped_date
    , hash(concat(property_type,room_type,accommodates)) as listing_key
    , host_id
    , case when host_is_superhost = TRUE then 1 else 0 end as host_is_superhost
    , b.lga_code as listing_lga_code
    , coalesce(d.lga_code, 0) as host_lga_code
    , price
    , case when has_availability = TRUE then 1 else 0 end as has_availability
    , avalability_30
    , number_of_reviews
    , review_scores_rating
    , review_scores_accuracy
    , review_scores_cleanliness
    , review_scores_checkin
    , review_scores_communication
    , review_scores_value
    , to_date(REPLACE(month_year,'_','/'), 'mm/yyyy') as month_year
FROM 
    (SELECT *
    FROM staging.staging_listings) a
LEFT JOIN 
    (SELECT * FROM staging.staging_lga) b 
ON a.listing_neighbourhood = b.lga_name
LEFT JOIN 
    (SELECT * FROM staging.staging_suburb) c 
ON a.host_neighbourhood = initcap(c.suburb_name)
LEFT JOIN 
    (SELECT * FROM staging.staging_lga) d 
ON initcap(c.lga_name) = d.lga_name;


CREATE OR REPLACE TABLE datawarehouse.dim_listing as
SELECT 
    distinct(hash(concat(property_type,room_type,accommodates))) as listing_key
    , property_type
    , room_type
    , accommodates 
FROM staging.staging_listings;


CREATE OR REPLACE TABLE datawarehouse.dim_host as
SELECT 
    distinct(a.host_id)
    , host_name
    , host_since
    , host_is_superhost
    , coalesce(host_neighbourhood, 'Other') as host_neighbourhood
FROM 
    (SELECT 
        host_id
        , max(scraped_date) as scraped_date 
     FROM staging.staging_listings 
     GROUP BY host_id) a 
LEFT JOIN 
    (SELECT 
        host_id
        , scraped_date
        , host_name
        , to_date(host_since, 'dd/mm/yyyy') as host_since
        , host_is_superhost
        , host_neighbourhood FROM staging.staging_listings) b 
ON a.scraped_date = b.scraped_date AND a.host_id = b.host_id;


"""



query_refresh_datamart = f"""
CREATE OR REPLACE TABLE datamart.dm_listing_neighbourhood as
SELECT
    lga_name as listing_neighbourhood
    , a.month_year
    , active_listings_rate
    , max_price
    , min_price
    , median_price
    , avg_price
    , distinct_hosts
    , distinct_superhosts/distinct_hosts * 100 as superhost_rate
    , avg_review_score_rating
    , (active_listings - active_listings_prev_month)/nullif(active_listings_prev_month, 0) * 100 as percent_change_active_listings
    , (inactive_listings - inactive_listings_prev_month)/nullif(inactive_listings_prev_month, 0) * 100 as percent_change_inactive_listings
    , total_stays
    , avg_revenue_per_active_listing
FROM 
    (SELECT 
        listing_lga_code 
        , month_year
        , sum(has_availability)/count(has_availability) * 100 as active_listings_rate
        , count(distinct host_id) as distinct_hosts
        , sum(has_availability) as active_listings
        , count(*) - sum(has_availability) as inactive_listings
    FROM datawarehouse.fact
    GROUP BY listing_lga_code, month_year) a
LEFT JOIN 
    (SELECT
        listing_lga_code 
        , month_year
        , max(price) as max_price
        , min(price) as min_price
        , median(price) as median_price
        , avg(price) as avg_price
        , avg(review_scores_rating) as avg_review_score_rating
        , sum(30 - avalability_30) as total_stays
        , avg(price*(30 - avalability_30)) as avg_revenue_per_active_listing
    FROM datawarehouse.fact
    WHERE has_availability = 1
    GROUP BY listing_lga_code, month_year) b
ON a.listing_lga_code = b.listing_lga_code AND a.month_year = b.month_year
LEFT JOIN 
    (SELECT
        listing_lga_code 
        , month_year
        , count(distinct host_id) as distinct_superhosts
    FROM datawarehouse.fact
    WHERE has_availability = 1 AND host_is_superhost = 1 
    GROUP BY listing_lga_code, month_year) c
ON a.listing_lga_code = c.listing_lga_code AND a.month_year = c.month_year
LEFT JOIN 
    (SELECT
        listing_lga_code 
        , month_year
        , sum(has_availability) as active_listings_prev_month
        , count(*) - sum(has_availability) as inactive_listings_prev_month
    FROM datawarehouse.fact
    GROUP BY listing_lga_code, month_year) d
ON a.listing_lga_code = d.listing_lga_code AND a.month_year = dateadd(month, 1, d.month_year)
LEFT JOIN
    (SELECT *
    FROM datawarehouse.dim_lga) e
ON a.listing_lga_code = e.lga_code
ORDER BY listing_neighbourhood, month_year;



CREATE OR REPLACE TABLE datamart.dm_property_type as
SELECT
    property_type
    , room_type
    , accommodates
    , a.month_year
    , active_listings_rate
    , max_price
    , min_price
    , median_price
    , avg_price
    , distinct_hosts
    , distinct_superhosts/distinct_hosts * 100 as superhost_rate
    , avg_review_score_rating
    , (active_listings - active_listings_prev_month)/nullif(active_listings_prev_month, 0) * 100 as percent_change_active_listings
    , (inactive_listings - inactive_listings_prev_month)/nullif(inactive_listings_prev_month, 0) * 100 as percent_change_inactive_listings
    , total_stays
    , avg_revenue_per_active_listing
FROM 
    (SELECT 
        listing_key
        , month_year
        , sum(has_availability)/count(has_availability) * 100 as active_listings_rate
        , count(distinct host_id) as distinct_hosts
        , sum(has_availability) as active_listings
        , count(*) - sum(has_availability) as inactive_listings
FROM datawarehouse.fact
GROUP BY listing_key, month_year) a
LEFT JOIN 
    (SELECT
        listing_key
        , month_year
        , max(price) as max_price
        , min(price) as min_price
        , median(price) as median_price
        , avg(price) as avg_price
        , avg(review_scores_rating) as avg_review_score_rating
        , sum(30 - avalability_30) as total_stays
        , avg(price*(30 - avalability_30)) as avg_revenue_per_active_listing
    FROM datawarehouse.fact
    WHERE has_availability = 1
    GROUP BY listing_key, month_year) b
ON a.listing_key = b.listing_key AND a.month_year = b.month_year
LEFT JOIN 
    (SELECT
        listing_key
        , month_year
        , count(distinct host_id) as distinct_superhosts
    FROM datawarehouse.fact
    WHERE has_availability = 1 AND host_is_superhost = 1 
    GROUP BY listing_key, month_year) c
ON a.listing_key = c.listing_key AND a.month_year = c.month_year
LEFT JOIN 
    (SELECT
        listing_key
        , month_year
        , sum(has_availability) as active_listings_prev_month
        , count(*) - sum(has_availability) as inactive_listings_prev_month
    FROM datawarehouse.fact
    GROUP BY listing_key, month_year) d
ON a.listing_key = d.listing_key AND a.month_year = d.month_year
LEFT JOIN 
    (SELECT *
    FROM datawarehouse.dim_listing) e
ON a.listing_key = e.listing_key
ORDER BY property_type, room_type, accommodates, month_year;


CREATE OR REPLACE TABLE datamart.dm_host_neighbourhood as
SELECT
    lga_name as host_neighbourhood_lga
    , a.month_year
    , distinct_hosts
    , est_revenue
    , est_revenue/distinct_hosts as est_revenue_per_host
FROM
    (SELECT 
        host_lga_code
        , month_year
        , count(distinct host_id) as distinct_hosts
    FROM datawarehouse.fact
    GROUP BY host_lga_code, month_year) a
LEFT JOIN
    (SELECT
        host_lga_code
        , month_year
        , sum(price*(30 - avalability_30)) as est_revenue
    FROM datawarehouse.fact
    WHERE has_availability = 1
    GROUP BY host_lga_code, month_year) b
ON a.host_lga_code = b.host_lga_code AND a.month_year = b.month_year
LEFT JOIN
    (SELECT *
    FROM datawarehouse.dim_lga) c
ON a.host_lga_code = c.lga_code
ORDER BY host_neighbourhood_lga, month_year;


"""

#########################################################
#
#   DAG Operator Setup
#
#########################################################


refresh_dim_static_tables = SnowflakeOperator(
    task_id='refresh_dim_static_tables',
    sql=query_refresh_dim_static_tables,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_airbnb_tables = SnowflakeOperator(
    task_id='refresh_airbnb_tables',
    sql=query_refresh_airbnb_tables,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_datamart = SnowflakeOperator(
    task_id='refresh_datamart_task',
    sql=query_refresh_datamart,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_dim_static_tables >> refresh_airbnb_tables >> refresh_datamart


