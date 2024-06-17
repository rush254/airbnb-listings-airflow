--3A
SELECT
    listing_neighbourhood
    , revenue_per_listing
    , age_u35
    , age_u25
FROM
    (SELECT 
        listing_neighbourhood
        , sum(avg_revenue_per_active_listing) as revenue_per_listing
        , row_number()over(order by sum(avg_revenue_per_active_listing) asc) as rownum_asc
        , row_number()over(order by sum(avg_revenue_per_active_listing) desc) as rownum_desc
    FROM datamart.dm_listing_neighbourhood
    GROUP BY listing_neighbourhood) a
LEFT JOIN 
    (SELECT *
    FROM datawarehouse.dim_lga) b
ON a.listing_neighbourhood = b.lga_name
LEFT JOIN
    (SELECT 
        (age_0_4_yr_p + age_5_14_yr_p + age_15_19_yr_p + age_20_24_yr_p + age_25_34_yr_p) as age_u35
        , (age_0_4_yr_p + age_5_14_yr_p + age_15_19_yr_p + age_20_24_yr_p) as age_u25
        , lga_code
    FROM datawarehouse.dim_census_g01) c
ON b.lga_code = c.lga_code
WHERE rownum_asc = 1 OR rownum_desc = 1;

--3B
SELECT
    a.listing_neighbourhood
    , revenue_per_active_listing
    , property_type
    , room_type
    , accommodates
    , total_stays
FROM
    (SELECT 
        listing_neighbourhood
        , sum(avg_revenue_per_active_listing) as revenue_per_active_listing
        , row_number()over(order by sum(avg_revenue_per_active_listing) desc) as rownum_desc
    FROM datamart.dm_listing_neighbourhood
    GROUP BY listing_neighbourhood) a
LEFT JOIN
    (SELECT 
        lga_name as listing_neighbourhood
        , property_type
        , room_type
        , accommodates
        , total_stays
    FROM
        (SELECT
            listing_lga_code
            , listing_key
            , sum(30 - avalability_30) as total_stays
            , row_number()over(partition by listing_lga_code order by total_stays desc) as rownum_listing
        FROM datawarehouse.fact
        WHERE has_availability = 1
        GROUP BY listing_lga_code, listing_key) c1
        LEFT JOIN 
            (SELECT *
            FROM datawarehouse.dim_listing) c2
        ON c1.listing_key = c2.listing_key
        LEFT JOIN 
            (SELECT *
            FROM datawarehouse.dim_lga) c3
        ON c1.listing_lga_code = c3.lga_code 
        WHERE rownum_listing = 1) c
ON a.listing_neighbourhood = c.listing_neighbourhood
WHERE rownum_desc <= 5;


--3C
SELECT
    count(case when count_same_lga = 0 then 1 else null end) as only_different_lga
    , count(case when count_different_lga = 0 then 1 else null end) as only_same_lga
    , count(case when same_lga_greater_than_different = 1 then 1 else null end) as total_same_greater_different
    , count(case when same_lga_greater_than_different = 0 then 1 else null end) as total_different_greater_same
FROM
    (SELECT 
        a.host_id
        , listing_count
        , count_same_lga
        , count_different_lga
        , case when count_same_lga > count_different_lga then 1 else 0 end as same_lga_greater_than_different
    FROM
        (SELECT
            host_id
            , count(distinct listing_id) as listing_count
        FROM datawarehouse.fact
        GROUP BY host_id) a
    LEFT JOIN 
        (SELECT 
            host_id
            , count(case when same_lga = 1 then 1 else null end) as count_same_lga
            , count(case when same_lga = 0 then 1 else null end) as count_different_lga
        FROM
            (SELECT
                distinct(host_id) as host_id
                , listing_id
                , case when host_lga_code = listing_lga_code then 1 else 0 end as same_lga
            FROM datawarehouse.fact)
            GROUP BY host_id) b
    ON a.host_id = b.host_id
    WHERE listing_count > 1);


--3D
SELECT 
    count(case when revenue_greater_than_mortgage = 1 then 1 else null end) as ct_revenue_greater_mortgage
    , count(case when revenue_greater_than_mortgage = 0 then 1 else null end) as ct_revenue_less_mortgage
    , round(avg(case when revenue_greater_than_mortgage = 1 then revenue_per_month - median_mortgage_repay_monthly else null end),2) as avg_rev_greater_mortgage
    , round(avg(case when revenue_greater_than_mortgage = 0 then revenue_per_month - median_mortgage_repay_monthly else null end),2) as avg_rev_less_mortgage
FROM
    (SELECT 
        a.host_id
        , listing_lga_code
        , sum_revenue_per_active_listing/12 as revenue_per_month
        , median_mortgage_repay_monthly
        , case when revenue_per_month >= median_mortgage_repay_monthly then 1 else 0 end as revenue_greater_than_mortgage
    FROM
        (SELECT
            host_id
            , listing_lga_code
            , count(distinct listing_id) as listing_count
        FROM datawarehouse.fact
        GROUP BY host_id, listing_lga_code) a
    LEFT JOIN
        (SELECT
            host_id
            , sum(price*(30 - avalability_30)) as sum_revenue_per_active_listing
        FROM datawarehouse.fact
        WHERE has_availability = 1
        GROUP BY host_id) b
    ON a.host_id = b.host_id
    LEFT JOIN
        (SELECT lga_code, Median_mortgage_repay_monthly
        FROM datawarehouse.dim_census_g02) c
    ON a.listing_lga_code = c.lga_code
    WHERE listing_count = 1);
