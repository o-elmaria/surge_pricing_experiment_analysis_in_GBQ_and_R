-- V2 contains variant instead of experiment_variant
-- This is the complete data extraction script for AB tests with target groups launched through the DPS UI 

-- Step 1: Declare the input variables used throughout the script
DECLARE test_name_var ARRAY <STRING>;
DECLARE exp_id_central_pre_post INT64;
DECLARE exp_id_tsw_pre INT64;
DECLARE exp_id_tsw_post INT64;
DECLARE entity, v_type, od_status STRING;
DECLARE parent_vertical ARRAY <STRING>;
DECLARE variants ARRAY <STRING>;
DECLARE exp_target_groups ARRAY <STRING>;
DECLARE vendor_group_id_var ARRAY <INT64>; -- These filters are NOT used
DECLARE start_date, end_date DATE;
DECLARE surge_mov_variant_central_pre_post ARRAY <STRING>;
DECLARE surge_mov_variant_tsw_pre_post ARRAY <STRING>;
DECLARE target_group_variant_scheme_id_valid_combos_central_pre_post ARRAY <STRING>;
DECLARE target_group_variant_scheme_id_valid_combos_tsw_pre ARRAY <STRING>;
DECLARE target_group_variant_scheme_id_valid_combos_tsw_post ARRAY <STRING>;
SET test_name_var = ['HK_20220404_R_EF_P_SurgeMOVCentralPrePost', 'HK_20220404_R_EF_P_ SurgeMOVTinShuiWaiPre', 'HK_20220404_R_EF_P_ SurgeMOVTinShuiWaiPost'];
SET exp_id_central_pre_post = 15;
SET exp_id_tsw_pre = 16;
SET exp_id_tsw_post = 17;
SET (entity, v_type, od_status) = ('FP_HK', 'restaurants', 'OWN_DELIVERY'); -- Name of the test in the AB test dashboard/DPS experiments tab
SET parent_vertical = ['restaurant', 'restaurants', 'Restaurant']; -- Different parent vertical names depending on the platform. APAC is 'Restaurant'
SET variants = ['Variation1', 'Variation2', 'Variation3', 'Variation4', 'Variation5', 'Variation6', 'Variation7', 'Variation8', 'Variation9', 'Control'];
SET exp_target_groups = ['TG1', 'TG2'];
SET vendor_group_id_var = [402, 403]; -- These filters are NOT used. You can check for these IDs by running this query --> SELECT DISTINCT vendor_group_id FROM `fulfillment-dwh-production.cl.dps_experiment_setups` WHERE test_name = 'MY_20220218_R_B0_P_LovedBrandsKlangValley'`
SET (start_date, end_date) = (DATE('2022-04-05'), DATE('2022-05-05')); -- Encompasses the entire duration of the hybrid test 
SET surge_mov_variant_central_pre_post = ['Variation9'];
SET surge_mov_variant_tsw_pre_post = [];
SET target_group_variant_scheme_id_valid_combos_central_pre_post = ['TG1 | Control | 2724', 'TG1 | Variation1 | 2724', 'TG1 | Variation2 | 2724', 'TG1 | Variation3 | 2724', 'TG1 | Variation4 | 2724', 'TG1 | Variation5 | 2724', 'TG1 | Variation6 | 2724', 'TG1 | Variation7 | 2724', 'TG1 | Variation8 | 2724', 'TG1 | Variation9 | 2725',
                                                                    'TG2 | Control | 2746', 'TG2 | Variation1 | 2746', 'TG2 | Variation2 | 2746', 'TG2 | Variation3 | 2746', 'TG2 | Variation4 | 2746', 'TG2 | Variation5 | 2746', 'TG2 | Variation6 | 2746', 'TG2 | Variation7 | 2746', 'TG2 | Variation8 | 2746', 'TG2 | Variation9 | 2745'];

SET target_group_variant_scheme_id_valid_combos_tsw_pre = ['TG1 | Control | 2726', 'TG1 | Variation1 | 2726', 'TG1 | Variation2 | 2726', 'TG1 | Variation3 | 2726', 'TG1 | Variation4 | 2726', 'TG1 | Variation5 | 2726', 'TG1 | Variation6 | 2726', 'TG1 | Variation7 | 2726', 'TG1 | Variation8 | 2726', 'TG1 | Variation9 | 2726',
                                                           'TG2 | Control | 2747', 'TG2 | Variation1 | 2747', 'TG2 | Variation2 | 2747', 'TG2 | Variation3 | 2747', 'TG2 | Variation4 | 2747', 'TG2 | Variation5 | 2747', 'TG2 | Variation6 | 2747', 'TG2 | Variation7 | 2747', 'TG2 | Variation8 | 2747', 'TG2 | Variation9 | 2747'];

SET target_group_variant_scheme_id_valid_combos_tsw_post = ['TG1 | Control | 2727', 'TG1 | Variation1 | 2727', 'TG1 | Variation2 | 2727', 'TG1 | Variation3 | 2727', 'TG1 | Variation4 | 2727', 'TG1 | Variation5 | 2727', 'TG1 | Variation6 | 2727', 'TG1 | Variation7 | 2727', 'TG1 | Variation8 | 2727', 'TG1 | Variation9 | 2727',
                                                            'TG2 | Control | 2748', 'TG2 | Variation1 | 2748', 'TG2 | Variation2 | 2748', 'TG2 | Variation3 | 2748', 'TG2 | Variation4 | 2748', 'TG2 | Variation5 | 2748', 'TG2 | Variation6 | 2748', 'TG2 | Variation7 | 2748', 'TG2 | Variation8 | 2748', 'TG2 | Variation9 | 2748'];
/*
Notes to self:
- There are so many **Non_TG sessions in the cvr_data table** (contrary to the orders table) due to overlapping zones. Some Non_TG vendors in the dps_cvr_events table are associated with multiple zones (some of them could be our targeted ones).
However, in the orders table, they are tagged with completely different zones than the targeted ones because an order can only have one zone (that's BI's logic). This causes these orders to NOT be caught and the number of Non_TG **orders** to be so little, contrary
to the number of Non_TG **sessions**

- You can fix the **Non_TG sessions issue** by filtering out the sessions with Non_TG (i.e., not analysing them at all) OR filtering for the targeted zones by the **ST_CONTAINS** instead of **zone_id**. This applies to the sessions_mapped_to_orders_v2 table
as well as dps_cvr_events. However, these two tables do NOT have the location field. You will need to get it from cDWH.orders AND dps_sessions_mapped_to_ga_sessions
*/

----------------------------------------------------------------END OF THE INPUT SECTION----------------------------------------------------------------

-- Step 2: Extract the vendor IDs per target group
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_hk_surge_mov` AS
SELECT DISTINCT
    entity_id,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    CONCAT('TG', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
WHERE TRUE
    AND test_name IN UNNEST(test_name_var)
ORDER BY 1,2,3,4;

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_zone_ids_hk_surge_mov` AS
SELECT DISTINCT
    entity_id,
    test_name,
    test_id,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
WHERE TRUE
    AND test_name IN UNNEST(test_name_var) 
ORDER BY 1,2;

-- Step 4: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_hk_surge_mov` AS
SELECT 
    p.entity_id,
    country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
    tgt.test_name,
    tgt.test_id
FROM fulfillment-dwh-production.cl.countries co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_hk_surge_mov` tgt ON p.entity_id = tgt.entity_id AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

-- Step 5: Pull the business and logisitcal KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_hk_surge_mov` AS
WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
),

entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.tg_name, 'Non_TG') AS target_group,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    a.vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (pick the ones that are applicable to your test)
    a.delivery_fee_local, -- The delivery fee amount of the dps session.
    a.dps_travel_time_fee_local, -- The (dps_delivery_fee - dps_surge_fee) of the dps session.
    a.dps_minimum_order_value_local AS mov_local, -- The minimum order value of the dps session.
    a.dps_surge_fee_local, -- The surge fee amount of the session.
    a.dps_delivery_fee_local,
    a.service_fee AS service_fee_local, -- The service fee amount of the session.
    a.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    a.gfv_local, -- The gfv (gross food value) of the order placed from backend
    a.standard_fee, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
    a.commission_local,
    a.commission_base_local,
    a.joker_vendor_fee_local,
    ROUND(a.commission_local / NULLIF(a.commission_base_local, 0), 4) AS commission_rate,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    cst.delivery_costs_local,
    
    -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) AS revenue_eur,

    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) - cst.delivery_costs_local / a.exchange_rate AS gross_profit_eur,

    -- Logistics KPIs
    a.dps_mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.dps_mean_delay_zone_id, 
    a.dps_travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT,

    -- Centra DWH fields	
    dwh.value.delivery_fee_local AS delivery_fee_local_cdwh,	
    dwh.value.delivery_fee_vat_local AS delivery_fee_vat_local_cdwh,
    dwh.value.voucher_dh_local AS voucher_dh_local_cdwh,	
    dwh.value.voucher_other_local AS voucher_other_local_cdwh,	
    dwh.value.discount_dh_local AS discount_dh_local_cdwh,	
    dwh.value.discount_other_local AS discount_other_local_cdwh,	
    dwh.value.joker_customer_discount_local AS joker_customer_discount_local_cdwh,
    dwh.value.joker_vendor_fee_local AS joker_vendor_fee_local_cdwh,
    dwh.is_joker,
    dwh.value.gbv_local AS gfv_local_cdwh,
    dwh.value.customer_paid_local AS customer_paid_local_cdwh,
    dwh.value.mov_local AS mov_local_cdwh,
    dwh.value.mov_customer_fee_local AS sof_cdwh,
    dwh.payment_method,
    dwh.payment_type,

    -- Pandata fields
    pd.service_fee_total_local AS service_fee_total_local_pd,
    pd.container_price_local AS container_price_local_pd,
    pd.delivery_fee_local AS delivery_fee_local_pd,
    pd.delivery_fee_forced_local AS delivery_fee_forced_local_pd,
    pd.delivery_fee_original_local AS delivery_fee_original_local_pd,
    pd.delivery_fee_vat_rate AS delivery_fee_vat_rate_pd,	
    pd.product_vat_groups AS product_vat_groups_pd,	
    pd.vat_rate AS vat_rate_pd,
    pd.delivery_fee_vat_local AS delivery_fee_vat_local_pd,	
    pd.products_vat_amount_local AS products_vat_amount_local_pd,
    pd.vat_amount_local AS vat_amount_local_pd,

    -- Special fields
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
        )
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END) -- If the order comes from a non-Pandora country, use delivery_fee_local
    END AS actual_df_paid_by_customer,
    a.is_delivery_fee_covered_by_discount,
    a.is_delivery_fee_covered_by_voucher,
    CASE WHEN is_delivery_fee_covered_by_discount = FALSE AND is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher' ELSE 'DF Voucher' END AS df_voucher_flag,
    CASE WHEN a.dps_minimum_order_value_local > 60 OR a.dps_surge_fee_local != 0 THEN 'surge_event' ELSE 'non_surge_event' END AS surge_event_flag,
    CASE WHEN pdos.is_free_delivery_subscription_order = TRUE THEN 'Subscription FD Order' ELSE 'Non-Subscription FD Order' END AS fd_subscription_flag, -- Only two possible values --> True or False
    pd.minimum_delivery_value_local AS mov_pd
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_sb_subscriptions` pdos ON pd.uuid = pdos.uuid AND pd.created_date_utc = pdos.created_date_utc
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
-- IMPORTANT NOTE: CHECK WHETHER YOU NEED INNER JOIN OR LEFT JOIN (In the case of LBs, we want INNER JOIN for the zones, but LEFT JOIN for the vendors, as we compare variants for LBs AND all vendors)
INNER JOIN `dh-logistics-product-ops.pricing.ds_city_eligibility_entities` ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
-- IMPORTANT NOTE: To filter for orders coming from the target zones, you can either INNER JOIN on `dh-logistics-product-ops.pricing.ab_test_zone_ids_hk_surge_mov` **OR** LEFT JOIN on  
-- INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_hk_surge_mov` zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id -- Filter for orders from vendors in the target zones `dh-logistics-product-ops.pricing.ab_test_geo_data_hk_surge_mov` AND add a delivery location condition to the WHERE clause
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_hk_surge_mov` zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id AND a.experiment_id = zn.test_id -- NEW
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_hk_surge_mov` tg ON a.entity_id = tg.entity_id AND a.vendor_id = tg.vendor_code AND a.experiment_id = tg.test_id -- Tag the vendors with their target group association
WHERE TRUE
    AND a.entity_id = entity
    AND a.created_date BETWEEN start_date AND end_date
    AND a.variant IN UNNEST(variants)
    AND a.is_own_delivery -- OD or MP
    AND a.vertical_type = v_type -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
    AND a.delivery_status = 'completed' -- Successful orders
    AND experiment_id IN (exp_id_central_pre_post, exp_id_tsw_pre, exp_id_tsw_post) -- Filter for the right experiment
    AND vendor_vertical_parent IN UNNEST(parent_vertical) -- Necessary filter in the case of parallel experiments
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

----------------------------------------------------------------END OF RAW ORDERS PART----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_hk_surge_mov` AS
SELECT -- TG1, TG2, Non_TG
    *,
    CASE
        -- Central 
        WHEN experiment_id = exp_id_central_pre_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_central_pre_post) AND mov_local > 60 THEN 'Drop' -- The MOV of all variants except Variation9 should be 60 
        -- TSW Pre
        WHEN experiment_id = exp_id_tsw_pre AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_local > 60 THEN 'Drop' -- The MOV of all variants should be 60
        -- TSW Post
        WHEN experiment_id = exp_id_tsw_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_local = 60 THEN 'Drop' -- The MOV of all variants should be > 60
    ELSE 'Keep' END AS drop_keep_flag -- NOTE: This is not a collectively exhaustive condition
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_hk_surge_mov`
WHERE TRUE
    AND ((target_group IN UNNEST(exp_target_groups) AND vendor_price_scheme_type = 'Experiment') OR (target_group = 'Non_TG')) -- Most of the invalid orders get filtered out with this filter
    AND (
        CASE
            -- Central 
            WHEN experiment_id = exp_id_central_pre_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_central_pre_post) AND mov_local > 60 THEN 'Drop' -- The MOV of all variants except Variation9 should be 60 
            -- TSW Pre
            WHEN experiment_id = exp_id_tsw_pre AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_local > 60 THEN 'Drop' -- The MOV of all variants should be 60
            -- TSW Post
            WHEN experiment_id = exp_id_tsw_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_local = 60 THEN 'Drop' -- The MOV of all variants should be > 60
        ELSE 'Keep' END
    ) = 'Keep'; -- NOTE: This is not a collectively exhaustive condition

----------------------------------------------------------------END OF BUSINESS KPIs PART----------------------------------------------------------------

-- Step 5: Pull all the vendor IDs in the targeted zone(s) and vertical
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_vendors_in_target_zones_hk_surge_mov` AS
SELECT DISTINCT
    v.entity_id,
    v.vendor_code
FROM `fulfillment-dwh-production.cl.vendors_v2` v
LEFT JOIN UNNEST(v.vendor) ven
LEFT JOIN UNNEST(v.dps) dps
LEFT JOIN UNNEST(v.hurrier) hur
LEFT JOIN UNNEST(v.zones) z
CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_hk_surge_mov` cd ON v.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, v.location) -- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the chosen city
WHERE TRUE 
    AND v.entity_id = entity -- Entity ID
    AND v.is_active -- Active vendors
    AND v.vertical_type = v_type -- Vertical type
    AND delivery_type = od_status; -- Filter for OD vendors

-- Step 6: Pull CVR data from dps_cvr_events (OVERALL)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_hk_surge_mov` AS
WITH dps_logs_stg_1 AS ( -- Will be used to get the the MOV and DF values of the session
    SELECT DISTINCT
        logs.entity_id,
        logs.created_date,
        endpoint,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        ex.id AS experiment_id,
        ex.variant,
        v.id AS vendor_code,
        v.meta_data.vendor_price_scheme_type,
        v.meta_data.scheme_id,
        v.delivery_fee.travel_time AS DF_tt,
        v.delivery_fee.fleet_utilisation AS DF_surge,
        v.delivery_fee.total AS DF_total,
        v.minimum_order_value.travel_time AS mov_tt,
        v.minimum_order_value.fleet_utilisation AS mov_surge,
        v.minimum_order_value.total AS mov_total,
        v.vertical_parent,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` logs
LEFT JOIN UNNEST(vendors) v
LEFT JOIN UNNEST(customer.experiments) ex
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_hk_surge_mov` cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) AND ex.id = cd.test_id -- Filter for sessions in the city specified above
WHERE TRUE -- No need for an endpoint filter here as we are not using dps_sessions_mapped_to_ga_sessions
    AND logs.entity_id = entity
    AND logs.created_date BETWEEN start_date AND end_date
    AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
    AND ex.variant IN UNNEST(variants) -- Filter for the variants that are part of the test.
    AND v.id IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.ab_test_vendors_in_target_zones_hk_surge_mov`) -- Filter for relevant DPS sessions ONLY (i.e., those that belong to vendor IDs that were selected in the sub-query above)
    AND ex.id IN (exp_id_central_pre_post, exp_id_tsw_pre, exp_id_tsw_post) -- Filter for the right experiment
    AND v.vertical_parent IN UNNEST(parent_vertical) -- Necessary filter in the case of parallel experiments
),

dps_logs_stg_2 AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entity_id, experiment_id, dps_session_id, vendor_code ORDER BY created_at DESC) AS row_num_dps_logs -- Create a row counter to take the last delivery fee/MOV seen in the session. We assume that this is the one that the customer took their decision to purchase/not purchase on
    FROM dps_logs_stg_1
),

dps_logs AS(
    SELECT *
    FROM dps_logs_stg_2 
    WHERE row_num_dps_logs = 1 -- Take the last DF/MOV seen by the customer during the session
),

cvr_not_agg AS (
    SELECT -- TG1, TG2, Non-TG
    e.* EXCEPT(vendor_code, dps_zones_id, ab_test_variant, variant), -- Exclude the AB test variant from the GA sessions because it will be replaced by the experiment_variant from the DPS logs
    COALESCE(tg.tg_name, 'Non_TG') AS target_group,
    ven_id,
    dps_zone
FROM `fulfillment-dwh-production.cl.dps_cvr_events` e
CROSS JOIN UNNEST(e.vendor_code) ven_id -- vendor_code is stored as an array in dps_cvr_events. We must UNNEST it
CROSS JOIN UNNEST(SPLIT(dps_zones_id, ", ")) dps_zone -- dps_zones_id is stored as a concatenated array in dps_cvr_events. We must convert it to rows
-- IMPORTANT NOTE: WE INNER JOIN ON BOTH TARGET ZONES AND **ALL** VENDORS IN THE TARGET ZONES, THEN LEFT JOIN ON THE TARGET GROUPS TABLE
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_hk_surge_mov` zn ON zn.entity_id = e.entity_id AND dps_zone = CAST(zn.zone_id AS STRING)  -- Filter for orders from vendors in the target zones
INNER JOIN  `dh-logistics-product-ops.pricing.ab_test_vendors_in_target_zones_hk_surge_mov` ven ON e.entity_id = ven.entity_id AND ven_id = ven.vendor_code -- Filter for sessions coming from **ALL** vendors in the targeted zones
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_hk_surge_mov` tg ON tg.entity_id = e.entity_id AND tg.vendor_code = ven_id -- Tag the vendors with their target group association
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` vv2 ON vv2.entity_id = e.entity_id AND vv2.vendor_code = ven_id -- Used to get the vertical
CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- Used to get the OD status
WHERE TRUE
    AND e.entity_id = entity
    AND e.created_date BETWEEN start_date AND end_date
    AND e.variant IN UNNEST(variants)
    AND delivery_type = od_status
    AND vv2.vertical_type = v_type
)

SELECT
    x.*,
    logs.DF_tt,
    logs.DF_surge,
    logs.DF_total,
    logs.mov_tt,
    logs.mov_surge,
    logs.mov_total,
    logs.vendor_price_scheme_type,
    logs.scheme_id,
    logs.experiment_id,
    logs.variant,
    logs.vertical_parent,
    CASE WHEN (mov_surge != 0 OR DF_surge != 0) THEN 'surge_event' ELSE 'non_surge_event' END AS surge_event_flag,
FROM cvr_not_agg x
INNER JOIN dps_logs logs -- INNER JOIN so that we only get the GA sessions that have corresponding data in the DPS logs. By doing an INNER JOIN, we don't need the DF_tt/DF_surge IS NOT NULL condition in the WHERE clause of the following sub-query
    ON TRUE
    AND x.entity_id = logs.entity_id 
    AND x.dps_session_id = logs.dps_session_id 
    AND x.created_date = logs.created_date 
    AND x.ven_id = logs.vendor_code -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
ORDER BY x.entity_id, logs.experiment_id, x.created_date, x.ga_session_id, x.ven_id;

-- Step 6: Drop incorrect records

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` AS
SELECT 
    *,
    CASE
        WHEN experiment_id = exp_id_central_pre_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_central_pre_post) AND mov_surge != 0 THEN 'Drop' -- Drop the GA sessions where MOV surge > 0 when it shouldn't have been
    ELSE 'Keep' 
    END AS drop_keep_flag
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_hk_surge_mov`
WHERE TRUE 
    AND (
        CASE
            -- Central
            WHEN experiment_id = exp_id_central_pre_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_central_pre_post) AND mov_surge != 0 THEN 'Drop' -- Drop the GA sessions where MOV surge > 0 when it shouldn't have been (it should be > 0 for Variation9 only)
            WHEN experiment_id = exp_id_central_pre_post AND CONCAT(target_group, ' | ', variant, ' | ', scheme_id) NOT IN UNNEST(target_group_variant_scheme_id_valid_combos_central_pre_post) THEN 'Drop' -- Drop the GA sessions with incorrect TG_variant_scheme combos
            
            -- TSW Pre
            WHEN experiment_id = exp_id_tsw_pre AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_surge != 0 THEN 'Drop' -- Drop the GA sessions where MOV surge > 0 when it shouldn't have been
            WHEN experiment_id = exp_id_tsw_pre AND CONCAT(target_group, ' | ', variant, ' | ', scheme_id) NOT IN UNNEST(target_group_variant_scheme_id_valid_combos_tsw_pre) THEN 'Drop'

            -- TSW Post
            WHEN experiment_id = exp_id_tsw_post AND surge_event_flag = 'surge_event' AND variant NOT IN UNNEST(surge_mov_variant_tsw_pre_post) AND mov_surge = 0 THEN 'Drop' -- Drop the GA sessions where MOV surge = 0 when it shouldn't have been
            WHEN experiment_id = exp_id_tsw_post AND CONCAT(target_group, ' | ', variant, ' | ', scheme_id) NOT IN UNNEST(target_group_variant_scheme_id_valid_combos_tsw_post) THEN 'Drop'
        ELSE 'Keep' END) = 'Keep'
    AND ((target_group IN UNNEST(exp_target_groups) AND vendor_price_scheme_type = 'Experiment') OR (target_group = 'Non_TG'));

----------------------------------------------------------------SUPPLEMENTARY PART----------------------------------------------------------------

-- **With** the surge event flag
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_with_surge_flag_hk_surge_mov_overall` AS
SELECT -- TG1 and TG2
    e.entity_id,
    e.experiment_id,
    'TG1_TG2' AS target_group,
    e.variant,
    surge_event_flag,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group IN ('TG1', 'TG2')
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- Non-TG
    e.entity_id,
    e.experiment_id,
    'Non_TG' AS target_group,
    e.variant,
    surge_event_flag,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group = 'Non_TG'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- **Without** the surge event flag
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_wout_surge_flag_hk_surge_mov_overall` AS
SELECT -- TG1 and TG2
    e.entity_id,
    e.experiment_id,
    'TG1_TG2' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group IN ('TG1', 'TG2')
GROUP BY 1,2,3,4

UNION ALL

SELECT -- Non-TG
    e.entity_id,
    e.experiment_id,
    'Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group = 'Non_TG'
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

----------------------------------------------------------------END OF BUSINESS KPIs PART----------------------------------------------------------------

-- Step 5: Pull CVR data from dps_cvr_events (PER DAY)
-- **With** surge flag
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_with_surge_flag_hk_surge_mov_per_day` AS
SELECT -- TG1 and TG2
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TG1_TG2' AS target_group,
    e.variant,
    surge_event_flag,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group IN ('TG1', 'TG2')
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT -- Non-TG
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'Non_TG' AS target_group,
    e.variant,
    surge_event_flag,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group = 'Non_TG'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6;

-- **Without** surge flag
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_wout_surge_flag_hk_surge_mov_per_day` AS
SELECT -- TG1 and TG2
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TG1_TG2' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group IN ('TG1', 'TG2')
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- Non-TG
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_hk_surge_mov` e
WHERE target_group = 'Non_TG'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;
