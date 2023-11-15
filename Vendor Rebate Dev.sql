USE WAREHOUSE SNOWBALL_ODS


SELECT * FROM ods."TRANSACTIONS" tr 
LIMIT 10 


--- Adjust transaction data for additional fields 
ALTER TABLE TM_IGLOO_ODS_STG.ods."TRANSACTIONS" 
ADD  REBATE_START_DATE TIMESTAMPTZ,
	REBATE_END_DATE TIMESTAMPTZ,
	REBATE_PERCENTAGE float,
	REBATE_DOLLAR_AMOUNT float,
	REBATE_BILLING_PERIOD varchar(100),
	REBATE_TYPE varchar(102),
	REBATE_CALC_TYPE varchar(103),
	REBATE_BILLING_METHOD varchar(104),
	FULFILLMENT_TRAN_ID number(36),
	NS_TRAN_ID number(35);



--update tables for gl mapping

SELECT * FROM ods.TRANSACTION_SUB_TYPE

INSERT INTO ods.TRANSACTION_SUB_TYPE  (TRAN_SUB_TYPE_ID,TRAN_TYPE,TRAN_SUB_TYPE)
VALUES (111,300,'Vendor Rebates'),
(112,300,'Brand Marketing Fees'),
(113,300,'TPR Vendor Funding'),
(114,300,'A&S Vendor Funding'),
(115,300,'Thrive Cash Vendor Funding'),
(116,300,'Cold Care'),
(117,300,'Summer Care'),
(118,300,'Coupon Vendor Funding')

INSERT INTO ods.GL_MAP_HEADER (MAP_ID,TRAN_TYPE,TRAN_SUB_TYPE_ID,PRIORITY,MAP_DESCRIPTION)
VALUES (174,300,111,9999,'Vendor Funding Invoice - Rebates '),
(175,300,112,9999,'Vendor Funding Invoice - Brand Marketing Fees'),
(176,300,113,9999,'Vendor Funding Invoice - TPR'),
(177,300,114,9999,'Vendor Funding Invoice - A&S'),
(178,300,115,9999,'Vendor Funding Invoice - Thrive Cash'),
(179,300,116,9999,'Vendor Funding Invoice - Cold Care'),
(180,300,117,9999,'Vendor Funding Invoice - Summer Care'),
(181,300,118,9999,'Vendor Funding Invoice - Coupons'),
(182,300,111,9999,'Vendor Funding Credit - Rebates '),
(183,300,112,9999,'Vendor Funding Credit - Brand Marketing Fees'),
(184,300,113,9999,'Vendor Funding Credit - TPR'),
(185,300,114,9999,'Vendor Funding Credit - A&S'),
(186,300,115,9999,'Vendor Funding Credit - Thrive Cash'),
(187,300,116,9999,'Vendor Funding Credit - Cold Care'),
(188,300,117,9999,'Vendor Funding Credit - Summer Care'),
(189,300,118,9999,'Vendor Funding Credit - Coupons')

INSERT INTO ods.GL_MAP_DETAIL (GMD_ID,MAP_ID,LINE_NUM,TRAN_COLUMN_NAME,DEBIT_ACCOUNT_ID,CREDIT_ACCOUNT_ID)
VALUES (359,174,1,'tran_amt',670,284),
(360,175,1,'tran_amt',670,570),
(361,176,1,'tran_amt',670,802),
(362,177,1,'tran_amt',670,885),
(363,178,1,'tran_amt',670,801),
(364,179,1,'tran_amt',670,648),
(365,180,1,'tran_amt',670,648),
(366,181,1,'tran_amt',670,277),
(367,182,1,'tran_amt',112,802),
(368,183,1,'tran_amt',112,570),
(369,184,1,'tran_amt',112,802),
(370,185,1,'tran_amt',112,885),
(371,186,1,'tran_amt',112,801),
(372,187,1,'tran_amt',112,648),
(373,188,1,'tran_amt',112,648),
(374,189,1,'tran_amt',112,802)

SELECT *FROM TM_IGLOO_ODS_STG.ods.GL_MAP_RULEs

insert INTO ods.GL_MAP_RULES (GMR_ID,MAP_ID,RULE_NUM)
VALUES (213,175,1),
(214,176,1),
(215,177,1),
(216,178,1),
(217,179,1),
(218,180,1),
(219,181,1),
(220,182,1),
(221,183,1),
(222,184,1),
(223,185,1),
(224,186,1),
(225,187,1),
(226,188,1),
(227,189,1);

SELECT * 
FROM ods."TRANSACTIONS" tr
WHERE tr.TRAN_SUB_TYPE_ID =111 AND tr.FULFILLMENT_TRAN_ID IS NOT NULL AND tr.TRAN_GL_DATE <'2023-10-30'

---Built on Transactions Table
insert into ods.transactions (TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,CREATED_AT,INCREMENT_ID,MAGENTO_LOCATION_ID,TRAN_COST_AMT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,TRAN_AMT,REBATE_BILLING_METHOD,FULFILLMENT_TRAN_ID
)
SELECT 300,'Vendor Rebates',tr.TRAN_DATE,tr.ORDER_TYPE,tr.ORDER_ID,tr.ORDER_LINE_ID,tr.COMPANY_ID,tr.LOCATION_ID,tr.CUSTOMER_ID,tr.SKU,tr.GROUP_ID,tr.GROUP_NAME,tr.CATEGORY_ID,tr.CATEGORY_NAME,tr.SUB_CATEGORY_ID,tr.SUB_CATEGORY_NAME,tr.CLASS_ID,tr.CLASS_NAME,tr.SUBCLASS_ID,tr.SUBCLASS_NAME,tr.TRAN_QTY,tr.CREATED_AT,tr.INCREMENT_ID,tr.MAGENTO_LOCATION_ID,tr.TRAN_COST_AMT,tr.SALE_DATE,tr.ITEM_ID,111,tr.TRAN_GL_DATE,tr.SHIP_DATE,tr.TRAN_COGS_AMT
,COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type
,
CASE	
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * tr.tran_cogs_amt
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (tran_qty)
END AS rebate_total	
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method,
tr.tran_id
FROM ods.TRANSACTIONS_PROD tr
LEFT JOIN ods.NS_FC_XREF fc ON tr.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = tr.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{end_date} AND end_date > $P{start_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{end_date} AND end_date > $P{start_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{end_date} AND end_date > $P{start_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{end_date} AND end_date > $P{start_date}) brin ON  brin.brand_id=ci.brand_id
WHERE tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND tr.TRAN_SUB_TYPE_ID =16 AND tr.TRAN_TYPE =300
AND fc.FC_TYPE !='drinks' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT NULL
 
SELECT * FROM ods."TRANSACTIONS" tr WHERE tr.TRAN_SUB_TYPE_ID  =111

insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name,tran_qty, tran_amt, created_at, sale_date, item_id, tran_cost_amt, tran_gl_date, ship_date,rebate_start_date,rebate_end_date,rebate_percentage,rebate_dollar_amount,rebate_billing_period,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD)         

        
        select 300 as tran_type, 111, 'Vendor Rebates'  as tran_sub_type, sof.tran_date, 'order' as order_type, sof.order_id, sof.increment_id,
sof.item_id as order_line_id, sof.customer_id,1, sof.fc, sof.magento_fc, sof.sku, sof.parent_item_id, ci.group_id, ci.group_name,
ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
sof.units_shipped 	, CASE	
	WHEN CASE 
	WHEN sof.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (sof.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * (sof.units_shipped * ci.net_unit_cost)
	WHEN CASE 
	WHEN sof.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (sof.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (sof.units_shipped )
END AS tran_amt,
current_timestamp, sof.sale_date, ci.item_id, sof.UNITS_SHIPPED  * ci.net_unit_cost,
convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date,
COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method
from staging.stage_order_fulfillment sof
left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < sysdate() AND end_date> sysdate() AND active_status_id = 1 )vr ON vr.ITEM_ID =sof.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < sysdate() AND end_date> sysdate() AND active_status_id = 2 )vrin ON vrin.ITEM_ID =sof.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < sysdate() AND end_date> sysdate()) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date <sysdate() AND end_date> sysdate()) brin ON  brin.brand_id=ci.brand_id
where sof.units_shipped  > 0 and sof.product_type <> 'bundle' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT null


-- rebate only report
WITH test AS (SELECT ci.brand_name,tr.REBATE_billing_method,tr.sku,ci.description, tr.REBATE_TYPE,tr.REBATE_BILLING_PERIOD,tr.REBATE_START_DATE, tr.REBATE_END_DATE, tr.TRAN_QTY ,tr.TRAN_AMT ,tr.TRAN_COGS_AMT ,tr.REBATE_PERCENTAGE,tr.REBATE_DOLLAR_AMOUNT,tr.TRAN_AMT  
FROM ods."TRANSACTIONS" tr
LEFT JOIN ods.CURR_ITEMS_PROD ci ON ci.item_name = tr.SKU  AND ci.fc_id = tr.MAGENTO_LOCATION_ID 
WHERE tr.TRAN_SUB_TYPE_ID in (111,16) AND tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date}) 
SELECT tt.*,bm.LIST_ITEM_NAME,
COALESCE(mt.BILLING_CUSTOMER_ID ,mt.VENDOR_ID) AS cust_vend FROM test tt
LEFT JOIN (SELECT DISTINCT BRAND_ID,BRAND_RECORDS_NAME ,BILLING_CUSTOMER_ID ,VENDOR_ID  FROM ods.NS_VENDOR_REBATES ) mt ON mt.BRAND_RECORDS_NAME = tt.BRAND_NAME 
LEFT JOIN ods.BILLING_METHOD bm ON bm.LIST_ID =tt.rebate_billing_method
WHERE tt.rebate_start_date IS NOT NULL



-- rebate only report grouped 
SELECT reb.BRAND_NAME,reb.REBATE_BILLING_METHOD,reb.SKU,reb.DESCRIPTION,reb.REBATE_TYPE,reb.REBATE_BILLING_PERIOD,reb.REBATE_START_DATE,reb.REBATE_END_DATE,sum(TRAN_QTY) AS TRAN_QTY,sum(reb.TRAN_COGS_AMT) AS TRAN_COGS_AMT,reb.REBATE_PERCENTAGE,reb.REBATE_DOLLAR_AMOUNT,sum(reb.TRAN_AMT) AS TRAN_AMT ,reb.LIST_ITEM_NAME,reb.CUST_VEND
from(
WITH test AS (SELECT ci.brand_name,tr.REBATE_billing_method,tr.sku,ci.description, tr.REBATE_TYPE,tr.REBATE_BILLING_PERIOD,tr.REBATE_START_DATE, tr.REBATE_END_DATE, tr.TRAN_QTY ,tr.TRAN_COGS_AMT ,tr.REBATE_PERCENTAGE,tr.REBATE_DOLLAR_AMOUNT,tr.TRAN_AMT  
FROM ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF ns ON ns.NS_FC_ID = tr.LOCATION_ID 
LEFT JOIN ods.CURR_ITEMS_PROD ci ON ci.item_name = tr.SKU  AND COALESCE(ci.fc_id,2) = ns.ODS_FC_ID  
WHERE tr.TRAN_SUB_TYPE_ID in (111,16) AND tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date}) 
SELECT tt.*,bm.LIST_ITEM_NAME,
COALESCE(mt.BILLING_CUSTOMER_ID ,mt.VENDOR_ID) AS cust_vend FROM test tt
LEFT JOIN (SELECT DISTINCT BRAND_ID,BRAND_RECORDS_NAME ,BILLING_CUSTOMER_ID ,VENDOR_ID  FROM ods.NS_VENDOR_REBATES ) mt ON mt.BRAND_RECORDS_NAME = tt.BRAND_NAME 
LEFT JOIN ods.BILLING_METHOD bm ON bm.LIST_ID =tt.rebate_billing_method
WHERE tt.rebate_start_date IS NOT NULL) reb 
group BY reb.BRAND_NAME,reb.REBATE_BILLING_METHOD,reb.SKU,reb.DESCRIPTION,reb.REBATE_TYPE,reb.REBATE_BILLING_PERIOD,reb.REBATE_START_DATE,reb.REBATE_END_DATE,reb.REBATE_PERCENTAGE,reb.REBATE_DOLLAR_AMOUNT,reb.LIST_ITEM_NAME,reb.CUST_VEND

SELECT * FROM ods.CURR_ITEMS_PROD cur WHERE cur.ITEM_NAME ='042272005475'

SELECT *  
FROM ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF ns ON ns.NS_FC_ID = tr.LOCATION_ID 
LEFT JOIN ods.CURR_ITEMS_PROD ci ON ci.item_name = tr.SKU  AND COALESCE(ci.fc_id,2) = ns.ODS_FC_ID  
WHERE tr.TRAN_SUB_TYPE_ID in (111) AND tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND ci.BRAND_NAME IS null

SELECT * FROM ods.CURR_ITEMS_PROD 

SELECT * FROM ods.NS_VENDOR_REBATES 

SELECT * FROM ODS."TRANSACTIONS" tr
WHERE tr.TRAN_SUB_TYPE_id = 111 
    
UPDATE ods."TRANSACTIONS" 
	SET 
	TRANSACTIONS.FULFILLMENT_TRAN_ID = upd.tran_id
	FROM (
    SELECT trn.tran_id, trn.ORDER_LINE_ID 
    FROM ods.TRANSACTIONS trn
    WHERE trn.tran_sub_type_id = 16) upd
    WHERE TRANSACTIONS.ORDER_LINE_ID = upd.ORDER_LINE_ID AND TRANSACTIONS.TRAN_SUB_TYPE_ID =111 AND TRANSACTIONS.fulfillment_tran_id IS NULL 
    
    
SELECT 300,'Vendor Rebates',tr.TRAN_DATE,tr.ORDER_TYPE,tr.ORDER_ID,tr.ORDER_LINE_ID,tr.COMPANY_ID,tr.LOCATION_ID,tr.CUSTOMER_ID,tr.SKU,tr.GROUP_ID,tr.GROUP_NAME,tr.CATEGORY_ID,tr.CATEGORY_NAME,tr.SUB_CATEGORY_ID,tr.SUB_CATEGORY_NAME,tr.CLASS_ID,tr.CLASS_NAME,tr.SUBCLASS_ID,tr.SUBCLASS_NAME,tr.TRAN_QTY,tr.CREATED_AT,tr.INCREMENT_ID,tr.MAGENTO_LOCATION_ID,tr.TRAN_COST_AMT,tr.SALE_DATE,tr.ITEM_ID,111,tr.TRAN_GL_DATE,tr.SHIP_DATE,tr.TRAN_COGS_AMT,
COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type
,
CASE	
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * tr.tran_cogs_amt
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (tran_qty)
END AS rebate_total	
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method,
tr.tran_id
FROM ods.TRANSACTIONS_PROD tr
LEFT JOIN ods.NS_FC_XREF fc ON tr.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = tr.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{start_date} AND end_date> $P{end_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{start_date} AND end_date> $P{end_date}) brin ON  brin.brand_id=ci.brand_id
WHERE tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND tr.TRAN_SUB_TYPE_ID =16 AND tr.TRAN_TYPE =300
AND fc.FC_TYPE !='drinks' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT NULL
 


INSERT INTO ods."TRANSACTIONS" (sku,tran_type ,TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE,tran_date ,TRAN_COGS_AMT,CREATED_AT,ITEM_ID,TRAN_GL_DATE,LOCATION_ID,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,TRAN_AMT,REBATE_BILLING_METHOD,NS_TRAN_ID)
SELECT ci.ITEM_name, 300,111,'Vendor Rebates',he.TRANDATE::TIMESTAMP_NTZ ,de.amount, de.DATE_CREATED::TIMESTAMP_NTZ ,de.ITEM_ID , he.TRANDATE,de.LOCATION_ID ,
COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type
,
CASE	
	WHEN CASE 
	WHEN he.trandate >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (he.trandate <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * de.amount
	WHEN CASE 
	WHEN he.trandate >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (he.trandate <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (0)
END AS rebate_total	
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method,he.transaction_id
FROM ods.NS_COSTADJ_DETAIL de 
INNER JOIN ods.NS_COSTADJ_HEADER he ON de.TRANSACTION_ID =he.TRANSACTION_ID 
FULL OUTER JOIN (SELECT DISTINCT tr.ns_tran_id FROM ods."TRANSACTIONS" tr) trn ON trn.ns_tran_id = de.TRANSACTION_ID 
LEFT JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = de.LOCATION_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = de.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date <= sysdate()  AND end_date> sysdate() AND active_status_id = 1 )vr ON vr.ITEM_ID =de.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date <= sysdate()  AND end_date> sysdate() AND active_status_id = 2 )vrin ON vrin.ITEM_ID =de.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date <= sysdate() AND end_date> sysdate() ) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date <= sysdate() AND end_date> sysdate() ) brin ON  brin.brand_id=ci.brand_id
WHERE de.ACCOUNT_ID  =279 AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage)  IS NOT NULL AND he.TRANDATE < sysdate()  AND trn.ns_tran_id IS null



SELECT * FROM ods.NS_COSTADJ_DETAIL de 
INNER JOIN ods.NS_COSTADJ_HEADER he ON he.TRANSACTION_ID =de.TRANSACTION_ID 

SELECT * FROM ods.CURR_ITEMS ci WHERE ci.ITEM_ID ='9073'

SELECT * FROM ods."TRANSACTIONS"  WHERE TRAN_SUB_TYPE_ID =111 AND NS_TRAN_ID IS NOT NULL

SELECT 
SKU,sum(tran_qty) AS quantity , sum(TRAN_COGS_AMT) AS COGS ,sum(PB_COST) AS pb_cogs,START_DATE,END_DATE,PERCENTAGE,DOLLAR_AMOUNT,BILLING_PERIOD,BRAND_SKU,REBATE_CALC_TYPE,sum(REBATE_TOTAL) AS fiforebate,sum(PB_REBATE_TOTAL) AS pb_rebate,BILLING_METHOD
FROM (
SELECT 300,'Vendor Rebates',tr.TRAN_DATE,tr.ORDER_TYPE,tr.ORDER_ID,tr.ORDER_LINE_ID,tr.COMPANY_ID,tr.LOCATION_ID,tr.CUSTOMER_ID,tr.SKU,tr.GROUP_ID,tr.GROUP_NAME,tr.CATEGORY_ID,tr.CATEGORY_NAME,tr.SUB_CATEGORY_ID,tr.SUB_CATEGORY_NAME,tr.CLASS_ID,tr.CLASS_NAME,tr.SUBCLASS_ID,tr.SUBCLASS_NAME,tr.TRAN_QTY,tr.CREATED_AT,tr.INCREMENT_ID,tr.MAGENTO_LOCATION_ID,tr.TRAN_COST_AMT,tr.SALE_DATE,tr.ITEM_ID,111,tr.TRAN_GL_DATE,tr.SHIP_DATE,tr.TRAN_COGS_AMT,tr.TRAN_AMT AS pb_cost
,COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type
,
CASE	
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * tr.tran_cogs_amt
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (tran_qty)
END AS rebate_total	
,CASE	
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * tr.tran_amt
	WHEN CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (tran_qty)
END AS pb_rebate_total	
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method,
tr.tran_id
FROM ods.TRANSACTIONS_PROD tr
LEFT JOIN ods.NS_FC_XREF fc ON tr.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = tr.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{end_date} AND end_date > $P{start_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{end_date} AND end_date > $P{start_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{end_date} AND end_date > $P{start_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{end_date} AND end_date > $P{start_date}) brin ON  brin.brand_id=ci.brand_id
WHERE tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND tr.TRAN_SUB_TYPE_ID =16 AND tr.TRAN_TYPE =300
AND fc.FC_TYPE !='drinks' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT NULL)
GROUP BY SKU,START_DATE,END_DATE,PERCENTAGE,DOLLAR_AMOUNT,BILLING_PERIOD,BRAND_SKU,REBATE_CALC_TYPE,BILLING_METHOD


SELECT * FROM ods.CURR_ITEMS WHERE ITEM_TYPE = 'Other Charge' AND ITEM_ID IN ('149998','150001','149999','150000','150002','167942','171783','46805','31184','23431','7735','31091','31185') AND FC_ID =2