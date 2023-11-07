USE WAREHOUSE SNOWBALL_FINANCE_BUS


BEGIN TRANSACTION

--- Adjust transaction data for additional fields 
ALTER TABLE TM_IGLOO_ODS_STG.ods."TRANSACTIONS" 
ADD  REBATE_START_DATE TIMESTAMPTZ,
	REBATE_END_DATE TIMESTAMPTZ,
	REBATE_PERCENTAGE float,
	REBATE_DOLLAR_AMOUNT float,
	REBATE_BILLING_PERIOD varchar(100),
	REBATE_TYPE varchar(102),
	REBATE_CALC_TYPE varchar(103);



--update tables for gl mapping

INSERT INTO ods.TRANSACTION_SUB_TYPE  (TRAN_SUB_TYPE_ID,TRAN_TYPE)

VALUES (111,300,'Vendor Rebates')

SELECT * FROM ods.TRANSACTION_SUB_TYPE 


INSERT INTO ods.GL_MAP_HEADER (MAP_ID,TRAN_TYPE,TRAN_SUB_TYPE_ID,PRIORITY,MAP_DESCRIPTION)


VALUES (167, 350, 109,9999,'Product Refund Adjustment'),
(168, 350, 110,9999,'Gift Card Refund Adjustment')

INSERT INTO ods.GL_MAP_DETAIL (GMD_ID,MAP_ID,LINE_NUM,TRAN_COLUMN_NAME,DEBIT_ACCOUNT_ID,CREDIT_ACCOUNT_ID)


VALUES (354,167,1,'tran_amt',594,277),
(355,168,1,'tran_amt',594,886)

insert INTO ods.GL_MAP_RULES (GMR_ID,MAP_ID,RULE_NUM)


VALUES (206,167,1),(207,168,1)


SELECT * FROM TRANSACTION_SUB_TYPE 


---Built on Transactions

SELECT 399,'Vendor Rebate', tr.TRAN_DATE , 'order',tr.order_id,
tr.LOCATION_ID ,tr.SKU,ci.GROUP_ID ,ci.GROUP_NAME ,ci.ITEM_CATEGORY_ID ,ci.ITEM_CATEGORY_NAME ,
ci.SUBCATEGORY_ID ,ci.SUBCATEGORY_NAME ,ci.CLASS_ID ,ci.CLASS_NAME 
,tr.ITEM_ID ,tr.TRAN_QTY ,tr.sku,tr.SALE_DATE ,
tr.INCREMENT_ID ,tr.tran_COGS_AMT ,
CASE 
	WHEN tr.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (tr.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END AS rebate_needed,
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
	WHEN rebate_needed ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * tr.tran_cogs_amt
	WHEN rebate_needed ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (tran_qty)
END AS rebate_total	 FROM TM_IGLOO_ODS_PRD.ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF fc ON tr.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = tr.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{start_date} AND end_date> $P{end_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{start_date} AND end_date> $P{end_date}) brin ON  brin.brand_id=ci.brand_id
WHERE tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND tr.TRAN_SUB_TYPE_ID =16 AND tr.TRAN_TYPE =300
AND fc.FC_TYPE !='drinks'



--reporting build
SELECT da.*
CASE 
	WHEN da.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (da.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END AS rebate_needed,
COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AS start_date,
COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) AS end_date,
COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) AS percentage,
COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) AS dollar_amount,
COALESCE(vr.billing_period,vrin.billing_period,br.billing_period,brin.billing_period) AS billing_period,
COALESCE(vr.brand_sku,vrin.brand_sku,br.brand_sku,brin.brand_sku) AS brand_sku,
CASE 
	WHEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN 'Dollar Amount (unit)' 
	WHEN COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN 'Percentage of COGS' 
END AS rebate_calc_type,
CASE	
	WHEN rebate_needed ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * da.cogs_amt
	WHEN rebate_needed ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (-tran_qty)
END AS rebate_total	
FROM TM_IGLOO_ODS_PRD.ods.DETAIL_ADJUSTMENTS da 
LEFT JOIN ods.NS_FC_XREF fc ON da.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = da.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =da.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =da.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{start_date} AND end_date> $P{end_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{start_date} AND end_date> $P{end_date}) brin ON  brin.brand_id=ci.brand_id
WHERE da.TRAN_DATE BETWEEN $P{start_date} AND $P{end_date} AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT NULL

SELECT * FROM ods.TRANSACTION_SUB_TYPE 



