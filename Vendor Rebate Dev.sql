USE WAREHOUSE SNOWBALL_ODS

--- Adjust transaction data for additional fields 
ALTER TABLE TM_IGLOO_ODS_STG.ods."TRANSACTIONS" 
ADD  REBATE_START_DATE TIMESTAMPTZ,
	REBATE_END_DATE TIMESTAMPTZ,
	REBATE_PERCENTAGE float,
	REBATE_DOLLAR_AMOUNT float,
	REBATE_BILLING_PERIOD varchar(100),
	REBATE_TYPE varchar(102),
	REBATE_CALC_TYPE varchar(103)
	REBATE_BILLING_METHOD varchar(104);



--update tables for gl mapping

INSERT INTO ods.TRANSACTION_SUB_TYPE  (TRAN_SUB_TYPE_ID,TRAN_TYPE,sub_type)

VALUES (111,300,'Vendor Rebates'),
(112,300,'Brand Marketing Fees'),
(113,300,'TPR Vendor Funding'),
(114,300,'A&S Vendor Funding'),
(115,300,'Thrive Cash Vendor Funding'),
(116,300,'Cold Care'),
(117,300,'Summer Care'),
(118,300,'Coupon Vendor Funding')

SELECT * FROM ods.TRANSACTION_SUB_TYPE  


INSERT INTO ods.GL_MAP_HEADER (MAP_ID,TRAN_TYPE,TRAN_SUB_TYPE_ID,PRIORITY,MAP_DESCRIPTION)

(174,300,111,9999,'Vendor Funding Invoice - Rebates '),
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

SELECT * FROM ods.GL_MAP_DETAIL 

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

insert INTO ods.GL_MAP_RULES (GMR_ID,MAP_ID,RULE_NUM,COLUMN_NAME,CONDITION,VALUE)


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
(227,189,1)

---Built on Transactions Table
insert into ods.transactions (TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,CREATED_AT,INCREMENT_ID,MAGENTO_LOCATION_ID,TRAN_COST_AMT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,TRAN_AMT,REBATE_BILLING_METHOD
)   
  
  
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
,COALESCE(vr.billing_method_id,vrin.billing_method_id,br.billing_method_id,brin.billing_method_id) AS billing_method FROM TM_IGLOO_ODS_PRD.ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF fc ON tr.LOCATION_ID = fc.NS_FC_ID 
LEFT JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID = tr.ITEM_ID AND fc.ODS_FC_ID= ci.FC_ID 
LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 1 )vr ON vr.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < $P{start_date} AND end_date> $P{end_date} AND active_status_id = 2 )vrin ON vrin.ITEM_ID =tr.ITEM_ID
LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < $P{start_date} AND end_date> $P{end_date}) br ON  br.brand_id=ci.brand_id
LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date < $P{start_date} AND end_date> $P{end_date}) brin ON  brin.brand_id=ci.brand_id
WHERE tr.TRAN_GL_DATE BETWEEN $P{start_date} AND $P{end_date} AND tr.TRAN_SUB_TYPE_ID =16 AND tr.TRAN_TYPE =300
AND fc.FC_TYPE !='drinks' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT NULL
LIMIT 5


--
--insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
--        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
--        subclass_id, subclass_name,tran_qty, tran_amt, created_at, sale_date, item_id, tran_cost_amt, tran_gl_date, ship_date)
--        
--        
        
select 300 as tran_type, 111, 'Vendor Rebates'  as tran_sub_type, sof.tran_date, 'order' as order_type, sof.order_id, sof.increment_id,
sof.item_id as order_line_id, sof.customer_id,1, sof.fc, sof.magento_fc, sof.sku, sof.parent_item_id, ci.group_id, ci.group_name,
ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
sof.units_shipped 	, CASE	
	WHEN CASE 
	WHEN sof.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (sof.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * (sof.qty_ordered * ci.net_unit_cost)
	WHEN CASE 
	WHEN sof.tran_date >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (sof.tran_date <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
	THEN 'Y'
	ELSE 'N'
END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (sof.units_shipped )
END AS tran_amt,
current_timestamp, sof.sale_date, ci.item_id, sof.qty_ordered * ci.net_unit_cost,
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
where sof.units_shipped  > 0 and sof.product_type <> 'bundle'







--reporting build
SELECT da.*,
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


SELECT * FROM ods."TRANSACTIONS" tr  WHERE tr.tran_sub_type_id = 16 LIMIT 10
