
INSERT INTO ods.TRANSACTIONS_DUPES 
SELECT *  FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,PARENT_ITEM_ID,PRODUCT_TYPE,COUPON_CODE,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,TRAN_AMT,TRAN_FULL_PRICE,TRAN_DISCOUNT_AMT,TRAN_TAX_AMT,BILLING_COUNTRY,BILLING_STATE,BILLING_CITY,SHIPPING_COUNTRY,SHIPPING_STATE,SHIPPING_CITY,MEMBERSHIP_TYPE_ID,MEMBERSHIP_EXP_DATE,ORIGINAL_ORDER_ID,CREDIT_MEMO_ID,MEMBERSHIP_START_DATE,JE_BATCH_ID,JE_BATCH_DATE,GIFT_CARD_ID,ITEM_TYPE,DISCOUNT_CODE,CANCEL_TYPE,COUPON_RULE_NAME,MEMBERSHIP_ID,MEMBERSHIP_CANCEL_DATE,CC_AUTH,CC_LAST4,BANK_DATE,DEPOSIT_DATE,CREATED_AT,FEE_AMT,CC_TYPE,CC_RECON_DATE,CC_RECON_AMT,CC_PAY_AMT,CM_ID,GC_ID,GC_TYPE,GC_SUB_TYPE,GC_REASON_ID,GC_REASON_NAME,GC_GROUP_CODE,GC_GROUP_NAME,INCREMENT_ID,REASON_ID,REFUND_REASON,MAGENTO_LOCATION_ID,COUPON_GROUP_CODE,RULE_ID,RULE_NAME,SOURCE_UUID,SOURCE_LINE_NUM,GL_GROUP_OVERRIDE,TRAN_COST_AMT,JE_MAP_ID,MESSAGE_UUID,START_DATE,END_DATE,START_STATUS,END_STATUS,UPDATED_AT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,ORIGINAL_TRAN_ID,FC_SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD,FULFILLMENT_TRAN_ID,NS_TRAN_ID,BRAND_MARKETING_ID,REBATE_TOTAL ORDER BY tr.tran_id) AS ROW_NUMBER
FROM ods."TRANSACTIONS" tr  WHERE tr.SALE_DATE  ='2023-11-15' )
HAVING ROW_NUMBER >1


WITH dupes AS (SELECT *  FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,PARENT_ITEM_ID,PRODUCT_TYPE,COUPON_CODE,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,TRAN_AMT,TRAN_FULL_PRICE,TRAN_DISCOUNT_AMT,TRAN_TAX_AMT,BILLING_COUNTRY,BILLING_STATE,BILLING_CITY,SHIPPING_COUNTRY,SHIPPING_STATE,SHIPPING_CITY,MEMBERSHIP_TYPE_ID,MEMBERSHIP_EXP_DATE,ORIGINAL_ORDER_ID,CREDIT_MEMO_ID,MEMBERSHIP_START_DATE,JE_BATCH_ID,JE_BATCH_DATE,GIFT_CARD_ID,ITEM_TYPE,DISCOUNT_CODE,CANCEL_TYPE,COUPON_RULE_NAME,MEMBERSHIP_ID,MEMBERSHIP_CANCEL_DATE,CC_AUTH,CC_LAST4,BANK_DATE,DEPOSIT_DATE,CREATED_AT,FEE_AMT,CC_TYPE,CC_RECON_DATE,CC_RECON_AMT,CC_PAY_AMT,CM_ID,GC_ID,GC_TYPE,GC_SUB_TYPE,GC_REASON_ID,GC_REASON_NAME,GC_GROUP_CODE,GC_GROUP_NAME,INCREMENT_ID,REASON_ID,REFUND_REASON,MAGENTO_LOCATION_ID,COUPON_GROUP_CODE,RULE_ID,RULE_NAME,SOURCE_UUID,SOURCE_LINE_NUM,GL_GROUP_OVERRIDE,TRAN_COST_AMT,JE_MAP_ID,MESSAGE_UUID,START_DATE,END_DATE,START_STATUS,END_STATUS,UPDATED_AT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,ORIGINAL_TRAN_ID,FC_SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD,FULFILLMENT_TRAN_ID,NS_TRAN_ID,BRAND_MARKETING_ID,REBATE_TOTAL ORDER BY tr.tran_id) AS ROW_NUMBER
FROM ods."TRANSACTIONS" tr  WHERE tr.SALE_DATE  ='2023-11-15' )
HAVING ROW_NUMBER >1) 
SELECT dp.tran_sub_type ,count(dp.tran_id) FROM dupes dp 
GROUP BY  dp.tran_sub_type 

BEGIN TRANSACTION

DELETE FROM ods."TRANSACTIONS" 
WHERE tran_id IN (
SELECT DISTINCT tran_id  FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,PARENT_ITEM_ID,PRODUCT_TYPE,COUPON_CODE,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,TRAN_AMT,TRAN_FULL_PRICE,TRAN_DISCOUNT_AMT,TRAN_TAX_AMT,BILLING_COUNTRY,BILLING_STATE,BILLING_CITY,SHIPPING_COUNTRY,SHIPPING_STATE,SHIPPING_CITY,MEMBERSHIP_TYPE_ID,MEMBERSHIP_EXP_DATE,ORIGINAL_ORDER_ID,CREDIT_MEMO_ID,MEMBERSHIP_START_DATE,JE_BATCH_ID,JE_BATCH_DATE,GIFT_CARD_ID,ITEM_TYPE,DISCOUNT_CODE,CANCEL_TYPE,COUPON_RULE_NAME,MEMBERSHIP_ID,MEMBERSHIP_CANCEL_DATE,CC_AUTH,CC_LAST4,BANK_DATE,DEPOSIT_DATE,CREATED_AT,FEE_AMT,CC_TYPE,CC_RECON_DATE,CC_RECON_AMT,CC_PAY_AMT,CM_ID,GC_ID,GC_TYPE,GC_SUB_TYPE,GC_REASON_ID,GC_REASON_NAME,GC_GROUP_CODE,GC_GROUP_NAME,INCREMENT_ID,REASON_ID,REFUND_REASON,MAGENTO_LOCATION_ID,COUPON_GROUP_CODE,RULE_ID,RULE_NAME,SOURCE_UUID,SOURCE_LINE_NUM,GL_GROUP_OVERRIDE,TRAN_COST_AMT,JE_MAP_ID,MESSAGE_UUID,START_DATE,END_DATE,START_STATUS,END_STATUS,UPDATED_AT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,ORIGINAL_TRAN_ID,FC_SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD,FULFILLMENT_TRAN_ID,NS_TRAN_ID,BRAND_MARKETING_ID,REBATE_TOTAL ORDER BY tr.tran_id) AS ROW_NUMBER
FROM ods."TRANSACTIONS" tr  WHERE tr.SALE_DATE  ='2023-11-15' )
HAVING ROW_NUMBER >1) 



SELECT * FROM ods.TRANSACTIONS_DUPES 

ROLLBACK


SELECT *,
ROW_NUMBER() OVER(PARTITION BY TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,ORDER_TYPE,ORDER_ID,ORDER_LINE_ID,COMPANY_ID,LOCATION_ID,CUSTOMER_ID,SKU,PARENT_ITEM_ID,PRODUCT_TYPE,COUPON_CODE,GROUP_ID,GROUP_NAME,CATEGORY_ID,CATEGORY_NAME,SUB_CATEGORY_ID,SUB_CATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,TRAN_QTY,TRAN_AMT,TRAN_FULL_PRICE,TRAN_DISCOUNT_AMT,TRAN_TAX_AMT,BILLING_COUNTRY,BILLING_STATE,BILLING_CITY,SHIPPING_COUNTRY,SHIPPING_STATE,SHIPPING_CITY,MEMBERSHIP_TYPE_ID,MEMBERSHIP_EXP_DATE,ORIGINAL_ORDER_ID,CREDIT_MEMO_ID,MEMBERSHIP_START_DATE,JE_BATCH_ID,JE_BATCH_DATE,GIFT_CARD_ID,ITEM_TYPE,DISCOUNT_CODE,CANCEL_TYPE,COUPON_RULE_NAME,MEMBERSHIP_ID,MEMBERSHIP_CANCEL_DATE,CC_AUTH,CC_LAST4,BANK_DATE,DEPOSIT_DATE,CREATED_AT,FEE_AMT,CC_TYPE,CC_RECON_DATE,CC_RECON_AMT,CC_PAY_AMT,CM_ID,GC_ID,GC_TYPE,GC_SUB_TYPE,GC_REASON_ID,GC_REASON_NAME,GC_GROUP_CODE,GC_GROUP_NAME,INCREMENT_ID,REASON_ID,REFUND_REASON,MAGENTO_LOCATION_ID,COUPON_GROUP_CODE,RULE_ID,RULE_NAME,SOURCE_UUID,SOURCE_LINE_NUM,GL_GROUP_OVERRIDE,TRAN_COST_AMT,JE_MAP_ID,MESSAGE_UUID,START_DATE,END_DATE,START_STATUS,END_STATUS,UPDATED_AT,SALE_DATE,ITEM_ID,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,SHIP_DATE,ORIGINAL_TRAN_ID,FC_SHIP_DATE,TRAN_COGS_AMT,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD,FULFILLMENT_TRAN_ID,NS_TRAN_ID,BRAND_MARKETING_ID,REBATE_TOTAL ORDER BY tr.tran_id) AS ROW_NUMBER

INSERT INTO ods.ORDER_header_dupe
SELECT * FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY MESSAGE_UUID ORDER BY MESSAGE_UUID) AS ROW_NUMBER
FROM ods.ORDER_HEADER oh  WHERE oh.SALE_DATE  ='2023-11-15')
WHERE ROW_NUMBER >1

GRANT ALL ON  ODS.order_header_dupe  TO ODS_DEVS

WITH dupes AS (
SELECT * FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY MESSAGE_UUID ORDER BY ODS_PROCESSED_AT desc) AS ROW_NUMBER 
FROM ods.ORDER_HEADER oh  WHERE oh.SALE_DATE  ='2023-11-15')
WHERE ROW_NUMBER >1)
DELETE FROM ods.ORDER_HEADER oh
WHERE (oh.MESSAGE_UUID,oh.ODS_PROCESSED_AT) IN (SELECT dp.MESSAGE_UUID,dp.ODS_PROCESSED_AT FROM dupes dp WHERE ROW_NUMBER >1)


DELETE FROM ods.ORDER_HEADER  
WHERE (MESSAGE_UUID,ODS_PROCESSED_AT) IN (
SELECT DISTINCT MESSAGE_UUID,ODS_PROCESSED_AT  FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY MESSAGE_UUID ORDER BY ODS_PROCESSED_AT desc) AS ROW_NUMBER 
FROM ods.ORDER_HEADER oh  WHERE oh.SALE_DATE  ='2023-11-15' )
HAVING ROW_NUMBER >1) 

SELECT *,
ROW_NUMBER() OVER(PARTITION BY MESSAGE_UUID ORDER BY ODS_PROCESSED_AT desc) AS ROW_NUMBER 
FROM ods.ORDER_HEADER oh  WHERE oh.SALE_DATE  ='2023-11-15'

create or replace TABLE TM_IGLOO_ODS_STG.ODS.TRANSACTIONS_DUPES (
	TRAN_ID NUMBER(38,0) autoincrement,
	TRAN_TYPE NUMBER(38,0),
	TRAN_SUB_TYPE VARCHAR(100),
	TRAN_DATE TIMESTAMP_NTZ(9),
	ORDER_TYPE VARCHAR(50),
	ORDER_ID NUMBER(38,0),
	ORDER_LINE_ID VARCHAR(50),
	COMPANY_ID NUMBER(38,0),
	LOCATION_ID NUMBER(38,0),
	CUSTOMER_ID VARCHAR(20),
	SKU VARCHAR(250),
	PARENT_ITEM_ID VARCHAR(50),
	PRODUCT_TYPE VARCHAR(250),
	COUPON_CODE VARCHAR(100),
	GROUP_ID NUMBER(38,0),
	GROUP_NAME VARCHAR(50),
	CATEGORY_ID NUMBER(38,0),
	CATEGORY_NAME VARCHAR(250),
	SUB_CATEGORY_ID NUMBER(38,0),
	SUB_CATEGORY_NAME VARCHAR(250),
	CLASS_ID NUMBER(38,0),
	CLASS_NAME VARCHAR(250),
	SUBCLASS_ID NUMBER(38,0),
	SUBCLASS_NAME VARCHAR(250),
	TRAN_QTY NUMBER(38,0),
	TRAN_AMT NUMBER(12,2),
	TRAN_FULL_PRICE NUMBER(12,2),
	TRAN_DISCOUNT_AMT NUMBER(12,2),
	TRAN_TAX_AMT NUMBER(12,2),
	BILLING_COUNTRY VARCHAR(50),
	BILLING_STATE VARCHAR(50),
	BILLING_CITY VARCHAR(50),
	SHIPPING_COUNTRY VARCHAR(50),
	SHIPPING_STATE VARCHAR(50),
	SHIPPING_CITY VARCHAR(50),
	MEMBERSHIP_TYPE_ID NUMBER(38,0),
	MEMBERSHIP_EXP_DATE TIMESTAMP_NTZ(9),
	ORIGINAL_ORDER_ID VARCHAR(50),
	CREDIT_MEMO_ID VARCHAR(50),
	MEMBERSHIP_START_DATE DATE,
	JE_BATCH_ID NUMBER(38,0),
	JE_BATCH_DATE TIMESTAMP_NTZ(9),
	GIFT_CARD_ID NUMBER(38,0),
	ITEM_TYPE VARCHAR(20),
	DISCOUNT_CODE VARCHAR(50),
	CANCEL_TYPE VARCHAR(250),
	COUPON_RULE_NAME VARCHAR(100),
	MEMBERSHIP_ID NUMBER(38,0),
	MEMBERSHIP_CANCEL_DATE TIMESTAMP_NTZ(9),
	CC_AUTH VARCHAR(20),
	CC_LAST4 VARCHAR(10),
	BANK_DATE DATE,
	DEPOSIT_DATE DATE,
	CREATED_AT TIMESTAMP_NTZ(9),
	FEE_AMT NUMBER(12,2),
	CC_TYPE VARCHAR(50),
	CC_RECON_DATE TIMESTAMP_NTZ(9),
	CC_RECON_AMT NUMBER(12,2),
	CC_PAY_AMT NUMBER(12,2),
	CM_ID NUMBER(38,0),
	GC_ID NUMBER(38,0),
	GC_TYPE NUMBER(38,0),
	GC_SUB_TYPE VARCHAR(25),
	GC_REASON_ID NUMBER(38,0),
	GC_REASON_NAME VARCHAR(50),
	GC_GROUP_CODE VARCHAR(25),
	GC_GROUP_NAME VARCHAR(50),
	INCREMENT_ID VARCHAR(50),
	REASON_ID NUMBER(38,0),
	REFUND_REASON VARCHAR(50),
	MAGENTO_LOCATION_ID NUMBER(38,0),
	COUPON_GROUP_CODE VARCHAR(100),
	RULE_ID NUMBER(38,0),
	RULE_NAME VARCHAR(150),
	SOURCE_UUID VARCHAR(50),
	SOURCE_LINE_NUM NUMBER(38,0),
	GL_GROUP_OVERRIDE VARCHAR(50),
	TRAN_COST_AMT NUMBER(12,2),
	JE_MAP_ID NUMBER(38,0),
	MESSAGE_UUID VARCHAR(250),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9),
	START_STATUS VARCHAR(50),
	END_STATUS VARCHAR(50),
	UPDATED_AT TIMESTAMP_NTZ(9),
	SALE_DATE DATE,
	ITEM_ID NUMBER(38,0),
	TRAN_SUB_TYPE_ID NUMBER(38,0),
	TRAN_GL_DATE DATE,
	SHIP_DATE DATE,
	ORIGINAL_TRAN_ID NUMBER(38,0),
	FC_SHIP_DATE TIMESTAMP_TZ(9),
	TRAN_COGS_AMT NUMBER(14,4),
	REBATE_START_DATE TIMESTAMP_TZ(9),
	REBATE_END_DATE TIMESTAMP_TZ(9),
	REBATE_PERCENTAGE FLOAT,
	REBATE_DOLLAR_AMOUNT FLOAT,
	REBATE_BILLING_PERIOD VARCHAR(100),
	REBATE_TYPE VARCHAR(102),
	REBATE_CALC_TYPE VARCHAR(103),
	REBATE_BILLING_METHOD VARCHAR(104),
	FULFILLMENT_TRAN_ID NUMBER(36,0),
	NS_TRAN_ID NUMBER(35,0),
	BRAND_MARKETING_ID NUMBER(35,0),
	REBATE_TOTAL NUMBER(12,6),
	rownumber number(38)
);

SELECT * FROM ods."TRANSACTIONS" tr WHERE tr.SALE_DATE ='2023-11-15'


create or replace TABLE TM_IGLOO_ODS_STG.ODS.ORDER_HEADER_DUPE (
	MESSAGE_UUID VARCHAR(100),
	ORDER_ID NUMBER(38,0) NOT NULL,
	INCREMENT_ID VARCHAR(100),
	ORDER_TYPE VARCHAR(100),
	ORIGINAL_ORDER_ID VARCHAR(20),
	CUSTOMER_ID NUMBER(38,0) NOT NULL,
	IS_AUTOSHIP VARCHAR(10),
	SECHEDULE_ID NUMBER(38,0),
	UPDATED_AT TIMESTAMP_NTZ(0),
	SENT_AT TIMESTAMP_NTZ(0),
	BASE_CURRENCY_CODE VARCHAR(10),
	ORDER_CURRENCY_CODE VARCHAR(10),
	BASE_TOTAL_ORDER NUMBER(10,2),
	BASE_TOTAL_PRODUCT NUMBER(10,2),
	BASE_TOTAL_DISCOUNT NUMBER(10,2),
	BASE_TOTAL_TAX NUMBER(10,2),
	BASE_DONATIONS NUMBER(10,2),
	BASE_SHIPMENT NUMBER(10,2),
	BASE_SHIPMENT_TAX NUMBER(10,2),
	BASE_TOTAL_PAYMENTS NUMBER(10,2),
	BASE_TOTAL_GIFT_CARDS NUMBER(10,2),
	BASE_TOTAL_THRIVE_CASH NUMBER(10,2),
	TOTAL_ORDER NUMBER(10,2),
	TOTAL_PRODUCT NUMBER(10,2),
	TOTAL_DISCOUNT NUMBER(10,2),
	TOTAL_TAX NUMBER(10,2),
	DONATIONS NUMBER(10,2),
	SHIPMENT NUMBER(10,2),
	SHIPMENT_TAX NUMBER(10,2),
	TOTAL_PAYMENTS NUMBER(10,2),
	TOTAL_GIFT_CARDS NUMBER(10,2),
	TOTAL_THRIVE_CASH NUMBER(10,2),
	BILLING_CITY VARCHAR(250),
	BILLING_STATE VARCHAR(100),
	BILLING_POSTAL_CODE VARCHAR(100),
	BILLING_COUNTRY VARCHAR(100),
	SHIPPING_CITY VARCHAR(250),
	SHIPPING_STATE VARCHAR(100),
	SHIPPING_POSTAL_CODE VARCHAR(100),
	SHIPPING_COUNTRY VARCHAR(100),
	MESSAGE_STATE_ID NUMBER(38,0),
	MESSAGE_STATE VARCHAR(100),
	INSERT_UTC_DATETIME TIMESTAMP_NTZ(9),
	UPDATE_UTC_DATETIME TIMESTAMP_NTZ(9),
	MESSAGE_TYPE VARCHAR(16777216),
	FROZEN_BASE_SHIPMENT NUMBER(10,2),
	FROZEN_BASE_SHIPMENT_TAX NUMBER(10,2),
	WINE_BASE_SHIPMENT NUMBER(10,2),
	WINE_BASE_SHIPMENT_TAX NUMBER(10,2),
	GROCERY_BASE_SHIPMENT NUMBER(10,2),
	GROCERY_BASE_SHIPMENT_TAX NUMBER(10,2),
	FROZEN_SHIPMENT NUMBER(10,2),
	GROCERY_SHIPMENT NUMBER(10,2),
	WINE_SHIPMENT NUMBER(10,2),
	FROZEN_SHIPMENT_TAX NUMBER(10,2),
	GROCERY_SHIPMENT_TAX NUMBER(10,2),
	WINE_SHIPMENT_TAX NUMBER(10,2),
	CREATED_AT TIMESTAMP_NTZ(9),
	ODS_PROCESSED_AT TIMESTAMP_NTZ(9),
	SALE_DATE DATE,
	GAAP_SALE_DATE DATE,
	STATUS VARCHAR(20),
	STATUS_DATE TIMESTAMP_NTZ(9),
	PPE_FEE NUMBER(10,4),
	PPE_FEE_TAX NUMBER(10,4),
	ROW_NUMBER number(38)
);

INSERT INTO ods.order_detail
SELECT * FROM ods.order_detail_dist

SELECT * FROM ods.order_detail WHERE sale_date = '2023-11-15'


GRANT ALL ON  ODS.TRANSACTIONS_TEST  TO FINANCE_BUS

-- TM_IGLOO_ODS_STG.ODS.ORDER_DETAIL definition

create or replace TABLE TM_IGLOO_ODS_STG.ODS.ORDER_DETAIL_DIST (
	ORDER_ID NUMBER(38,0) NOT NULL,
	ITEM_ID NUMBER(38,0) NOT NULL,
	PARENT_ITEM_ID VARCHAR(100),
	PRODUCT_ID NUMBER(38,0) NOT NULL,
	PRODUCT_TYPE VARCHAR(100),
	IS_VIRTUAL VARCHAR(100),
	SKU VARCHAR(100),
	IS_AUTOSHIP_SUBSCRIBED VARCHAR(10) NOT NULL,
	SUBSCRIPTION_QTY NUMBER(38,0),
	QTY_ORDERED NUMBER(38,0),
	BASE_ORIGINAL_PRICE NUMBER(10,4),
	BASE_PRICE NUMBER(10,4),
	TAX_PERCENT NUMBER(10,4),
	BASE_TAX_AMT NUMBER(10,4),
	DISCOUNT_PERCENT NUMBER(10,4),
	BASE_DISCOUNT_AMT NUMBER(10,4),
	BASE_ROW_TOTAL NUMBER(10,2),
	BASE_ROW_TOTAL_INCL_TAX NUMBER(10,2),
	ORIGINAL_PRICE NUMBER(10,4),
	PRICE NUMBER(10,4),
	TAX_AMT NUMBER(10,4),
	DISCOUNT_AMT NUMBER(10,4),
	ROW_TOTAL NUMBER(10,2),
	ROW_TOTAL_INCL_TAX NUMBER(10,2),
	FULFILLMENT_QTY NUMBER(38,0),
	SHORT_QTY NUMBER(38,0),
	RETURN_QTY NUMBER(38,0),
	INSERT_UTC_DATETIME TIMESTAMP_NTZ(9),
	UPDATE_UTC_DATETIME TIMESTAMP_NTZ(9),
	MESSAGE_TYPE VARCHAR(100),
	MESSAGE_UUID VARCHAR(100),
	SALE_DATE DATE,
	GAAP_SALE_DATE DATE,
	ASSIGNMENT_FC NUMBER(38,0),
	ASSIGNMENT_QTY NUMBER(38,0),
	FULFILLMENT_DATE TIMESTAMP_NTZ(9),
	EXT_COST_AMT NUMBER(12,2),
	ODS_UPDATED_AT TIMESTAMP_NTZ(9),
	IS_GWP BOOLEAN
);
