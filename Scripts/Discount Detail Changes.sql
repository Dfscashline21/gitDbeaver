ALTER Table staging.STG_ORDER_CAPTURE_DETAIL_DISCOUNTS_V1 
ADD COLUMN SPONSORED_BY varchar(100), SPONSORED_PERCENTAGE float;


ALTER Table STAGING.STG_ORDER_CAPTURE_DETAIL_DISCOUNTS_V1_PROCESSED 
ADD COLUMN SPONSORED_BY varchar(100), SPONSORED_PERCENTAGE float;


ALTER Table ods.ORDER_DETAIL_DISCOUNTS 
ADD COLUMN SPONSORED_BY varchar(100), SPONSORED_PERCENTAGE float;

SELECT * FROM staging.ODS_MESSAGE_QUEUE 
 

--
--                        
--SELECT * FROM staging.STG_ORDER_CAPTURE_DETAIL_DISCOUNTS_V1
--
---- TM_IGLOO_ODS_STG.STAGING.STG_ORDER_CAPTURE_DETAIL_DISCOUNTS_V1_PROCESSED definition
--
--create or replace TABLE TM_IGLOO_ODS_STG.STAGING.tempdiscounts (
--	ORDER_ID NUMBER(38,0) NOT NULL,
--	ITEM_ID NUMBER(38,0) NOT NULL,
--	DISCOUNT_ID NUMBER(38,0) NOT NULL,
--	DISCOUNT_TYPE VARCHAR(100),
--	RULE_ID NUMBER(38,0) NOT NULL,
--	RULE_PCT_OFF NUMBER(10,2),
--	RULE_AMT_OFF NUMBER(10,2),
--	PCT_OFF_APPLIED NUMBER(12,4),
--	START_PRICE NUMBER(10,2),
--	BASE_DISCOUNT_AMT NUMBER(12,4),
--	DISCOUNT_AMT NUMBER(12,4),
--	UNITS_APPLIED NUMBER(38,0),
--	BASE_AMT_OFF_APPLIED NUMBER(12,4),
--	AMT_OFF_APPLIED NUMBER(12,4),
--	BASE_TAX_DISCOUNT NUMBER(12,4),
--	TAX_DISCOUNT NUMBER(12,4),
--	BASE_DUTY_DISCOUNT NUMBER(12,4),
--	DUTY_DISCOUNT NUMBER(12,4),
--	INSERT_UTC_DATETIME TIMESTAMP_NTZ(9),
--	UPDATE_UTC_DATETIME TIMESTAMP_NTZ(9),
--	MESSAGE_UUID VARCHAR(16777216),
--	RULE_NAME VARCHAR(500),
--	RULE_GROUP VARCHAR(500),
--	SPONSORED_BY varchar(100), 
--	SPONSORED_PERCENTAGE float,
--	ODS_PROCESSED_AT TIMESTAMP_NTZ(9)
--);
--
--
--
--SELECT * FROM STAGING.STG_ORDER_CAPTURE_DETAIL_DISCOUNTS_V1_PROCESSED  
--
--SELECT * FROM ods.ORDER_DETAIL_DISCOUNTS
--
--SELECT * FROM STAGING.ODS_MESSAGE_QUEUE 
--
--SELECT * FROM curr_items WHERE PRIMARY_VENDOR_ID =40863
--
--INSERT INTO STAGING.ODS_MESSAGE_QUEUE (id, CREATED_AT, S3_FILENAME,S3_LINE_NUM, MESSAGE_TYPE, MESSAGE_UUID, ORDER_ID, JSON_DATA)
--select UNIFORM(1,100000, random()), current_timestamp, 'test 2',1,'oc', UUID_STRING(), 12115405345,
-- parse_json($${"base_currency_code":"USD","base_donations":100,"base_total_discount":15,"base_total_gift_cards":0,"base_total_order":224.54,"base_total_payments":224.54,"base_total_product":100.95,"base_total_tax":9.59,"base_total_thrive_cash":0,"billing_city":"LOS ANGELES","billing_country":"US","billing_postal_code":"90003","billing_state":"CA","created_at":"2023-11-09T19:16:33+00:00","customer_id":1079,"donations":100,"frozen_base_shipment":0,"frozen_base_shipment_tax":0,"frozen_shipment":0,"frozen_shipment_tax":0,"grocery_base_shipment":0,"grocery_base_shipment_tax":0,"grocery_shipment":0,"grocery_shipment_tax":0,"increment_id":"1910004145","is_autoship":false,"is_autoship_customer":true,"items":[{"base_discount_amt":0.35,"base_original_price":7.45,"base_price":7.1,"base_row_total":7.100000000000001,"base_row_total_incl_tax":7.77,"base_tax_amt":0.67,"discount_amt":0.35,"discount_detail":[{"amt_off_applied":0.35,"base_amt_off_applied":0.35,"base_discount_amt":0.35,"base_duty_discount":0,"base_tax_discount":0.0315,"discount_amt":0.35,"discount_type":"coupon","duty_discount":0,"pct_off_applied":4.698,"rule_amt_off":0,"rule_group":"101: Finance, TAO","rule_id":95,"rule_name":"Custom 5$","rule_pct_off":0,"sponsored_by":"Brand","sponsored_percentage":.57,"start_price":7.45,"tax_discount":0.0315,"units_applied":1}],"discount_percent":4.7,"is_autoship_subscribed":false,"is_gwp":0,"is_virtual":false,"item_id":6182,"original_price":7.45,"parent_item_id":0,"price":7.1,"product_id":777,"product_type":"simple","qty_ordered":1,"row_total":7.100000000000001,"row_total_incl_tax":7.77,"sku":"858847000680","subscription_qty":0,"tax_amt":0.67,"tax_percent":9},{"base_discount_amt":0.17,"base_original_price":3.55,"base_price":3.38,"base_row_total":3.38,"base_row_total_incl_tax":3.7,"base_tax_amt":0.32,"discount_amt":0.17,"discount_detail":[{"amt_off_applied":0.17,"base_amt_off_applied":0.17,"base_discount_amt":0.17,"base_duty_discount":0,"base_tax_discount":0.01615,"discount_amt":0.17,"discount_type":"coupon","duty_discount":0,"pct_off_applied":4.7887,"rule_amt_off":0,"rule_group":"101: Finance, TAO","rule_id":95,"rule_name":"Custom 5$","rule_pct_off":0,"sponsored_by":"Brand","sponsored_percentage":1,"start_price":3.55,"tax_discount":0.01615,"units_applied":1}],"discount_percent":4.79,"is_autoship_subscribed":true,"is_gwp":0,"is_virtual":false,"item_id":6181,"original_price":3.55,"parent_item_id":0,"price":3.38,"product_id":589,"product_type":"simple","qty_ordered":1,"row_total":3.38,"row_total_incl_tax":3.7,"sku":"077326830932","subscription_qty":3,"tax_amt":0.32,"tax_percent":9.5},{"base_discount_amt":0.23,"base_original_price":4.95,"base_price":4.72,"base_row_total":4.72,"base_row_total_incl_tax":5.17,"base_tax_amt":0.45,"discount_amt":0.23,"discount_detail":[{"amt_off_applied":0.23,"base_amt_off_applied":0.23,"base_discount_amt":0.23,"base_duty_discount":0,"base_tax_discount":0.02185,"discount_amt":0.23,"discount_type":"coupon","duty_discount":0,"pct_off_applied":4.6465,"rule_amt_off":0,"rule_group":"101: Finance, TAO","rule_id":95,"rule_name":"Custom 5$","rule_pct_off":0,"sponsored_by":"Brand","sponsored_percentage":100,"start_price":4.95,"tax_discount":0.02185,"units_applied":1}],"discount_percent":4.65,"is_autoship_subscribed":false,"is_gwp":0,"is_virtual":false,"item_id":6183,"original_price":4.95,"parent_item_id":0,"price":4.72,"product_id":783,"product_type":"simple","qty_ordered":1,"row_total":4.72,"row_total_incl_tax":5.17,"sku":"858847000956","subscription_qty":0,"tax_amt":0.45,"tax_percent":9.5},{"base_discount_amt":14.25,"base_original_price":100,"base_price":85.75,"base_row_total":85.75,"base_row_total_incl_tax":93.90000000000001,"base_tax_amt":8.15,"discount_amt":14.25,"discount_detail":[{"amt_off_applied":10,"base_amt_off_applied":10,"base_discount_amt":10,"base_duty_discount":0,"base_tax_discount":0.95,"discount_amt":10,"discount_type":"tpa","duty_discount":0,"pct_off_applied":10,"rule_amt_off":0,"rule_id":2,"rule_name":"10% off","rule_pct_off":10,"start_price":100,"tax_discount":0.95,"units_applied":1},{"amt_off_applied":4.25,"base_amt_off_applied":4.25,"base_discount_amt":4.25,"base_duty_discount":0,"base_tax_discount":0.40375,"discount_amt":4.25,"discount_type":"coupon","duty_discount":0,"pct_off_applied":4.25,"rule_amt_off":0,"rule_group":"101: Finance, TAO","rule_id":95,"rule_name":"Custom 5$","rule_pct_off":0,"sponsored_by":"Brand","sponsored_percentage":50,"start_price":90,"tax_discount":0.40375,"units_applied":1}],"discount_percent":14.25,"is_autoship_subscribed":true,"is_gwp":0,"is_virtual":false,"item_id":6184,"original_price":100,"parent_item_id":0,"price":85.75,"product_id":1545,"product_type":"simple","qty_ordered":1,"row_total":85.75,"row_total_incl_tax":93.90000000000001,"sku":"089836187611","subscription_qty":1,"tax_amt":8.15,"tax_percent":9.5}],"message_type":"Order Capture","message_uuid":"247417ed-4f1a-0071-f0e3-1ff315cc1a24","order_currency_code":"USD","order_id":10105,"order_type":"Order","original_order_id":"","payments":{"payment_detail":[{"id":"3127","payment_gateway":"braintree","cc_type":"VI","pay_amt":224.54,"base_pay_amt":224.54,"gateway_tran_id":"g3mc3xmz","cc_tran_id":"g3mc3xmz","cc_last_four":"1111"}],"ebt_detail":[],"gift_card_details":[],"thrive_cash_applied_details":{"applied_amt":0}},"ppe_fee":0,"ppe_fee_tax":0,"schedule_id":0,"sent_at":"2023-11-09T19:16:41+00:00","shipping_city":"LOS ANGELES","shipping_country":"US","shipping_postal_code":"90003","shipping_state":"CA","total_discount":15,"total_gift_cards":0,"total_order":224.54,"total_payments":224.54,"total_product":100.95,"total_tax":9.59,"total_thrive_cash":0,"updated_at":"2023-11-09T19:16:40+00:00","wine_base_shipment":14,"wine_base_shipment_tax":0,"wine_shipment":14,"wine_shipment_tax":0}$$)
-- 
-- 
-- SELECT
--                        (json_data:order_id)::int  as order_id
--                        ,itm.value:item_id::int
--                        ,row_number() over (partition by json_data:order_id order by json_data:order_id, itm.value:item_id) as discount_id
--                        ,dd.value:discount_type::varchar    
--                        ,dd.value:rule_id::int
--                        ,dd.value:rule_pct_off::numeric(12,2)
--                        ,dd.value:rule_amt_off::numeric(12,2)
--                        ,dd.value:pct_off_applied::numeric(12,4)
--                        ,dd.value:start_price::numeric(12,2)
--                        ,dd.value:base_discount_amt::numeric(12,4)
--                        ,dd.value:discount_amt::numeric(12,4)
--                        ,dd.value:units_applied::int
--                        ,dd.value:base_amt_off_applied::numeric(12,4)
--                        ,dd.value:amt_off_applied::numeric(12,4)
--                        ,dd.value:base_tax_discount::numeric(12,4)
--                        ,dd.value:tax_discount::numeric(12,4)
--                        ,dd.value:base_duty_discount::numeric(12,4)
--                        ,dd.value:duty_discount::numeric(12,4)
--                        ,current_timestamp as insert_utc_datetime
--                        ,current_timestamp as update_utc_datetime
--                        ,json_data:message_uuid::varchar  as message_uuid
--                        ,dd.value:rule_name::varchar
--                        ,dd.value:rule_group::varchar
--                        ,dd.value:sponsored_by::varchar
--                        ,dd.value:sponsored_percentage::float  FROM staging.ODS_MESSAGE_QUEUE, LATERAL flatten(input => json_data:items) itm,
--                        LATERAL flatten(INPUT => itm.value:discount_detail) dd WHERE ORDER_ID = '100000001'
--                        