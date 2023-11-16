

getAdjDetail =   """
                select trn.ACCOUNT_ID,trn.AMOUNT,trn.CLASS_ID,trn.COMPANY_ID,trn.DATE_CREATED,trn.DATE_LAST_MODIFIED_GMT,trn.ITEM_ID,trn.LOCATION_ID,trn.MEMO,trn.TRANSACTION_ID,trn.TRANSACTION_LINE_ID,trn.TRANSACTION_ORDER
                from Administrator.TRANSACTION_LINES trn
                inner join Administrator.TRANSACTIONS tr on tr.transaction_id = trn.transaction_id
                where trn.MEMO ='Cost of Sales Adjustment' and tr.trandate between '2022-07-01' and TO_DATE(current_date - 1) 
                """

getAdjHeader =   """
                select trn.ACCOUNTING_PERIOD_ID,trn.CREATE_DATE,trn.STATUS,trn.TRANDATE,trn.TRANSACTION_ID,trn.TRANSACTION_NUMBER,trn.TRANSACTION_TYPE
                from Administrator.TRANSACTIONS trn
                inner join (select distinct trn.TRANSACTION_ID
                from Administrator.TRANSACTION_LINES trn
                where trn.MEMO ='Cost of Sales Adjustment') tl on tl.transaction_id = trn.TRANSACTION_ID 
                where trn.trandate between '2022-07-01' and TO_DATE(current_date - 1) 
                """

spPostAdj = {
                    "1 - Insert new rows into ODS.NS_COSTADJ_HEADER":
                    """
                    INSERT INTO ODS.NS_COSTADJ_HEADER
                    SELECT snj.*, sysdate()
                    FROM staging.STG_NS_COSTADJ_HEADER snj
                    LEFT OUTER JOIN ODS.NS_COSTADJ_HEADER nj ON snj.transaction_id = nj.transaction_id
                    WHERE nj.transaction_id IS NULL 

                    """,
                    "2 - Insert new rows into ODS.NS_COSTADJ_DETAIL":
                    """
                    INSERT INTO ODS.NS_COSTADJ_DETAIL 
                    SELECT  snd.*,sysdate() 
                    FROM staging.STG_NS_COSTADJ_DETAIL snd
                    LEFT OUTER JOIN ODS.NS_COSTADJ_DETAIL nd on snd.transaction_id = nd.transaction_id AND snd.ITEM_ID =nd.ITEM_ID AND snd.TRANSACTION_LINE_ID =nd.TRANSACTION_LINE_ID 
                    INNER JOIN STAGING.STG_NS_COSTADJ_HEADER snh ON snd.TRANSACTION_ID = snh.TRANSACTION_ID 
                    WHERE nd.transaction_id IS null
                    """,
                    " 3 -  Create Vendor Rebates on Cost Adjustments":
                    """
                    INSERT INTO ods."TRANSACTIONS" (sku,tran_type ,TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE,tran_date ,TRAN_COGS_AMT,CREATED_AT,ITEM_ID,TRAN_GL_DATE,LOCATION_ID,REBATE_START_DATE,REBATE_END_DATE,REBATE_PERCENTAGE,REBATE_DOLLAR_AMOUNT,REBATE_BILLING_PERIOD,REBATE_TYPE,REBATE_CALC_TYPE,TRAN_AMT,REBATE_BILLING_METHOD,NS_TRAN_ID,REBATE_TOTAL)
                    SELECT ci.ITEM_name,300,111,'Vendor Rebates',he.TRANDATE::TIMESTAMP_NTZ ,de.amount, de.DATE_CREATED::TIMESTAMP_NTZ ,de.ITEM_ID , he.TRANDATE,de.LOCATION_ID ,
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
                    END AS rebate_totalamount	
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
                    """
                    }