

getBrandMarketing =   """
                select bm.*, ty.LIST_ITEM_NAME , em.EMAIL ,em.FULL_NAME ,em.JOBTITLE , br.BRAND_RECORDS_NAME , br.BILLING_CUSTOMER_ID,vm.VENDOR_ID  , ct.COMPANYNAME , ct.EMAIL
                , ct.FULL_NAME , bil.LIST_ITEM_NAME as billing_method,sysdate(),COALESCE(cnt.EMAIL,cst.EMAIL,vnd.email) from Administrator.VENDOR_FUNDING__BRAND_MARKETI bm
                left join Administrator.EMPLOYEES em on em.EMPLOYEE_ID = bm.MERCH_MARKETING_CONTACT_ID 
                left join Administrator.BRAND_RECORDS br on br.BRAND_RECORDS_ID = bm.BRAND_ID 
                left join Administrator.CUSTOMERS ct on ct.CUSTOMER_ID = br.BILLING_CUSTOMER_ID 
                left join Administrator.BILLING_METHOD bil on bil.LIST_ID =  br.BILLING_METHOD_ID 
                left join Administrator.VENDOR_MARKETING_ASSET_TYPES ty on ty.LIST_ID = bm.ASSET_TYPE_ID 
                left join Administrator.BRAND_RECORDS_BILLING_VENDOR_MAP vm on vm.BRAND_RECORDS_ID =br.BRAND_RECORDS_ID 
                left join (select c.brand_records_id as BRAND_RECORDS_ID , Max(c.Contact_id) as CONTACT_ID from Administrator.BRAND_RECORDS_VENDOR_CONTACT_MAP c group by c.BRAND_RECORDS_ID )cm on cm.BRAND_RECORDS_ID  = bm.BRAND_ID 
                left join (select CONTACT_ID as CONTACT_ID, EMAIL as EMAIL from Administrator.CONTACTS )cnt on cnt.CONTACT_ID = cm.CONTACT_ID
                left join (select cust.CUSTOMER_ID ,cust.EMAIL from Administrator.CUSTOMERS cust)cst on cst.customer_id = br.billing_customer_id
                left join (select  vendor_id,email from Administrator.VENDORS ) vnd on vnd.VENDOR_ID = vm.VENDOR_ID 

                """

spPostBrandMarketing = {
                    "1 - Insert new rows into ODS.BRAND_MARKETING":
                    """
                    INSERT INTO ODS.BRAND_MARKETING
                    SELECT  br.* FROM STAGING.stg_brand_marketing br
                    LEFT OUTER JOIN ods.brand_marketing bm ON br.VENDOR_FUNDING__BRAND_MARKE_ID = bm.VENDOR_FUNDING__BRAND_MARKE_ID
                    WHERE bm.VENDOR_FUNDING__BRAND_MARKE_ID IS NULL 
                    """,
                    "2 - Update existing records in ODS.BRAND_MARKETING":
                    """
                  UPDATE ods.BRAND_MARKETING 
                      SET 
                      BRAND_MARKETING.BILLING_CUSTOMER_ID = upd.BILLING_CUSTOMER_ID, 
                      BRAND_MARKETING.BILLING_VENDOR_ID  = upd.BILLING_VENDOR_ID, 
                      BRAND_MARKETING.BILLING_METHOD  = upd.BILLING_METHOD ,
                      BRAND_MARKETING.BRANDEMAIL = upd.BRANDEMAIL
                      FROM (SELECT * FROM staging.STG_BRAND_MARKETING) upd
                      WHERE BRAND_MARKETING.VENDOR_FUNDING__BRAND_MARKE_ID  = upd.VENDOR_FUNDING__BRAND_MARKE_ID"""
                    ,
                    "3 - create Brand Marketing transactions":
                    """
                    insert into ods.transactions (TRAN_TYPE,TRAN_SUB_TYPE,TRAN_DATE,COMPANY_ID,CREATED_AT,TRAN_SUB_TYPE_ID,TRAN_GL_DATE,TRAN_AMT,BRAND_MARKETING_ID)
                    SELECT 300,'Brand Marketing Fees',bm.START_DATE::TIMESTAMP_NTZ,'1',bm.ODS_PROCESSED_DATE::TIMESTAMP_NTZ,112,bm.START_DATE ::date ,bm.TOTAL_COST, bm.VENDOR_FUNDING__BRAND_MARKE_ID  
                    FROM ods.brand_marketing bm
                    FULL OUTER JOIN (SELECT DISTINCT tr.BRAND_MARKETING_ID FROM ods."TRANSACTIONS" tr ) trn ON trn.brand_marketing_id = bm.VENDOR_FUNDING__BRAND_MARKE_ID 
                    WHERE trn.brand_marketing_iD IS NULL AND bm.START_DATE IS NOT null
                    """

}