

getVendRebates =   """
                    select ra.REBATE_AGREEMENT_ID ,ra.ACTIVE_STATUS_ID ,ac.LIST_ITEM_NAME ,ra.BILLING_PERIOD_ID,bp.LIST_ITEM_NAME ,ra.BRAND_ID,
                    br.BRAND_RECORDS_NAME  ,ra.START_DATE ,ra.END_DATE,ra.DOLLAR_AMOUNT ,ra.PERCENTAGE ,ra.ITEM_ID ,ra.IS_INACTIVE,
                    ra.TYPE_ID  , rt.LIST_ITEM_NAME 
                    ,br.BILLING_METHOD_ID,br.BILLING_CUSTOMER_ID ,vm.VENDOR_ID 
                    ,CASE 
                        when ra.item_id is null then 'By Brand'
                        else 'By SKU'
                    END as 'Brand/SKU',sysdate(),COALESCE(cnt.EMAIL,cst.EMAIL,vnd.email,csts.email,vndr.email)
                    from Administrator.REBATE_AGREEMENT ra
                    left join Administrator.REBATE_BILLING_PERIOD_LIST bp on bp.LIST_ID = ra.BILLING_PERIOD_ID 
                    left join Administrator.EDI_PRICE_ACTIVE_STATUS ac on ac.LIST_ID = ra.ACTIVE_STATUS_ID 
                    left join Administrator.BRAND_RECORDS br on ra.BRAND_ID = br.BRAND_RECORDS_ID 
                    left join Administrator.REBATE_TYPE rt on rt.LIST_ID = ra.TYPE_ID 
                    left join Administrator.BRAND_RECORDS_BILLING_VENDOR_MAP vm on vm.BRAND_RECORDS_ID =br.BRAND_RECORDS_ID 
                    left join (select c.brand_records_id as BRAND_RECORDS_ID , Max(c.Contact_id) as CONTACT_ID from Administrator.BRAND_RECORDS_VENDOR_CONTACT_MAP c group by c.BRAND_RECORDS_ID )cm on cm.BRAND_RECORDS_ID  = ra.BRAND_ID 
                    left join (select CONTACT_ID as CONTACT_ID, EMAIL as EMAIL from Administrator.CONTACTS )cnt on cnt.CONTACT_ID = cm.CONTACT_ID
                    left join (select cust.CUSTOMER_ID ,cust.EMAIL from Administrator.CUSTOMERS cust)cst on cst.customer_id = br.billing_customer_id
                    left join (select cust.NAME ,cust.EMAIL from Administrator.CUSTOMERS cust)csts on csts.NAME = br.BRAND_RECORDS_NAME 
                    left join (select  vendor_id,email from Administrator.VENDORS ) vnd on vnd.VENDOR_ID = vm.VENDOR_ID 
                    left join (select  NAME,EMAIL from Administrator.VENDORS ) vndr on vndr.NAME = br.BRAND_RECORDS_NAME 
                    where ra.type_Id = 1
                """

spPostRebates = {
                    "1 - Insert new rows into ODS.NS_VENDOR_REBATES":
                    """
                  INSERT INTO ODS.NS_VENDOR_REBATES
                SELECT svr.* FROM staging.STG_VENDOR_REBATES svr
                LEFT OUTER JOIN ods.ns_vendor_rebates nvr ON svr.rebate_agreement_id = nvr.rebate_agreement_id
                WHERE nvr.rebate_agreement_id IS NULL  
                        """,
                    "2 - Update existing records in ODS.NS_VENDOR_REBATE":
                    """
                  UPDATE ods.NS_VENDOR_REBATES 
                      SET 
                      NS_VENDOR_REBATES.BILLING_PERIOD = upd.BILLING_PERIOD, 
                      NS_VENDOR_REBATES.BRAND_RECORDS_NAME  = upd.BRAND_RECORDS_NAME, 
                      NS_VENDOR_REBATES.DOLLAR_AMOUNT  = upd.DOLLAR_AMOUNT,
                      NS_VENDOR_REBATES.PERCENTAGE= upd.PERCENTAGE, 
                      NS_VENDOR_REBATES.BILLING_METHOD_ID  = upd.BILLING_METHOD_ID  , 
                      NS_VENDOR_REBATES.BILLING_CUSTOMER_ID = upd.BILLING_CUSTOMER_ID, 
                      NS_VENDOR_REBATES.VENDOR_ID  = upd.VENDOR_ID,
                      NS_VENDOR_REBATES.BRANDEMAIL  = upd.BRANDEMAIL
                      FROM (SELECT * FROM staging.STG_VENDOR_REBATES) upd
                      WHERE NS_VENDOR_REBATES.REBATE_AGREEMENT_ID  = upd.REBATE_AGREEMENT_ID
                    
                    """
}