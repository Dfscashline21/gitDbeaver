-- VFI JE Template

WITH journal AS (SELECT CASE 
	WHEN fd.tran_sub_type_id = 111 THEN '51035 Cost of Goods Sold : Rebates from Vendors'
	WHEN fd.tran_sub_type_id = 112 AND fd.vendor_funding_item IN ('Reviews','Review Incentives') THEN '43105 - Referral Credit - Vendor Funds Earned' 
	WHEN fd.tran_sub_type_id = 112  THEN '51041 - Brand Marketing Fees' 
	WHEN fd.tran_sub_type_id = 113 THEN '43113 Marketplace Revenue : TPA - Vendor Funds Earned'
	WHEN fd.tran_sub_type_id = 114 THEN '43114 Marketplace Revenue : TPA - Autoship & Save - Vendor funds earned'
END AS acct, NULL AS debit
,CASE 
	WHEN fd.tran_sub_type_id = 111 THEN fd.rebate_total
	WHEN fd.tran_sub_type_id = 112 THEN round(fd.tran_amt,2)
	WHEN fd.tran_sub_type_id = 113 THEN round(fd.rebate_total,2)
	WHEN fd.tran_sub_type_id = 114 THEN round(fd.rebate_total,2)
END AS credit,
fd.MONTHNAME ||' '|| fd.CALENDARYEAR || ' Vendor Funding Accrual' AS memo
,fd.MONTHNAME ||' '|| fd.CALENDARYEAR ||' '|| fd.tran_sub_type ||' Accrual' AS linememo,
NULL AS department,
fd.month_end_date AS jedate,
fd.month_end_date + 1 AS reversaldate
FROM (SELECT * FROM ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF ns ON ns.ns_fc_id =  tr.LOCATION_ID 
LEFT JOIN ods.CURR_ITEMS cii ON cii.item_name = tr.sku AND COALESCE(cii.fc_id,2) = ns.ods_fc_id
LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = tr.sku AND COALESCE(ci.fc_id,2) = ns.ods_fc_id
LEFT JOIN ods.BRAND_RECORDS br ON br.BRAND_RECORDS_ID =COALESCE(ci.BRAND_ID,cii.brand_id)
LEFT JOIN ods.CAL_LU cl ON cl.fulldate = tr.tran_gl_date
LEFT JOIN ods.BRAND_MARKETING bm ON tr.brand_marketing_id = bm.VENDOR_FUNDING__BRAND_MARKE_ID
WHERE tr.TRAN_SUB_TYPE_ID IN (111,112,113,114) and tr.tran_gl_date BETWEEN $P{start_date} AND $P{end_date} AND (br.billing_method_id in (1,2,3) OR bm.billing_method IN ('UNFI Bill','AP Credit','AR Invoice') ) ) fd),
test AS (SELECT CASE 
	WHEN fd.billing_method_id =1 OR fd.billing_method = 'AP Credit' THEN '20103 Accounts Payable : Vendor Funding Credits'
	WHEN fd.billing_method_id IN(2,3) OR fd.billing_method IN ('AR Invoice','UNFI Bill')  THEN '12010 Unbilled Accounts Receivable'
END AS acct, CASE 
	WHEN fd.tran_sub_type_id = 111 THEN fd.rebate_total
	WHEN fd.tran_sub_type_id = 112 THEN round(fd.tran_amt,2)
	WHEN fd.tran_sub_type_id = 113 THEN round(fd.rebate_total,2)
	WHEN fd.tran_sub_type_id = 114 THEN round(fd.rebate_total,2)
END AS debit
,NULL AS credit,
fd.MONTHNAME ||' '|| fd.CALENDARYEAR || ' Vendor Funding Accrual' AS memo
,fd.MONTHNAME ||' '|| fd.CALENDARYEAR ||' '|| fd.tran_sub_type ||' Accrual' AS linememo,
NULL AS department,
fd.month_end_date AS jedate,
fd.month_end_date + 1 AS reversaldate
FROM (SELECT * FROM ods."TRANSACTIONS" tr
LEFT JOIN ods.NS_FC_XREF ns ON ns.ns_fc_id =  tr.LOCATION_ID 
LEFT JOIN ods.CURR_ITEMS cii ON cii.item_name = tr.sku AND COALESCE(cii.fc_id,2) = ns.ods_fc_id
LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = tr.sku AND COALESCE(ci.fc_id,2) = ns.ods_fc_id
LEFT JOIN ods.BRAND_RECORDS br ON br.BRAND_RECORDS_ID =COALESCE(ci.BRAND_ID,cii.brand_id)
LEFT JOIN ods.CAL_LU cl ON cl.fulldate = tr.tran_gl_date
LEFT JOIN ods.BRAND_MARKETING bm ON tr.brand_marketing_id = bm.VENDOR_FUNDING__BRAND_MARKE_ID
WHERE tr.TRAN_SUB_TYPE_ID IN (111,112,113,114) and tr.tran_gl_date BETWEEN $P{start_date} AND $P{end_date} AND (br.billing_method_id in (1,2,3) OR bm.billing_method IN ('UNFI Bill','AP Credit','AR Invoice') and tr.cancel_type is null  )) fd)
SELECT ACCT,sum(DEBIT)AS debit,sum(CREDIT) AS credit ,MEMO,LINEMEMO,DEPARTMENT,JEDATE,REVERSALDATE FROM journal je 
GROUP BY ACCT,MEMO,LINEMEMO,DEPARTMENT,JEDATE,REVERSALDATE
UNION ALL 
SELECT ACCT,sum(DEBIT)AS debit,sum(CREDIT) AS credit ,MEMO,LINEMEMO,DEPARTMENT,JEDATE,REVERSALDATE FROM test  
GROUP BY ACCT,MEMO,LINEMEMO,DEPARTMENT,JEDATE,REVERSALDATE

