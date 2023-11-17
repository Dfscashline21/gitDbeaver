connPostgresToAuroraODS = {'target_conn_type': 'postgres',
                           'target_conn_id': 'Aurora_ODS'
                           }

spOrderCapture = {
    "0 - Temp fix for thrive_zero_cash orders test":
        """
        update staging.stg_order_capture_items_v1
        set discount_amt = 0, base_discount_amt = 0, discount_percent = 0
        from(
                select od.item_id
                from staging.stg_order_capture_payments_v1 op
                inner join staging.stg_order_capture_items_v1 od on op.order_id = od.order_id
                where payment_gateway = 'thrive_zero_charge'  and od.price = 0) upd
        where stg_order_capture_items_v1.item_id = upd.item_id
        """,
    "0.5 - Temp fix for gift card negative discount":
        """
        update staging.stg_order_capture_items_v1
        set discount_amt = 0, base_discount_amt = 0, discount_percent = 0
        from(
                select item_id
                from staging.stg_order_capture_items_v1
                where coalesce(discount_amt,0) < 0 and product_type = 'aw_giftcard') upd
        where stg_order_capture_items_v1.item_id = upd.item_id
        """,
    "1 - Truncate work file":
        "truncate staging.order_work_list",
    "2 - Create temp list of orders":
        """
        insert into staging.order_work_list
        select order_id, message_uuid
        from staging.stg_order_capture_v1
        """,
    "3 - Change order_type to Reshipment for thrive_zero_charge orders":
        """
        update staging.stg_order_capture_v1
        set order_type = 'Reshipment'
        from(
                select oh.order_id
                from staging.stg_order_capture_payments_v1 op
                inner join staging.stg_order_capture_v1 oh on op.order_id = oh.order_id
                where payment_gateway = 'thrive_zero_charge') upd 
        where stg_order_capture_v1.order_id = upd.order_id
        """,
    "10 - Create Braintree payment transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt, cc_auth, cc_last4, cc_type,
        cc_pay_amt,  created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 100, 6, 'Payment Capture' as tran_sub_type, soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1, sop.base_pay_amt,
        sop.cc_tran_id, sop.cc_last_four, case when coalesce(sop.cc_type, '') <> '' then sop.cc_type else sop.payment_gateway end as cc_type,
        coalesce(sop.base_pay_amt,0), current_timestamp, soc.message_uuid, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        left outer join staging.stg_order_capture_payments_v1 sop on soc.order_id = sop.order_id
        """,
    "11- Create gift card payment transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt,
         created_at, gc_sub_type, gc_type, gift_card_id, updated_at, sale_date, tran_gl_date)
        select 100, 9, 'Gift Card Capture' as tran_sub_type, soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1, sogc.gift_card_amt,
        current_timestamp, case when coalesce(gc.issue_type, gcpo.issue_type) = 'other' then 'Make-Good' else INITCAP(coalesce(gc.issue_type, gcpo.issue_type)) end ,
         1, sogc.gift_card_id::int, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        left outer join staging.stg_order_capture_gift_cards_v1 sogc on soc.order_id = sogc.order_id
        left outer join ods.gift_cards gc on sogc.gift_card_id::int = gc.card_id
        left outer join ods.gift_cards_pre_ods gcpo on sogc.gift_card_id = gcpo.giftcard_id::varchar(50)
        where sogc.gift_card_id is not null
        """,
    "12 - Create product tax transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt,
        created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 105, 13, 'Tax Capture' as tran_sub_type, soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1,
        (coalesce(soc.base_total_tax,0) - coalesce(PPE_FEE_TAX ,0) - coalesce(base_shipment_tax,0) - coalesce(grocery_base_shipment_tax,0) - coalesce(frozen_base_shipment_tax,0) - coalesce(wine_base_shipment_tax,0)) as tax_amt,
        current_timestamp, soc.message_uuid, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        where (coalesce(soc.base_total_tax,0) - coalesce(PPE_FEE_TAX ,0) - coalesce(base_shipment_tax,0) - coalesce(grocery_base_shipment_tax,0) - coalesce(frozen_base_shipment_tax,0) - coalesce(wine_base_shipment_tax,0)) > 0
        """,
    "13 -Create shipping tax transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt,
        created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 105, 28, 'Shipping Tax Capture' as tran_sub_type, soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1,
        ( coalesce(base_shipment_tax,0) + coalesce(grocery_base_shipment_tax,0) + coalesce(frozen_base_shipment_tax,0) + coalesce(wine_base_shipment_tax,0)) as tax_amt,
        current_timestamp, soc.message_uuid, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        where (coalesce(base_shipment_tax,0) + coalesce(grocery_base_shipment_tax,0) + coalesce(frozen_base_shipment_tax,0) + coalesce(wine_base_shipment_tax,0)) > 0
        """,
    "13.5 - Create ppe tax transaction":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt,
        created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 105, 95, 'PPE Tax Capture' as tran_sub_type, soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1,
        coalesce(ppe_fee_tax,0) as tax_amt,
        current_timestamp, soc.message_uuid, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        where coalesce(ppe_fee_tax,0)  > 0
        """,
    "14 -Create donation transactions":
        """
       insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id,
         tran_amt, created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 400, 5, 'Donation', soc.created_at, soc.order_type, soc.order_id, soc.increment_id, soc.customer_id, 1, 1, soc.base_donations , 
        current_timestamp, soc.message_uuid, current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date,
        convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        where coalesce(base_donations,0) > 0
        """,
    "15 - Copy stage data to ods.order_header":
        """
         insert into ods.order_header ( message_uuid, order_id, increment_id, order_type, original_order_id, customer_id, is_autoship, sechedule_id, updated_at, sent_at, base_currency_code, 
        order_currency_code, base_total_order, base_total_product, base_total_discount, base_total_tax, base_donations, base_shipment, base_shipment_tax, 
        base_total_payments, base_total_gift_cards, base_total_thrive_cash, total_order, total_product, total_discount, total_tax, donations, shipment, shipment_tax, 
        total_payments, total_gift_cards, total_thrive_cash, billing_city, billing_state, billing_postal_code, billing_country, shipping_city, shipping_state, shipping_postal_code, 
        shipping_country, message_state_id, message_state, insert_utc_datetime, update_utc_datetime, message_type, frozen_base_shipment, frozen_base_shipment_tax, 
        wine_base_shipment, wine_base_shipment_tax, grocery_base_shipment, grocery_base_shipment_tax, frozen_shipment, grocery_shipment, wine_shipment, frozen_shipment_tax, grocery_shipment_tax, 
        wine_shipment_tax, created_at, ods_processed_at, sale_date, ppe_fee, ppe_fee_tax, status, status_date )
        select soc.message_uuid, soc.order_id, soc.increment_id, soc.order_type, soc.original_order_id, soc.customer_id, soc.is_autoship, 
        soc.sechedule_id, soc.updated_at, soc.sent_at, soc.base_currency_code, soc.order_currency_code, soc.base_total_order, 
        soc.base_total_product, soc.base_total_discount, soc.base_total_tax, soc.base_donations, soc.base_shipment, soc.base_shipment_tax, 
        soc.base_total_payments, soc.base_total_gift_cards, soc.base_total_thrive_cash, soc.total_order, soc.total_product, soc.total_discount, 
        soc.total_tax, soc.donations, soc.shipment, soc.shipment_tax, soc.total_payments, soc.total_gift_cards, soc.total_thrive_cash,
        soc.billing_city, soc.billing_state, soc.billing_postal_code, soc.billing_country, soc.shipping_city, soc.shipping_state, soc.shipping_postal_code, 
        soc.shipping_country, soc.message_state_id, soc.message_state, soc.insert_utc_datetime, soc.update_utc_datetime, soc.message_type, 
        soc.frozen_base_shipment, soc.frozen_base_shipment_tax, soc.wine_base_shipment, soc.wine_base_shipment_tax, soc.grocery_base_shipment, 
        soc.grocery_base_shipment_tax, soc.frozen_shipment, soc.grocery_shipment, soc.wine_shipment, soc.frozen_shipment_tax, 
        soc.grocery_shipment_tax, soc.wine_shipment_tax, soc.created_at, current_timestamp, 
        convert_timezone('UTC', 'America/Los_Angeles',created_at)::date, soc.ppe_fee, soc.ppe_fee_tax, 'open', current_timestamp 
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "16 - Copy stage data to staging.stg_order_capture_v1_processed":
        """
        insert into staging.stg_order_capture_v1_processed
        select soc.message_uuid, soc.order_id, soc.increment_id, soc.order_type, soc.original_order_id, soc.customer_id, soc.is_autoship, soc.sechedule_id,
        soc.updated_at, soc.sent_at, soc.base_currency_code, soc.order_currency_code, soc.base_total_order, soc.base_total_product, soc.base_total_discount,
        soc.base_total_tax, soc.base_donations, soc.base_shipment, soc.base_shipment_tax, soc.base_total_payments, soc.base_total_gift_cards,
        soc.base_total_thrive_cash, soc.total_order, soc.total_product, soc.total_discount, soc.total_tax, soc.donations, soc.shipment, soc.shipment_tax,
        soc.total_payments, soc.total_gift_cards, soc.total_thrive_cash, soc.billing_city, soc.billing_state, soc.billing_postal_code, soc.billing_country,
        soc.shipping_city, soc.shipping_state, soc.shipping_postal_code, soc.shipping_country, soc.message_state_id, soc.message_state,
        soc.insert_utc_datetime, soc.update_utc_datetime, soc.message_type, soc.frozen_base_shipment, soc.frozen_base_shipment_tax,
        soc.wine_base_shipment, soc.wine_base_shipment_tax, soc.grocery_base_shipment, soc.grocery_base_shipment_tax, soc.frozen_shipment,
        soc.grocery_shipment, soc.wine_shipment, soc.frozen_shipment_tax, soc.grocery_shipment_tax, soc.wine_shipment_tax,
        soc.created_at, current_timestamp, soc.ppe_fee, soc.ppe_fee_tax
        from staging.stg_order_capture_v1 soc
        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "17 - Copy stage data to ods.order_detail":
        """
        insert into ods.order_detail(order_id, item_id, parent_item_id, product_id, product_type, is_virtual, sku, is_autoship_subscribed, subscription_qty, 
        qty_ordered, base_original_price, base_price, tax_percent, base_tax_amt, discount_percent, base_discount_amt, base_row_total, base_row_total_incl_tax, 
        original_price, price, tax_amt, discount_amt, row_total, row_total_incl_tax, fulfillment_qty, short_qty, return_qty, insert_utc_datetime, update_utc_datetime, 
        message_type, message_uuid, is_gwp, ods_updated_at, sale_date)
        select soci.*,current_timestamp, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_items_v1 soci
        inner join staging.stg_order_capture_v1 soc on soci.order_id = soc.order_id and soci.message_uuid = soc.message_uuid
        inner join staging.order_work_list ol on soci.order_id = ol.order_id and soci.message_uuid = ol.message_uuid
        """,
    "18 - Copy stage data to staging.stg_order_capture_v1_processed":
        """
        insert into staging.stg_order_capture_items_v1_processed
        select soci.*,current_timestamp
        from staging.stg_order_capture_items_v1 soci
        inner join staging.order_work_list ol on soci.order_id = ol.order_id and soci.message_uuid = ol.message_uuid
        """,
    "19 - Copy stage data to ods.order_detail":
        """
        insert into ods.order_detail_discounts
        select socd.ORDER_ID,socd.ITEM_ID,socd.DISCOUNT_ID,socd.DISCOUNT_TYPE,socd.RULE_ID,socd.RULE_PCT_OFF,socd.RULE_AMT_OFF,socd.PCT_OFF_APPLIED,socd.START_PRICE,socd.BASE_DISCOUNT_AMT,socd.DISCOUNT_AMT,socd.UNITS_APPLIED,socd.BASE_AMT_OFF_APPLIED,socd.AMT_OFF_APPLIED,socd.BASE_TAX_DISCOUNT,socd.TAX_DISCOUNT,socd.BASE_DUTY_DISCOUNT,socd.DUTY_DISCOUNT,socd.INSERT_UTC_DATETIME,socd.UPDATE_UTC_DATETIME,socd.MESSAGE_UUID,socd.RULE_NAME,socd.RULE_GROUP
        ,current_timestamp,socd.SPONSORED_BY,socd.SPONSORED_PERCENTAGE
        from staging.stg_order_capture_detail_discounts_v1 socd
        inner join staging.stg_order_capture_v1 soc on socd.order_id = soc.order_id --and socd.message_uuid = soc.message_uuid
        inner join staging.order_work_list ol on socd.order_id = ol.order_id --and socd.message_uuid = ol.message_uuid
        """,
    "20 - Copy stage data to  staging.stg_order_capture_detail_discounts_v1_processed":
        """
        insert into staging.stg_order_capture_detail_discounts_v1_processed
        select socd.ORDER_ID,socd.ITEM_ID,socd.DISCOUNT_ID,socd.DISCOUNT_TYPE,socd.RULE_ID,socd.RULE_PCT_OFF,socd.RULE_AMT_OFF,socd.PCT_OFF_APPLIED,socd.START_PRICE,socd.BASE_DISCOUNT_AMT,socd.DISCOUNT_AMT,socd.UNITS_APPLIED,socd.BASE_AMT_OFF_APPLIED,socd.AMT_OFF_APPLIED,socd.BASE_TAX_DISCOUNT,socd.TAX_DISCOUNT,socd.BASE_DUTY_DISCOUNT,socd.DUTY_DISCOUNT,socd.INSERT_UTC_DATETIME,socd.UPDATE_UTC_DATETIME,socd.MESSAGE_UUID,socd.RULE_NAME,socd.RULE_GROUP
        ,current_timestamp,socd.SPONSORED_BY,socd.SPONSORED_PERCENTAGE
        from staging.stg_order_capture_detail_discounts_v1 socd
        inner join staging.order_work_list ol on socd.order_id = ol.order_id --and socd.message_uuid = ol.message_uuid
        """,
    "21 - Copy stage data to ods.order_payments with Braintree ":
        """
        insert into ods.order_payments
        select sop.*, current_timestamp, 'cc', null, null, convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date, null
        from staging.stg_order_capture_payments_v1 sop
        inner join staging.stg_order_capture_v1 soc on sop.order_id = soc.order_id --and sop.message_uuid = soc.message_uuid
        inner join staging.order_work_list ol on sop.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
        """,
    "22 - Copy stage data to  staging.stg_order_capture_payments_v1_processed":
        """
        insert into staging.stg_order_capture_payments_v1_processed
        select sop.*,current_timestamp
        from staging.stg_order_capture_payments_v1 sop
        inner join staging.order_work_list ol on sop.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
        """,
    "23 - Copy stage data to ods.order_payments with gift cards":
        """
        insert into ods.order_payments(order_id, gift_card_id, pay_amt, insert_utc_datetime, update_utc_datetime, message_uuid, ods_processed_at, payment_type, sale_date)
        select sogc.*, current_timestamp, 'gc', convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_gift_cards_v1 sogc
        inner join staging.stg_order_capture_v1 soc on sogc.order_id = soc.order_id --and sop.message_uuid = soc.message_uuid
        inner join staging.order_work_list ol on sogc.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
        """,
    "24 - Copy stage data to  staging.stg_order_capture_gift_cards_v1_processed":
        """
        insert into staging.stg_order_capture_gift_cards_v1_processed
        select sogc.*, current_timestamp
        from staging.stg_order_capture_gift_cards_v1 sogc
        inner join staging.order_work_list ol on sogc.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
        """,
    "25 - Copy stage data to ods.order_payments with thrive cash":
        """
        insert into ods.order_payments(order_id, tc_id, pay_amt, insert_utc_datetime, update_utc_datetime, message_uuid, ods_processed_at, payment_type, sale_date)
        select sotc.*, current_timestamp, 'tc', convert_timezone('UTC', 'America/Los_Angeles',soc.created_at)::date
        from staging.stg_order_capture_thrive_cash_v1 sotc
        inner join staging.stg_order_capture_v1 soc on sotc.order_id = soc.order_id --and sotc.message_uuid = soc.message_uuid
        inner join staging.order_work_list ol on sotc.order_id = ol.order_id --and sotc.message_uuid = ol.message_uuid
        """,
    "26 - Copy stage data to  staging.stg_order_capture_gift_cards_v1_processed":
        """
        insert into staging.stg_order_capture_thrive_cash_v1_processed
        select sotc.*, current_timestamp
        from staging.stg_order_capture_thrive_cash_v1 sotc
        inner join staging.order_work_list ol on sotc.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
        """,
    "27 - Delete processed records from staging.stg_order_capture_detail_discounts_v1":
        """
        delete from staging.stg_order_capture_detail_discounts_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_detail_discounts_v1 sod
                        inner join staging.order_work_list ol on sod.order_id = ol.order_id --and sod.message_uuid = ol.message_uuid
                        where sod.order_id = del.order_id)
        """,
    "28 - Delete processed records from staging.stg_order_capture_detail_discounts_v1":
        """
        delete from staging.stg_order_capture_thrive_cash_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_thrive_cash_v1 sotc
                        inner join staging.order_work_list ol on sotc.order_id = ol.order_id --and stc.message_uuid = ol.message_uuid
                        where sotc.order_id = del.order_id)
        """,
    "29 - Delete processed records from staging.stg_order_capture_detail_discounts_v1":
        """
        delete from staging.stg_order_capture_gift_cards_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_gift_cards_v1 sogc
                        inner join staging.order_work_list ol on sogc.order_id = ol.order_id --and sogc.message_uuid = ol.message_uuid
                        where sogc.order_id = del.order_id)
        """,
    "30 - Delete processed records from staging.stg_order_capture_detail_discounts_v1":
        """
        delete from staging.stg_order_capture_payments_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_payments_v1 sop
                        inner join staging.order_work_list ol on sop.order_id = ol.order_id --and sop.message_uuid = ol.message_uuid
                        where sop.order_id = del.order_id)
        """,
    "31 - Delete processed records from staging.stg_order_capture_detail_discounts_v1":
        """
        delete from staging.stg_order_capture_items_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_items_v1 soci
                        inner join staging.order_work_list ol on soci.order_id = ol.order_id and soci.message_uuid = ol.message_uuid
                        where soci.order_id = del.order_id)
        """,
    "32 - Delete processed records from staging.stg_order_capture":
        """
        delete from staging.stg_order_capture_v1 del
        where exists (	select 1
                        from staging.stg_order_capture_v1 soc
                        inner join staging.order_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """
}

spOrderAssignment = {
    "1- Truncate assignment work table ":
        "truncate staging.assignment_work"
    ,
    "2 - Create working list of assignments":
        """
        insert into staging.assignment_work 
        select soa.item_id, soa.fc_id, soa.qty_assigned, ci.net_unit_cost * od.qty_ordered
        from staging.stg_order_assignment_v1  soa
        inner join ods.order_detail od on soa.item_id = od.item_id
        left outer join ods.ns_fc_xref nfx on soa.fc_id = nfx.ns_fc_id
        left outer join ods.curr_items ci on od.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        """,
    "3 - Update order_detail with assignement info":
        """
        update ods.order_detail
        set assignment_fc  = upd.fc_id, assignment_qty = upd.qty_assigned, ext_cost_amt = upd.ext_cost_amt, ods_updated_at = current_timestamp 
        from (
                select *
                from staging.assignment_work ) upd
        where order_detail.item_id = upd.item_id
        """,
    "4 - Insert rows into processed table":
        """
        insert into staging.stg_order_assignment_v1_processed
        select soa.*, current_timestamp
        from staging.stg_order_assignment_v1 soa
        inner join staging.assignment_work  aw on soa.item_id = aw.item_id
        """,
    "5 - Delete processed rows from staging table":
        """
        delete from staging.stg_order_assignment_v1 del
        where exists (	select 1
                        from staging.stg_order_assignment_v1 soa
                        inner join staging.assignment_work  aw on soa.item_id = aw.item_id
                        where soa.item_id = del.item_id)
        """
}

spMemberCapture = {
    "1 - Truncate work file":
        "truncate staging.member_work_list"
    ,
    "2 -Create temp list of membership orders":
        """
        insert into staging.member_work_list
        select order_id, message_uuid
        from staging.stg_membership_capture_v1
        """,
    "3 - Create membership Braintree transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, 
        tran_amt, cc_auth, cc_last4, cc_type, cc_pay_amt,  created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 150, 10, 'Payment Capture' as tran_sub_type, smc.updated_at::timestamp, 
        case when start_status = 'member' then 'Renewal' when start_status = 'canceled' then 'Re-Activation' else 'New Member' end, smc.order_id, 
        smc.increment_id, smc.customer_id, 1, 1, smp.base_pay_amt,smp.cc_tran_id, smp.cc_last_four, 
        case when coalesce(smp.cc_type, '') <> '' then smp.cc_type else smp.payment_gateway end as cc_type,
        coalesce(smp.base_pay_amt,0), current_timestamp, smc.message_uuid, current_timestamp, convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date,
        convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date
        from staging.stg_membership_capture_v1 smc
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id and smc.message_uuid = mwl.message_uuid 
        left outer join staging.stg_membership_capture_payments_v1 smp on smc.order_id = smp.order_id
        """,
    "4 - Create membership Gift Card payment trasnactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt, 
         created_at, gc_sub_type, gc_type, updated_at, sale_date, tran_gl_date)
        select 150, 21, 'Gift Card Capture' as tran_sub_type, smc.updated_at::timestamp, 
        case when start_status = 'member' then 'Renewal' when start_status = 'canceled' then 'Re-Activation' else 'New Member' end, smc.order_id, smc.increment_id, 
        smc.customer_id, 1, 1, smgc.gift_card_amt, current_timestamp, 'Purchased', 2, current_timestamp, convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date,
        convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date
        from staging.stg_membership_capture_v1 smc
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id and smc.message_uuid = mwl.message_uuid 
        left outer join staging.stg_membership_capture_gift_cards_v1 smgc on smc.order_id = smgc.order_id
        where smgc.gift_card_id is not null
        """,
    "5 - Create membership payment tax transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt,
        created_at, message_uuid, updated_at, sale_date, tran_gl_date)
        select 155, 68, 'Tax Capture' as tran_sub_type,  smc.updated_at::timestamp, 
        case when start_status = 'member' then 'Renewal' when start_status = 'canceled' then 'Re-Activation' else 'New Member' end, smc.order_id, smc.increment_id, smc.customer_id, 1, 1, 
        coalesce(smc.base_total_tax,0)  as tax_amt, current_timestamp, smc.message_uuid, current_timestamp, convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date,
        convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date 
        from staging.stg_membership_capture_v1 smc
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id and smc.message_uuid = mwl.message_uuid 
        where coalesce(smc.base_total_tax,0) > 0
        """,
    "6 - Copy membership header data to ods table":
        """
        insert into ods.membership_header                         
        select smc.END_DATE, smc.END_STATUS, smc.DONATIONS, smc.BASE_TOTAL_GIFT_CARDS, 
        smc.TOTAL_PAYMENTS,
         smc.BASE_DONATIONS, smc.BASE_TOTAL_THRIVE_CASH, 
        smc.TOTAL_ORDER, smc.BASE_TOTAL_MEMBERSHIP, smc.UPDATED_AT, smc.INCREMENT_ID, 
        smc.BASE_CURRENCY_CODE, smc.ORIGINAL_ORDER_ID, smc.MEMBERSHIP_TYPE, smc.START_DATE, 
        smc.MESSAGE_TYPE, smc.MESSAGE_UUID, smc.TOTAL_TAX, smc.START_STATUS, smc.SENT_AT, 
        smc.BASE_TOTAL_ORDER, smc.BASE_TOTAL_TAX, smc.TOTAL_THRIVE_CASH, smc.TOTAL_GIFT_CARDS, 
        smc.CUSTOMER_ID, smc.ORDER_ID, smc.ORDER_CURRENCY_CODE, smc.BASE_TOTAL_PAYMENTS, 
        smc.TOTAL_MEMBERSHIP, smc.INSERT_UTC_DATETIME, smc.UPDATE_UTC_DATETIME, current_timestamp, 
        convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date, NULL, 
        NULL, NULL, smc.BILLING_STATE  
        from staging.stg_membership_capture_v1 smc
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id and smc.message_uuid = mwl.message_uuid    
        """,
    "7 - Copy memebrship header data to processed table":
        """
        insert into staging.stg_membership_capture_v1_processed
        select smc.END_DATE, smc.END_STATUS, smc.DONATIONS, smc.BASE_TOTAL_GIFT_CARDS, 
        smc.TOTAL_PAYMENTS, smc.BASE_DONATIONS, smc.BASE_TOTAL_THRIVE_CASH, 
        smc.TOTAL_ORDER, smc.BASE_TOTAL_MEMBERSHIP, smc.UPDATED_AT, smc.INCREMENT_ID, 
        smc.BASE_CURRENCY_CODE, smc.ORIGINAL_ORDER_ID, smc.MEMBERSHIP_TYPE, 
        smc.START_DATE, smc.MESSAGE_TYPE, smc.MESSAGE_UUID, smc.TOTAL_TAX, smc.START_STATUS, 
        smc.SENT_AT, smc.BASE_TOTAL_ORDER, smc.BASE_TOTAL_TAX, smc.TOTAL_THRIVE_CASH, 
        smc.TOTAL_GIFT_CARDS, smc.CUSTOMER_ID, smc.ORDER_ID, smc.ORDER_CURRENCY_CODE, 
        smc.BASE_TOTAL_PAYMENTS, smc.TOTAL_MEMBERSHIP, smc.INSERT_UTC_DATETIME, 
        smc.UPDATE_UTC_DATETIME, current_timestamp, smc.BILLING_STATE
        from staging.stg_membership_capture_v1 smc
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id and smc.message_uuid = mwl.message_uuid 
        """,
    "8 - Copy membership payment data to ods table":
        """
        insert into ods.membership_payments
        select smp.*, current_timestamp, 'cc', null, convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date, NULL, NULL
        from staging.stg_membership_capture_payments_v1 smp
        inner join staging.stg_membership_capture_v1 smc on smp.order_id = smc.order_id --and sop.message_uuid = soc.message_uuid
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id --and smc.message_uuid = mwl.message_uuid
        """,
    "9 - copy membership payment data to payment table":
        """
        insert into staging.stg_membership_capture_payments_v1_processed
        select smp.*,current_timestamp
        from staging.stg_membership_capture_payments_v1 smp
        inner join staging.member_work_list mwl on smp.order_id = mwl.order_id --and smc.message_uuid = mwl.message_uuid
        """,
    "10 - Copy membership gift card payments to ods table":
        """
        insert into ods.membership_payments(order_id, gift_card_id, pay_amt, insert_utc_datetime, update_utc_datetime, message_uuid, ods_processed_at, payment_type, sale_date)
        select smgc.*, current_timestamp, 'gc', convert_timezone('UTC','America/Los_Angeles',smc.updated_at::timestamp)::date
        from staging.stg_membership_capture_gift_cards_v1 smgc
        inner join staging.stg_membership_capture_v1 smc on smgc.order_id = smc.order_id --and sop.message_uuid = soc.message_uuid
        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id --and smc.message_uuid = mwl.message_uuid
        """,
    "11 - Copy membership gift card payments to processed table":
        """
        insert into staging.stg_membership_capture_gift_cards_v1_processed
        select smgc.*, current_timestamp
        from staging.stg_membership_capture_gift_cards_v1 smgc
        inner join staging.member_work_list mwl on smgc.order_id = mwl.order_id --and smc.message_uuid = mwl.message_uuid
        """,
    "12 - Delete processed gift cards from stage":
        """
        delete from staging.stg_membership_capture_gift_cards_v1 del
        where exists (	select 1
                        from staging.stg_membership_capture_gift_cards_v1 smgc
                        join staging.member_work_list mwl on smgc.order_id = mwl.order_id --and sogc.message_uuid = ol.message_uuid
                        where smgc.order_id = del.order_id)
        """,
    "13 - Delete processed payments from stage":
        """
        delete from staging.stg_membership_capture_payments_v1 del
        where exists (	select 1
                        from staging.stg_membership_capture_payments_v1 smp
                        inner join staging.member_work_list mwl on smp.order_id = mwl.order_id --and smc.message_uuid = mwl.message_uuid
                        where smp.order_id = del.order_id)
        """,
    "14 - Delete processed memberships from stage":
        """
        delete from staging.stg_membership_capture_v1 del
        where exists (	select 1
                        from staging.stg_membership_capture_v1 smc
                        inner join staging.member_work_list mwl on smc.order_id = mwl.order_id
                        where smc.order_id = del.order_id)
        """
}

spOrderCancel = {
    "1 --Truncate work file":
        "truncate staging.cancel_work_list"
    ,
    "2--Create temp list of orders":
        """
        insert into staging.cancel_work_list
        select soc.order_id, soc.message_uuid, soc.updated_at, oh.sale_date, coalesce(soc.ms_initiated, false), coalesce(soc.is_fraud, false),
        parse_json(refunds):payment_detail:voided::boolean is_voided
        from staging.stg_order_cancel_v1 soc
        inner join ods.order_header oh on soc.order_id = oh.order_id
        """,
    "3 - Reverse all posted transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt, cc_auth, cc_last4, cc_type,
        cc_pay_amt,  created_at, message_uuid, gc_sub_type, gc_type, sale_date, tran_gl_date, cancel_type)
        select trn.tran_type + 10, tst.tran_sub_type_id, trn.tran_sub_type||' Cancel', cwl.updated_at, order_type, cwl.order_id, increment_id, customer_id, company_id, location_id, tran_amt, cc_auth, cc_last4, cc_type,
        cc_pay_amt,  created_at, cwl.message_uuid, gc_sub_type, gc_type, cwl.sale_date,
        convert_timezone('UTC', 'America/Los_Angeles',cwl.updated_at )::date, 'void'
        from ods.transactions trn
        left outer join  ods.transaction_sub_type tst on trn.tran_type + 10 = tst.tran_type and trn.tran_sub_type||' Cancel' = tst.tran_sub_type
        inner join staging.cancel_work_list cwl on trn.order_id::int = cwl.order_id
        where trn.tran_type in (100,105,400) and cwl.is_voided = true
        """,
    "4 - Update order_header with cancel status":
        """
        update ods.order_header
        set status = case when upd.is_voided = True then 'void' else 'cancel' end,  
        status_date = upd.updated_at, updated_at = upd.updated_at
        from (select * from staging.cancel_work_list ) upd
        where order_header.order_id = upd.order_id
        """,
    "5 - Insert stg_order_cancel_gift_cards_v1 to processed":
        """
        insert into staging.stg_order_cancel_gift_cards_v1_processed
        select soc.*
        from staging.stg_order_cancel_gift_cards_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "6 - delete stg_order_cancel_gift_cards_v1 processed":
        """
        delete from staging.stg_order_cancel_gift_cards_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_gift_cards_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "7 - insert stg_order_cancel_refunds_v1 to processed":
        """
        insert into staging.stg_order_cancel_refunds_v1_processed
        select soc.*
        from staging.stg_order_cancel_refunds_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "8 - delete stg_order_cancel_refunds_v1 processed":
        """
        delete from staging.stg_order_cancel_refunds_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_refunds_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "9 - insert stg_order_cancel_thrive_cash_earned_v1 to processed":
        """
        insert into staging.stg_order_cancel_thrive_cash_earned_v1_processed
        select soc.*
        from staging.stg_order_cancel_thrive_cash_earned_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "10 - delete stg_order_cancel_thrive_cash_earned_v1 processed":
        """
        delete from staging.stg_order_cancel_thrive_cash_earned_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_thrive_cash_earned_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "11 - Insert stg_order_cancel_thrive_cash_v1":
        """
        insert into staging.stg_order_cancel_thrive_cash_v1_processed
        select soc.*, null
        from staging.stg_order_cancel_thrive_cash_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "12 - Delete stg_order_cancel_thrive_cash_v1 processed":
        """
        delete from staging.stg_order_cancel_thrive_cash_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_thrive_cash_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "13 - Insert stg_order_cancel_v1 processed":
        """
        insert into staging.stg_order_cancel_v1_processed
        select soc.*
        from staging.stg_order_cancel_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "14 - delete stg_order_cancel_v1 processed":
        """
        delete from staging.stg_order_cancel_v1 
        where order_id in (	select order_id
                        from staging.cancel_work_list)
        """
}

spMemberCancel = {
    "1 --Truncate work file":
        "truncate staging.cancel_work_list"
    ,
    "2--Create temp list of orders":
        """
        insert into staging.cancel_work_list
        select soc.order_id, soc.message_uuid, soc.updated_at, mh.sale_date, coalesce(soc.ms_initiated, false), coalesce(soc.is_fraud, false),
        parse_json(refunds):payment_detail:voided::boolean is_voided
        from staging.stg_order_cancel_v1 soc
        inner join ods.membership_header mh on soc.order_id = mh.order_id
        """,
    "3 - Reverse all posted transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, company_id, location_id, tran_amt, cc_auth, cc_last4, cc_type,
        cc_pay_amt,  created_at, message_uuid, gc_sub_type, gc_type, sale_date, tran_gl_date, cancel_type)
        select trn.tran_type + 10, tst.tran_sub_type_id, trn.tran_sub_type||' Cancel', cwl.updated_at, order_type, cwl.order_id, increment_id, customer_id, company_id, location_id, tran_amt, coalesce(scmr.gateway_tran_id,trn.cc_auth), cc_last4, trn.cc_type,
        trn.cc_pay_amt,  created_at, cwl.message_uuid, gc_sub_type, gc_type, cwl.sale_date,
        convert_timezone('UTC', 'America/Los_Angeles', cwl.updated_at)::date ,'void'
        from ods.transactions trn
        inner join staging.cancel_work_list cwl on trn.order_id::int = cwl.order_id
        left outer join  ods.transaction_sub_type tst on trn.tran_type + 10 = tst.tran_type and trn.tran_sub_type||' Cancel' = tst.tran_sub_type
        left outer join staging.stg_credit_memo_refunds_v1 scmr  on cwl.order_id = scmr.order_id and trn.tran_amt = scmr.base_pay_amt
        where trn.tran_type in (150,155) and cwl.is_voided = true
        """,
    "4 - Update order_header with cancel status":
        """
        update ods.membership_header 
        set status = 'cancel',  status_date = upd.updated_at, updated_at = upd.updated_at
        from (select * from staging.cancel_work_list where is_voided = true) upd
        where membership_header.order_id = upd.order_id
        """,
    "5 - Insert stg_order_cancel_gift_cards_v1 to processed":
        """
        insert into staging.stg_order_cancel_gift_cards_v1_processed
        select soc.*
        from staging.stg_order_cancel_gift_cards_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "6 - delete stg_order_cancel_gift_cards_v1 processed":
        """
        delete from staging.stg_order_cancel_gift_cards_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_gift_cards_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "7 - insert stg_order_cancel_refunds_v1 to processed":
        """
        insert into staging.stg_order_cancel_refunds_v1_processed
        select soc.*
        from staging.stg_order_cancel_refunds_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "8 - delete stg_order_cancel_refunds_v1 processed":
        """
        delete from staging.stg_order_cancel_refunds_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_refunds_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "9 - insert stg_order_cancel_thrive_cash_earned_v1 to processed":
        """
        insert into staging.stg_order_cancel_thrive_cash_earned_v1_processed
        select soc.*
        from staging.stg_order_cancel_thrive_cash_earned_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "10 - delete stg_order_cancel_thrive_cash_earned_v1 processed":
        """
        delete from staging.stg_order_cancel_thrive_cash_earned_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_thrive_cash_earned_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "11 - Insert stg_order_cancel_thrive_cash_v1":
        """
        insert into staging.stg_order_cancel_thrive_cash_v1_processed
        select soc.*, null
        from staging.stg_order_cancel_thrive_cash_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "12 - Delete stg_order_cancel_thrive_cash_v1 processed":
        """
        delete from staging.stg_order_cancel_thrive_cash_v1 del
        where exists (	select 1
                        from staging.stg_order_cancel_thrive_cash_v1 soc
                        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
                        where soc.order_id = del.order_id)
        """,
    "13 - Insert stg_order_cancel_v1 processed":
        """
        insert into staging.stg_order_cancel_v1_processed
        select soc.*
        from staging.stg_order_cancel_v1 soc
        inner join staging.cancel_work_list ol on soc.order_id = ol.order_id and soc.message_uuid = ol.message_uuid
        """,
    "14 - delete stg_order_cancel_v1 processed":
        """
        delete from staging.stg_order_cancel_v1 
        where order_id in (	select order_id
                        from staging.cancel_work_list)
        """
}
spOrderFulfillment = {
    "1 - Truncate fulfill_work":
        "truncate staging.fulfill_work"
    ,
    "2 - update fulfill_work with orders to fulfill":
        """
         insert into staging.fulfill_work 
         select distinct sof.order_id, max(scm.credit_id) as credit_id, max(sof.updated_at) as tran_date, max(oh.sale_date) 
        from staging.stg_order_fulfillment_v1 sof
        left outer join staging.stg_order_fulfillment_detail_v1 sofd on sof.order_id = sofd.order_id and sof.message_uuid = sofd.message_uuid
        left outer join ods.order_header oh on sof.order_id = oh.order_id
        left outer join ods.order_detail od on sofd.item_id = od.item_id
        left outer join staging.stg_credit_memo_v1 scm on parse_json(credit_memo_id)[0]::int = scm.credit_id 
        where od.sale_date >= '9/26/21'
        group by sof.order_id
        having sum(case when od.item_id is null then 1 else 0 end) = 0 and (sum(case when (sof.is_last_shipment or sof.updated_at::date < current_date - 7) then 1 else 0 end) > 0 or max(oh.gaap_sale_date) is not null)
        """
    ,
    "3 - Copy already updated item_id's to error table":
        """
        insert into staging.stg_order_fulfillment_detail_v1_error  
        select sofd.*, current_timestamp as error_date
        from staging.stg_order_fulfillment_detail_v1 sofd
        inner join ods.order_detail od on sofd.item_id = od.item_id
        where coalesce(od.fulfillment_qty,0) + coalesce(od.short_qty,0) <> 0 
        """,
    "4 - Delete already update item_id's":
        """
        delete from staging.stg_order_fulfillment_detail_v1
        where item_id in (
                            select sofd.item_id
                            from staging.stg_order_fulfillment_detail_v1 sofd
                            inner join ods.order_detail od on sofd.item_id = od.item_id
                            where coalesce(od.fulfillment_qty,0) + coalesce(od.short_qty,0) <> 0)
        """,
    "5 -truncate stage_order_fulfillment":
        "truncate staging.stage_order_fulfillment"
    ,
    "6 - update stage_order_fulfillment with fuilfillment info":
        """
        insert into staging.stage_order_fulfillment 
        select sof.order_id, oh.increment_id, oh.customer_id, sof.message_uuid,sof.updated_at::timestamp as fulfilled_at, sofd.item_id, sofd.parent_item_id, sofd.product_type, 
        nfx.ns_fc_id as fc, parse_json(sofd.fc)[0]::int as magento_fc,sofd.sku, sofd.units_shipped, od.qty_ordered, sofd.units_shorted, od.base_price, 
            case when coalesce (od.is_gwp,  false) = false then od.base_discount_amt else 0 end as base_discount_amt,
        od.base_tax_amt, od.base_row_total,sofd.shipment_id, sofd.tracking_id, sof.is_last_shipment, 
        case when scmi.qty_refunded > 0 then parse_json(sof.credit_memo_id)[0]::int else null end as cm_id, 
        scmi.qty_refunded, scmi.base_price as credit_price, scmi.base_tax_amt as credit_tax_amt, scmi.base_discount_amt as credit_discount_amt, 
        scmi.base_row_total_incl_tax - scmi.base_discount_amt as credit_total_amt,
        null,null,null,null,odd.tpa_amt, fw.tran_date, oh.sale_date
        from staging.stg_order_fulfillment_v1 sof
        inner join staging.fulfill_work fw on sof.order_id = fw.order_id
        inner join staging.stg_order_fulfillment_detail_v1 sofd on sof.order_id = sofd.order_id and sof.message_uuid = sofd.message_uuid
        inner join ods.order_header oh on sof.order_id = oh.order_id
        inner join ods.order_detail od on sofd.item_id = od.item_id
        left outer join (
                            select odd.item_id, sum(odd.discount_amt) as tpa_amt
                            from staging.stg_order_fulfillment_v1 sof
                            inner join ods.order_detail_discounts odd on sof.order_id = odd.order_id
                            where odd.discount_type = 'tpa' and odd.discount_amt > 0
                            group by odd.item_id) odd on sofd.item_id = odd.item_id
        left outer join staging.stg_credit_memo_items_v1 scmi on fw.credit_id = scmi.credit_id and sofd.item_id = scmi.order_item_id
        left outer join ods.ns_fc_xref nfx on parse_json(sofd.fc)[0]::int = nfx.fc_id
        order by sofd.item_id
        """
    ,
    "7 - update stage_order_fulfillment with current retail from item file":
        """
        update staging.stage_order_fulfillment
        set retail_price = upd.retail_price
        from (
                select sof.item_id, 
                case when coalesce(ci.retail_price,cid.retail_price,0) > 0 then  coalesce(ci.retail_price,cid.retail_price,0) else coalesce(ci.net_unit_cost, cid.net_unit_cost,0) * 1.3 end as retail_price
                from staging.stage_order_fulfillment  sof
                left outer join ods.ns_fc_xref nfx on  sof.magento_fc = nfx.fc_id
                left outer  join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id, 2) = ci.fc_id
                left outer  join ods.curr_items cid on sof.sku = cid.item_name and  2 = cid.fc_id) upd 
        where stage_order_fulfillment.item_id = upd.item_id
        """
    ,
    "8 - create temp table to allocate bundles to the componets":
        """
        create temp table temp_allocate_bundle as
        select sof.item_id, sof.parent_item_id, sof.fc, sof.order_id, sof.sku,
        round(dtl.bundle_total_amt * (CASE WHEN dtl.dtl_amt <> 0 THEN sof.units_shipped * sof.retail_price/ dtl.dtl_amt ELSE 0 end),2)::numeric(12,2) as alloc_revenue_amt,
        round(dtl.bundle_tax_amt *  (CASE WHEN dtl.dtl_amt <> 0 THEN sof.units_shipped * sof.retail_price/ dtl.dtl_amt ELSE 0 end),2)::numeric(12,2) as alloc_tax_amt,
        round(dtl.bundle_discount_amt *  (CASE WHEN dtl.dtl_amt <> 0 THEN sof.units_shipped * sof.retail_price/ dtl.dtl_amt ELSE 0 end),2)::numeric(12,2) as alloc_discount_amt,
        dtl.bundle_total_amt ,dtl.bundle_discount_amt, dtl.bundle_tax_amt
        from (
                select sof.order_id, sof.parent_item_id,  psof.units_shipped * (psof.base_price + psof.base_discount_amt) as bundle_total_amt,
                psof.base_discount_amt as bundle_discount_amt, psof.base_tax_amt as bundle_tax_amt, sum(sof.units_shipped * sof.retail_price) as dtl_amt
                from staging.stage_order_fulfillment sof
                inner join staging.stage_order_fulfillment psof on sof.parent_item_id = psof.item_id
                where sof.parent_item_id is not null and psof.base_price > 0 and psof.units_shipped > 0
                group by sof.order_id, sof.parent_item_id, psof.units_shipped, psof.base_price, psof.base_discount_amt, psof.base_tax_amt) dtl
        inner join staging.stage_order_fulfillment sof on sof.parent_item_id = dtl.parent_item_id
        """
    ,
    "9 - create temp table to balance out penny diffrences in allocation":
        """
        create temp table oob as
         select tab.order_id, bun.item_id, bun.bundle_total_amt, sum(tab.alloc_revenue_amt) as dtl_amt, ((coalesce(bun.bundle_total_amt,0) - sum(coalesce(tab.alloc_revenue_amt,0))) *100)::int as adj_revenue_amt,
               bun.bundle_discount_amt, sum(tab.alloc_discount_amt) as discount_amt, ((coalesce(bun.bundle_discount_amt,0) - sum(coalesce(tab.alloc_discount_amt,0))) *100)::int as adj_discount_amt,
               sum(tab.alloc_tax_amt) as tax_amt, ((coalesce(bun.bundle_tax_amt,0) - sum(coalesce(tab.alloc_tax_amt,0))) *100)::int as adj_tax_amt
               from (
                   select psof.item_id, psof.units_shipped * (psof.base_price + psof.base_discount_amt) as bundle_total_amt,
                   psof.base_discount_amt as bundle_discount_amt, psof.base_tax_amt as bundle_tax_amt
                   from staging.stage_order_fulfillment psof
                   where product_type = 'bundle' and base_price > 0 and psof.units_shipped > 0) bun
           inner join temp_allocate_bundle tab on tab.parent_item_id = bun.item_id
           group by tab.order_id, bun.item_id, bun.bundle_total_amt, bun.bundle_discount_amt, bun.bundle_tax_amt
        """
    ,
    "10 - fix penny's in revenue":
        """
        update temp_allocate_bundle
        set alloc_revenue_amt = new_alloc_amt
        from (
                select det.item_id, det.alloc_revenue_amt + case when adj_revenue_amt > 0 then .01 else -.01 end as new_alloc_amt, det.alloc_revenue_amt
                from (
                        select tab.item_id, tab.parent_item_id, tab.alloc_revenue_amt,
                        row_number() over (partition by parent_item_id order by parent_item_id, alloc_revenue_amt desc) as rn
                        from  temp_allocate_bundle tab
                        where tab.alloc_revenue_amt > 0 and tab.parent_item_id is not null) det
                inner join oob on det.parent_item_id = oob.item_id and rn <= abs(oob.adj_revenue_amt)
                order by det.parent_item_id, rn) upd
        where temp_allocate_bundle.item_id = upd.item_id

        """
    ,
    "11 -fix penny's in tax":
        """
        update temp_allocate_bundle
        set alloc_tax_amt = new_alloc_amt
        from (
                select det.item_id, det.alloc_tax_amt + case when adj_tax_amt > 0 then .01 else -.01 end as new_alloc_amt, det.alloc_tax_amt
                from (
                        select tab.item_id, tab.parent_item_id, tab.alloc_tax_amt,
                        row_number() over (partition by parent_item_id order by parent_item_id, alloc_tax_amt desc) as rn
                        from  temp_allocate_bundle tab
                        where tab.alloc_tax_amt > 0 and tab.parent_item_id is not null) det
                inner join oob on det.parent_item_id = oob.item_id and rn <= abs(oob.adj_tax_amt)
                order by det.parent_item_id, rn) upd
        where temp_allocate_bundle.item_id = upd.item_id
        """
    ,
    "12 - fix penny's in discount:":
        """
        update temp_allocate_bundle
        set alloc_discount_amt = new_alloc_amt
        from (
                select det.item_id, det.alloc_discount_amt + case when adj_discount_amt > 0 then .01 else -.01 end as new_alloc_amt, det.alloc_discount_amt
                from (
                        select tab.item_id, tab.parent_item_id, tab.alloc_discount_amt,
                        row_number() over (partition by parent_item_id order by parent_item_id, alloc_discount_amt desc) as rn
                        from  temp_allocate_bundle tab
                        where tab.alloc_discount_amt > 0 and tab.parent_item_id is not null) det
                inner join oob on det.parent_item_id = oob.item_id and rn <= abs(oob.adj_discount_amt)
                order by det.parent_item_id, rn) upd
        where temp_allocate_bundle.item_id = upd.item_id
        """
    ,
    "13 - write allocation to stage_order_fulfillment":
        """
        update staging.stage_order_fulfillment
        set allocated_price = upd.alloc_revenue_amt,
            allocated_discount_amt = upd.alloc_discount_amt,
            allocated_tax_amt = upd.alloc_tax_amt
        from (select * from temp_allocate_bundle) upd
        where stage_order_fulfillment.item_id = upd.item_id
        """
    ,
    "14 - delete temp_allocate_bundle":
        "drop table temp_allocate_bundle"
    ,
    "15 - delete oob":
        "drop table oob"
    ,
    "16 - create temp_allocate_bundle_dsc to allocate detail discounts":
        """
        create temp table temp_allocate_bundle_dsc as
        select sof.item_id, sof.parent_item_id, sof.fc, sof.order_id, sof.sku,
        round(dsc.dsc_amt * (CASE WHEN dtl.dtl_amt <> 0 THEN sof.units_shipped * sof.retail_price/ dtl.dtl_amt else 0 end),2)::numeric(12,2) as alloc_discount_amt,
        dsc.dsc_amt, dsc.rule_id, dsc.rule_name, dsc.rule_group, dsc.discount_id, dsc.discount_type
        from (
                select sof.order_id, sof.parent_item_id, (psof.units_shipped * psof.base_price) + psof.base_discount_amt as bundle_total_amt,
                psof.base_discount_amt as bundle_discount_amt, psof.base_tax_amt as bundle_tax_amt, sum(sof.units_shipped * sof.retail_price) as dtl_amt
                from staging.stage_order_fulfillment sof
                inner join staging.stage_order_fulfillment psof on sof.parent_item_id = psof.item_id
                where sof.parent_item_id is not null and psof.base_price > 0 and psof.units_shipped > 0
                group by sof.order_id, sof.parent_item_id, psof.units_shipped, psof.base_price, psof.base_discount_amt, psof.base_tax_amt) dtl
        inner join staging.stage_order_fulfillment sof on sof.parent_item_id = dtl.parent_item_id
        inner join (
                        select odd.item_id, odd.discount_id, odd.base_amt_off_applied as dsc_amt, odd.rule_id, odd.rule_name, odd.rule_group, odd.discount_type
                        from staging.stage_order_fulfillment sof
                        inner join ods.order_detail_discounts odd on sof.item_id = odd.item_id
                        where product_type = 'bundle'
                        order by sof.item_id) dsc on sof.parent_item_id = dsc.item_id
        """
    ,
    "17 - create oob_dsc to allocate penny variances":
        """
        create temp table oob_dsc as
         select tab.order_id, bun.discount_id, bun.item_id, sum(tab.alloc_discount_amt) as dtl_amt, bun.dsc_amt, ((coalesce(bun.dsc_amt,0) - sum(coalesce(tab.alloc_discount_amt,0))) *100)::int as adj_discount_amt
               from (
                   select odd.item_id, odd.discount_id, odd.base_amt_off_applied as dsc_amt, odd.rule_id, odd.rule_name, odd.rule_group
                        from staging.stage_order_fulfillment sof
                        inner join ods.order_detail_discounts odd on sof.item_id = odd.item_id
                        where product_type = 'bundle'
                        order by sof.item_id) bun
           inner join temp_allocate_bundle_dsc tab on tab.parent_item_id = bun.item_id and bun.discount_id = tab.discount_id
           group by tab.order_id,  bun.item_id,bun.dsc_amt, bun.discount_id
           order by  bun.item_id,  bun.discount_id
        """
    ,
    "18 - fix penny's on discount_amt":
        """
        update temp_allocate_bundle_dsc
        set alloc_discount_amt = new_alloc_amt
        from (
                select det.item_id, det.discount_id, det.alloc_discount_amt + case when adj_discount_amt > 0 then .01 else -.01 end as new_alloc_amt, det.alloc_discount_amt
                from (
                        select tab.item_id, tab.parent_item_id, tab.alloc_discount_amt, tab.discount_id,
                        row_number() over (partition by parent_item_id, discount_id order by parent_item_id, discount_id, alloc_discount_amt desc) as rn
                        from  temp_allocate_bundle_dsc tab
                        where tab.alloc_discount_amt > 0 and tab.parent_item_id is not null) det
                inner join oob_dsc oob on det.parent_item_id = oob.item_id and det.discount_id = oob.discount_id and rn <= abs(oob.adj_discount_amt)
                order by det.parent_item_id, rn) upd
        where temp_allocate_bundle_dsc.item_id = upd.item_id and temp_allocate_bundle_dsc.discount_id = upd.discount_id
        """
    ,
    "19 - truncate stage_bundle_alloc_discounts":
        "truncate staging.stage_bundle_alloc_discounts"
    ,
    "20 - Update stage_bundle_alloc_discounts wit allocated discounts":
        """
        insert into staging.stage_bundle_alloc_discounts
        select order_id, item_id, discount_id, parent_item_id, alloc_discount_amt, rule_id, rule_name, rule_group, discount_type
        from temp_allocate_bundle_dsc
        order by item_id, discount_id
        """
    ,

    "22 - drop table temp_allocate_bundle_dsc":
        "drop table temp_allocate_bundle_dsc"
    ,
    "23 - drop table oob_dsc":
        "drop table oob_dsc"
    ,
    "23 - Update product fulfillments units and full price":
        """
         insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name,tran_qty, tran_amt, created_at, sale_date, item_id, tran_cost_amt, tran_gl_date, ship_date)
        select 300 as tran_type, 16, 'Product'  as tran_sub_type, sof.tran_date, 'order' as order_type, sof.order_id, sof.increment_id,
        sof.item_id as order_line_id, sof.customer_id,1, sof.fc, sof.magento_fc, sof.sku, sof.parent_item_id, ci.group_id, ci.group_name,
        ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
        sof.units_shipped ,
        round(coalesce(allocated_price, (sof.qty_ordered * (base_price + coalesce(base_discount_amt,0)))- (coalesce(qty_refunded,0) * coalesce(credit_price,0))) - (sof.qty_ordered - sof.units_shipped)::numeric(12,2) * coalesce(sof.tpa_unit_amt,0),2), 
        current_timestamp, sof.sale_date, ci.item_id, sof.qty_ordered * ci.net_unit_cost,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.units_shipped  > 0 and sof.product_type <> 'bundle'
        """
    ,
    "24 -update fulfillment coupon discounts":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, tran_qty, tran_amt, rule_id, coupon_rule_name, coupon_group_code, created_at, sale_date, item_id, tran_gl_date, ship_date)
        select 300, 69, 'Coupon', sof.tran_date, 'order', sof.order_id, sof.increment_id, sof.item_id, sof.customer_id, 1, sof.fc, sof.magento_fc ,
        sof.sku , sof.parent_item_id, ci.group_id, ci.group_name, ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name,
        ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
        sof.units_shipped, odd.amt_off_applied, odd.rule_id, odd.rule_name, odd.rule_group, current_timestamp, sof.sale_date, ci.item_id,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        inner join ods.order_detail_discounts odd on sof.item_id = odd.item_id
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.product_type <> 'bundle' and odd.discount_type = 'coupon' and sof.units_shipped  > 0
        """
    ,
    "25 - update fulfillment coupon discounts allocated":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, tran_qty, tran_amt, rule_id, coupon_rule_name, coupon_group_code, created_at, sale_date, item_id, tran_gl_date, ship_date)
        select 300, 69, 'Coupon', sof.tran_date, 'order', sof.order_id, sof.increment_id, sof.item_id, sof.customer_id, 1, sof.fc, sof.magento_fc ,
        sof.sku , sof.parent_item_id, ci.group_id, ci.group_name, ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name,
        ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
        sof.units_shipped, sbad.alloc_discount_amt, sbad.rule_id, sbad.rule_name, sbad.rule_group, current_timestamp, sof.sale_date, ci.item_id,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        inner join staging.stage_bundle_alloc_discounts sbad on sof.item_id = sbad.item_id
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.product_type <> 'bundle' and sbad.discount_type = 'coupon'  and sof.units_shipped  > 0
        """
    ,
    "26  - update TPA discounts":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, rule_id, rule_name, tran_qty, tran_amt, created_at, sale_date,item_id, tran_gl_date, ship_date)
        select 300, 48, 'TPA', sof.tran_date, 'order', sof.order_id, sof.increment_id, sof.item_id, sof.customer_id, 1, sof.fc, sof.magento_fc ,
        sof.sku , sof.parent_item_id, ci.group_id, ci.group_name, ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name,
        ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name, odd.rule_id,
        odd.rule_name, sof.units_shipped,
        case when sof.units_shipped = odd.units_applied then odd.amt_off_applied else round(sof.units_shipped * odd.discount_amt,2) end, current_timestamp, sof.sale_date, ci.item_id,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        inner join ods.order_detail_discounts odd on sof.item_id = odd.item_id
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.product_type <> 'bundle' and odd.discount_type = 'tpa' and sof.units_shipped > 0 and sof.base_price > 0
        """
    ,
    "27 - update TPA discounts bundles":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, rule_id, rule_name, tran_qty, tran_amt, created_at, sale_date, item_id, tran_gl_date, ship_date)
        select 300, 48, 'TPA', sof.tran_date, 'order', sof.order_id, sof.increment_id, sof.item_id, sof.customer_id, 1, sof.fc, sof.magento_fc ,
        sof.sku , sof.parent_item_id, ci.group_id, ci.group_name, ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name,
        ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name, sbad.rule_id,
        sbad.rule_name, sof.units_shipped, sbad.alloc_discount_amt, current_timestamp, sof.sale_date, ci.item_id,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        inner join staging.stage_bundle_alloc_discounts sbad on sof.item_id = sbad.item_id
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.product_type <> 'bundle' and sbad.discount_type = 'tpa'and sof.units_shipped > 0
        """
    ,
    "27.5 - update tiered pricing discount":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
        sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, rule_id, rule_name, tran_qty, tran_amt, created_at, sale_date,item_id, tran_gl_date, ship_date)
        select 300, 98, 'Tiered Pricing', sof.tran_date, 'order', sof.order_id, sof.increment_id, sof.item_id, sof.customer_id, 1, sof.fc, sof.magento_fc ,
        sof.sku , sof.parent_item_id, ci.group_id, ci.group_name, ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name,
        ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name, odd.rule_id,
        odd.rule_name, sof.units_shipped,
        case when sof.units_shipped = odd.units_applied then odd.amt_off_applied else round(sof.units_shipped * odd.discount_amt,2) end, current_timestamp, sof.sale_date, ci.item_id,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date, 
        convert_timezone('UTC', 'America/Los_Angeles', sof.fulfilled_at)::date
        from staging.stage_order_fulfillment sof
        inner join ods.order_detail_discounts odd on sof.item_id = odd.item_id
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        where sof.product_type <> 'bundle' and odd.discount_type = 'tiered_pricing' and sof.units_shipped > 0 and sof.base_price > 0
        """,
    "28 - Make good discounts":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, company_id, tran_amt, created_at, gift_card_id, gc_type, 
        gc_sub_type, order_id,  order_type, increment_id, gc_reason_id , gc_reason_name , gc_group_code, gc_group_name, sale_date, tran_gl_date)
        select  300, 64, 'Make-Good Discount', sof.tran_date, 1, op.pay_amt, current_timestamp, op.gift_card_id::int,99, 'Make-Good',
        sof.order_id, 'order', sof.increment_id,
        coalesce(gca.reason_id, gc.reason_id), coalesce(gca.reason, gc.reason), coalesce(gca.group_code, gc.group_code), coalesce(gca.group_name, gc.group_name), sof.sale_Date,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date
        from ods.order_payments op
        inner join (select distinct order_id, increment_id, tran_date, sale_date from staging.stage_order_fulfillment where is_last_shipment = true) sof on op.order_id = sof.order_id
        LEFT OUTER JOIN (
					        SELECT CARD_ID, ISSUE_TYPE, max(GROUP_CODE) AS GROUP_CODE, max(GROUP_LABEL) AS GROUP_name,
							max(REASON_ID) AS REASON_ID, max(REASON_LABEL) AS REASON 
							FROM ods.GIFT_CARD_ADJUSTMENTS 
							GROUP BY CARD_ID, ISSUE_TYPE) gca ON op.gift_card_id::int = gca.CARD_ID 
        left outer join ods.gift_cards gc on op.gift_card_id::int = gc.card_id
        where op.payment_type = 'gc' and gca.issue_type = 'Make-Good' and op.sale_date >= '9/30/21' 
        """
    ,
    "29 - update grocery shipment revenue":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id,  customer_id, company_id,
         tran_amt, created_at, sale_date, tran_gl_date)
        select 300 as tran_type, 17, 'Grocery Shipment' as tran_sub_type, sof.tran_date, 'order' as order_type, oh.order_id, oh.increment_id, oh.customer_id,1,
        oh.grocery_base_shipment, current_timestamp, sof.sale_date,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date
        from ods.order_header oh
        inner join (select distinct order_id, tran_date, sale_date from staging.stage_order_fulfillment where is_last_shipment = true) sof on oh.order_id = sof.order_id
        where grocery_base_shipment > 0
        """
    ,
    "30 - update frozen shipment revenue":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id,  customer_id, company_id,
         tran_amt, created_at, sale_date, tran_gl_date)
        select 300 as tran_type, 70, 'Frozen Shipment' as tran_sub_type, sof.tran_date, 'order' as order_type, oh.order_id, oh.increment_id, oh.customer_id,1,
        oh.frozen_base_shipment, current_timestamp, sof.sale_date, convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date
        from ods.order_header oh
        inner join (select distinct order_id, tran_date, sale_date from staging.stage_order_fulfillment where is_last_shipment = true) sof on oh.order_id = sof.order_id
        where oh.frozen_base_shipment > 0
        """
    ,
    "31 - update wine shipment revenue":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id,  customer_id, company_id,
         tran_amt, created_at, sale_date, tran_gl_date)
        select 300 as tran_type, 71, 'Wine Shipment' as tran_sub_type, sof.tran_date, 'order' as order_type, oh.order_id, oh.increment_id, oh.customer_id,1,
        oh.wine_base_shipment, current_timestamp, sof.sale_date, convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date
        from ods.order_header oh
        inner join (select distinct order_id, tran_date, sale_date from staging.stage_order_fulfillment where is_last_shipment = true) sof on oh.order_id = sof.order_id
        where oh.wine_base_shipment > 0
        """
    ,
    "32 - update ppe fee revenue":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id,  customer_id, company_id,
        tran_amt, created_at, sale_date, tran_gl_date)
        select 300 as tran_type, 96, 'PPE Revenue' as tran_sub_type, sof.tran_date, 'order' as order_type, oh.order_id, oh.increment_id, oh.customer_id,1,
        oh.ppe_fee, current_timestamp, sof.sale_date,
        convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date )::date
        from ods.order_header oh
        inner join (select distinct order_id, tran_date, sale_date from staging.stage_order_fulfillment where is_last_shipment = true) sof on oh.order_id = sof.order_id
        where oh.ppe_fee> 0
        """
    ,"33 - update Vendor Rebates":
         """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, order_line_id, customer_id, company_id, location_id, magento_location_id,
                sku, parent_item_id, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
                subclass_id, subclass_name,tran_qty, tran_amt, created_at, sale_date, item_id, tran_cost_amt, tran_gl_date, ship_date,rebate_start_date,rebate_end_date,rebate_percentage,rebate_dollar_amount,rebate_billing_period,REBATE_TYPE,REBATE_CALC_TYPE,REBATE_BILLING_METHOD,REBATE_TOTAL) 
        select 300 as tran_type, 111, 'Vendor Rebates'  as tran_sub_type, sof.tran_date, 'order' as order_type, sof.order_id, sof.increment_id,
        sof.item_id as order_line_id, sof.customer_id,1, sof.fc, sof.magento_fc, sof.sku, sof.parent_item_id, ci.group_id, ci.group_name,
        ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name,
        sof.units_shipped 	, CASE	
            WHEN CASE 
            WHEN convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
            THEN 'Y'
            ELSE 'N'
        END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * (sof.units_shipped * ci.net_unit_cost)
            WHEN CASE 
            WHEN convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
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
        , CASE	
            WHEN CASE 
            WHEN convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
            THEN 'Y'
            ELSE 'N'
        END ='Y' AND COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) IS NOT NULL THEN (COALESCE(vr.percentage,vrin.percentage,br.percentage,brin.percentage) / 100) * (sof.units_shipped * ci.net_unit_cost)
            WHEN CASE 
            WHEN convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) >= COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) AND (convert_timezone('UTC', 'America/Los_Angeles', sof.tran_date ) <= COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) OR COALESCE(vr.end_DATE,vrin.end_date,br.end_date,brin.end_date) IS NULL)
            THEN 'Y'
            ELSE 'N'
        END ='Y' AND COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) IS NOT NULL THEN COALESCE(vr.dollar_amount,vrin.dollar_amount,br.dollar_amount,brin.dollar_amount) * (sof.units_shipped )
        END AS rebate_total
        from staging.stage_order_fulfillment sof
        left outer join ods.ns_fc_xref nfx on sof.fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on sof.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        LEFT JOIN (SELECT DISTINCT * FROM TM_IGLOO_ODS_STG.ods.NS_VENDOR_REBATES WHERE start_date < sysdate() AND end_date> sysdate() AND active_status_id = 1 )vr ON vr.ITEM_ID =ci.ITEM_ID
        LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES WHERE start_date < sysdate() AND end_date> sysdate() AND active_status_id = 2 )vrin ON vrin.ITEM_ID =ci.ITEM_ID
        LEFT JOIN (SELECT DISTINCT * FROM ods.ns_vendor_rebates vr WHERE vr.ITEM_ID IS NULL AND active_status_id =1 AND start_date < sysdate() AND end_date> sysdate()) br ON  br.brand_id=ci.brand_id
        LEFT JOIN (SELECT DISTINCT ACTIVE_STATUS_ID,ACTIVE_STATUS,BILLING_PERIOD_ID,BILLING_PERIOD,BRAND_ID,BRAND_RECORDS_NAME,START_DATE,END_DATE,DOLLAR_AMOUNT,PERCENTAGE,ITEM_ID,IS_INACTIVE,TYPE_ID,REBATE_TYPE,BILLING_METHOD_ID,BILLING_CUSTOMER_ID,VENDOR_ID,BRAND_SKU,DATE_PROCESSED,REBATE_DATE FROM ods.NS_VENDOR_REBATES  WHERE ITEM_ID IS NULL AND active_status_id =2 AND start_date <sysdate() AND end_date> sysdate()) brin ON  brin.brand_id=ci.brand_id
        where sof.units_shipped  > 0 and sof.product_type <> 'bundle' AND COALESCE(vr.START_DATE,vrin.start_date,br.start_date,brin.start_date) IS NOT null
        """
        ,
    "33.5 - Link Rebates to Fulfillments":
        """    
        
        UPDATE ods."TRANSACTIONS" 
            SET 
            TRANSACTIONS.FULFILLMENT_TRAN_ID = upd.tran_id
            FROM (
            SELECT trn.tran_id, trn.ORDER_LINE_ID 
            FROM ods.TRANSACTIONS trn
            WHERE trn.tran_sub_type_id = 16) upd
            WHERE TRANSACTIONS.ORDER_LINE_ID = upd.ORDER_LINE_ID AND TRANSACTIONS.TRAN_SUB_TYPE_ID =111 AND TRANSACTIONS.fulfillment_tran_id IS NULL 
        """
        ,
    "36 - Create Thrive Cash Discount for fulfilled orders":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, increment_id, customer_id, 
        company_id, tran_amt, created_at, sale_date, tran_gl_date)
        select 300, 62, 'Thrive Cash Discount', tran_date, 'order', ord.order_id, ord.increment_id, ord.customer_id, 1, op.pay_amt, current_timestamp, ord.sale_date,
        convert_timezone('UTC', 'America/Los_Angeles', ord.tran_date )::date
        from (
                select distinct order_id, tran_date, customer_id,increment_id, sale_date
                from staging.stage_order_fulfillment
                where is_last_shipment = true) ord
        inner join ods.order_payments op on ord.order_id = op.order_id
        where op.payment_type = 'tc' and op.pay_amt > 0
        """
    ,
    "40 - update order detail with product fulfillment":
        """
        update ods.order_detail
        set fulfillment_qty = upd.units_shipped, short_qty = upd.units_shorted, fulfillment_date = upd.fulfilled_at, ods_updated_at = current_timestamp 
        from (
                select item_id, units_shipped, units_shorted, fulfilled_at
                from staging.stage_order_fulfillment) upd
        where order_detail.item_id = upd.item_id
        """
    ,
    "41 - update order_header status and gaap_sale_date ":
        """
        update ods.order_header
        set status = 'shipped', status_date = fulfilled_at, gaap_sale_date = convert_timezone('UTC', 'America/Los_Angeles', fulfilled_at)::date, updated_at = current_timestamp 
        from (
        select order_id, max(fulfilled_at) as fulfilled_at
        from staging.stage_order_fulfillment sof
        where is_last_shipment = 'true'
        group by order_id) upd
        where order_header.order_id = upd.order_id and order_header.gaap_sale_date is null
        """
    ,
    "50 - copy staging.stg_order_fulfillment_detail_v1 to processed":
        """
        insert into staging.stg_order_fulfillment_detail_v1_processed
        select sofd.*
        from staging.stg_order_fulfillment_detail_v1 sofd
        inner join staging.fulfill_work fw on sofd.order_id = fw.order_id
        """
    ,
    "51 - copy staging.stg_order_fulfillment_v1 to processed":
        """
        insert into staging.stg_order_fulfillment_v1_processed
        select sof.*
        from staging.stg_order_fulfillment_v1 sof
        inner join staging.fulfill_work fw on sof.order_id = fw.order_id
        """
    ,
    "58 - delete records from stg_order_fulfillment_detail_v1":
        """
        delete from staging.stg_order_fulfillment_detail_v1
        where order_id in (select order_id from staging.fulfill_work)
        """
    ,
    "59 - delete records from stg_order_fulfillment_v1":
        """
        delete from staging.stg_order_fulfillment_v1
        where order_id in (select order_id from staging.fulfill_work)
        """
}

spGiftCardFulfillment = {
    "1 - Truncate work file":
        "truncate staging.temp_gc_fulfillments"
    ,
    "2 - Update work file with gc fulfillments":
        """
        insert into staging.temp_gc_fulfillments
        select sgf.item_id, sgf.order_id, sgf.card_id, sgf.updated_at, null
        from staging.stg_giftcard_fulfillment_v1 sgf
        inner join ods.order_detail od on sgf.item_id = od.item_id
        """,
    "3 - Create gift card fulfillment transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, company_id, tran_amt, tran_full_price, tran_discount_amt, created_at, gift_card_id, gc_type, 
        gc_sub_type, order_id, order_line_id, order_type, increment_id, gc_reason_name,  gc_group_name, sale_date, tran_gl_date)
        select  case when sgf.card_type = 1 then 310 else 320 end as tran_type, case when sgf.card_type = 1 then 30 else 32 end , 'Purchased',  sgf.updated_at, 1, 
        od.base_price, base_original_price, base_discount_amt, current_timestamp, sgf.card_id, sgf.card_type,
        'Purchased', od.order_id, sgf.item_id, 'order', oh.increment_id, 'Purchased', 'Purchased', oh.sale_date, 
        convert_timezone('UTC', 'America/Los_Angeles',sgf.updated_at)::date
        from staging.stg_giftcard_fulfillment_v1 sgf
        inner join staging.temp_gc_fulfillments tgf on sgf.item_id = tgf.item_id
        inner join staging.stg_gift_card_adjustments_v1 sgca on sgf.card_id = sgca.card_id::int and 'Created' = sgca.action_label
        inner join ods.order_detail od on sgf.item_id = od.item_id
        inner join ods.order_header oh on od.order_id = oh.order_id
        """,
    "4 - Update order_detail with fulfillment qty":
        """
        update ods.order_detail
        set fulfillment_qty = 1, ods_updated_at = current_timestamp 
        where item_id in (select item_id from staging.temp_gc_fulfillments)
        """,
    "5 - Update order_header with update_at time":
        """
        update ods.order_header
        set updated_at = current_timestamp
        where order_id in (select distinct order_id from staging.temp_gc_fulfillments)
        """,
    "6 - Update gift_card table":
        """
        insert into ods.gift_cards(card_id, card_type, initial_amount, current_balance, issue_type, order_id, order_increment_id,
        item_id, created_at, updated_at, source_uuid )
        select sgf.card_id, sgf.card_type, sgf.card_amt, sgf.card_amt, 'purchased', sgf.order_id, sgf.increment_id,
        sgf.item_id, sgf.updated_at, current_timestamp, sgf.message_uuid
        from staging.temp_gc_fulfillments tgf 
        inner join staging.stg_giftcard_fulfillment_v1 sgf on tgf.item_id = sgf.item_id
        """,
    "7 - insert stg_giftcard_fulfillment_v1 into processed":
        """
        insert into staging.stg_giftcard_fulfillment_v1_processed
        select sgf.*
        from staging.stg_giftcard_fulfillment_v1 sgf
        inner join staging.temp_gc_fulfillments tgf on sgf.item_id = tgf.item_id
        """,
    "8 - delete stg_giftcard_fulfillment_v1 processed":
        """
        delete from staging.stg_giftcard_fulfillment_v1 del
        where exists (	select 1
                        from staging.stg_giftcard_fulfillment_v1 sgf
                        inner join staging.temp_gc_fulfillments tgf on sgf.item_id = tgf.item_id
                        where sgf.item_id = del.item_id)
        """
}

spUpdateCreditMemo = {
    "1 - Truncate ship_cm":
        """
        truncate staging.ship_cm
        """
    ,
    "2 - Insert Pre ODS pre-shipment credits to work table":
        """
        insert into staging.ship_cm 
        select credit_id, '1/1/21'
        from staging.stg_credit_memo_v1 scm
        where  order_has_shipped = 'false' and coalesce(scm.base_total_product,0) > 0 and 
        scm.order_id < (select int_value from ods.control where control_name = 'pre_ods_order_id') 
        """,
    "3 - create short shipment transaction detail for Pre ODS pre-shipment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, order_line_id, increment_id, customer_id, company_id, tran_qty, tran_amt, tran_full_price, tran_tax_amt,
        tran_discount_amt, created_at, reason_id, refund_reason, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name , 
        subclass_id, subclass_name, credit_memo_id, sale_date, sku, item_id, tran_gl_date)
        select 365, 82, 'Product', scm.updated_at, 'order', scm.order_id, scmi.order_item_id, scm.increment_id, scm.customer_id,1, scmi.qty_refunded,
        (scmi.qty_refunded * scmi.base_price) - coalesce(scmi.base_discount_amt,0) + coalesce(scmi.base_tax_amt,0),
        (scmi.qty_refunded * scmi.base_price), coalesce(scmi.tax_amt,0), coalesce(scmi.discount_amt,0), current_timestamp, 0,'Short Shipment', ci.group_id, ci.group_name,
        ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name, scm.credit_id, sc.sale_date,
        od.sku, ci.item_id, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        left outer join staging.stg_credit_memo_items_v1 scmi on scm.credit_id = scmi.credit_id
        left outer join ods.order_detail od on scmi.order_item_id = od.item_id
        left outer join ods.ns_fc_xref nfx on od.assignment_fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on scmi.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        """,
    "4 - create short shipment transaction refund payment Pre ODS pre-shipment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt, tran_full_price, tran_tax_amt,
        cc_auth,  cc_type, created_at, increment_id, reason_id, refund_reason, credit_memo_id, sale_date, tran_gl_date)
        select case when split_part(increment_id,'-',1)::int >  99999999 then 365 else 380 end, case when split_part(increment_id,'-',1)::int >  99999999 then 38 else 84 end, 'Refund Capture' , scm.updated_at, 'order', scm.order_id, scm.customer_id,1, scmr.base_pay_amt, 
        scm.base_total_product - scm.base_total_gift_cards + scm.base_donations + scm.ppe_fee + (scm.base_shipment - scm.base_shipment_tax - coalesce(scm.base_frozen_shipment, 0) - coalesce(scm.base_frozen_shipment_tax, 0) - coalesce(scm.base_wine_shipment, 0) - coalesce(scm.base_wine_shipment_tax, 0)) + scm.base_total_discount + base_adjustment - total_thrive_cash, scm.base_total_tax, 
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, scm.increment_id, 0,'Short Shipment', scm.credit_id, sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where scmr.base_pay_amt > 0 
        """,
    "5 - create short shipment transaction refund to Gift Card Pre ODS pre-shipment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_qty, tran_amt,  tran_tax_amt, tran_full_price, 
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select case when split_part(increment_id,'-',1)::int >  99999999 then 360 else 380 end, case when split_part(increment_id,'-',1)::int >  99999999 then 79 else 23 end, 'Gift Card', scm.updated_at, 'order', 
        scm.order_id, scm.customer_id,1,1,
        scmgc.gift_card_amt,  scm.base_total_tax,  scmgc.gift_card_amt - scm.base_total_tax,
         current_timestamp, scm.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0
        """,
    "6 - truncate staging.ship_cm_processed":
        """
        truncate staging.ship_cm_processed
        """,
    "7 - insert processed cm into processed work table":
        """
        insert into staging.ship_cm_processed
        select *
        from staging.ship_cm
        """,
    "8 - Truncate ship_cm":
        "truncate staging.ship_cm"
    ,
    "9 - Insert Pre ODS post-shipment and appeasment credits to work table":
        """
        insert into staging.ship_cm 
        select credit_id, '1/1/21'
        from staging.stg_credit_memo_v1 scm
        where  (order_has_shipped = 'true' or coalesce(scm.base_total_product,0) = 0) and
        scm.order_id < (select int_value from ods.control where control_name = 'pre_ods_order_id')  
        """,
    "10 - process shipped returns to credit card for Pre ODS post-shipment and appeasment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt,  tran_tax_amt, tran_full_price, 
         cc_auth,  cc_type, created_at, increment_id,  credit_memo_id, sale_date, tran_gl_date)
        select 360, case when scm.base_total_product > 0 then 81 else 78 end,
        case when scm.base_total_product > 0 then 'Refund Capture' else 'Appeasement Capture' end, scm.updated_at, 'order', scm.order_id, scm.customer_id,1,
        scmr.base_pay_amt,  scm.base_total_tax, scmr.base_pay_amt - scm.base_total_tax,
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, scm.increment_id, scm.credit_id, sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where  scmr.base_pay_amt > 0 and split_part(increment_id,'-',1)::int >  99999999 
        """,
    "10.5 - process shipped returns to credit card for Pre ODS post-shipment and appeasment credits Membership":
        """
         insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt,  tran_tax_amt, tran_full_price, 
         cc_auth,  cc_type, created_at, increment_id,  credit_memo_id, sale_date, tran_gl_date)
        select 380, case when scm.base_total_product > 0 then 84 else 83 end, 
        case when scm.base_total_product > 0 then 'Refund Capture' else 'Appeasement Capture' end, scm.updated_at, 'order', scm.order_id, scm.customer_id,1,
        scmr.base_pay_amt,  scm.base_total_tax, scmr.base_pay_amt - scm.base_total_tax,
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, scm.increment_id, scm.credit_id, sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where  scmr.base_pay_amt > 0 and split_part(increment_id,'-',1)::int <=  99999999 
        """,
    "11 - process shipped returns to gift card for Pre ODS post-shipment and appeasment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt,  tran_tax_amt, tran_full_price,
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select 360, case when scm.base_total_product > 0 then 79 else 80 end,
        case when scm.base_total_product > 0 then 'Gift Card' else 'Gift Card Appeasement' end, scm.updated_at, 'order', scm.order_id, scm.customer_id,1,
        scmgc.gift_card_amt,  scm.base_total_tax, scmgc.gift_card_amt - scm.base_total_tax,
         current_timestamp, scm.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0 and split_part(increment_id,'-',1)::int >  99999999
        """,
    "11.5 - process shipped returns to gift card for Pre ODS post-shipment and appeasment credits":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt,  tran_tax_amt, tran_full_price,
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select 380, case when scm.base_total_product > 0 then 23 else 92 end,
        case when scm.base_total_product > 0 then 'Gift Card' else 'Gift Card Appeasement' end, scm.updated_at, 'order', scm.order_id, scm.customer_id,1,
        scmgc.gift_card_amt,  scm.base_total_tax, scmgc.gift_card_amt - scm.base_total_tax,
         current_timestamp, scm.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0 and split_part(increment_id,'-',1)::int <=  99999999
        """,
    "12 - update gift_card table with new gift cards":
        """
        insert into ods.gift_cards (card_id, card_type, initial_amount, current_balance, issue_type, order_id, order_increment_id,  created_at, updated_at, source_uuid)
        select scmgc.gift_card_id::int, 1, scmgc.gift_card_amt, scmgc.gift_card_amt, 'refund', 
        scm.order_id, scm.increment_id, scm.updated_at, current_timestamp, scm.message_uuid
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        left outer join ods.gift_cards gc on sgca.card_id::int = gc.card_id
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0 AND
        (scmgc.gift_card_id NOT IN (SELECT CARD_ID a FROM ods.GIFT_CARDS))
        """,
    "13 - insert processed cm into processed work table":
        """
        insert into staging.ship_cm_processed
        select *
        from staging.ship_cm
        """,
    "14 - Truncate ship_cm":
        "truncate staging.ship_cm"
    ,
    "15 -insert records to work table pre-shipment":
        """
        insert into staging.ship_cm 
        select credit_id, oh.sale_date
        from staging.stg_credit_memo_v1 scm
        inner join ods.order_header oh on scm.order_id = oh.order_id
        where  order_has_shipped = 'false' and scm.order_id >= (select int_value from ods.control 
        where control_name = 'pre_ods_order_id') and coalesce(scm.base_total_product,0) > 0 
        """,
    "16 - create short shipment transaction detail pre-shipment":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, order_line_id, increment_id, customer_id, company_id, tran_qty, tran_amt, tran_full_price, tran_tax_amt,
        created_at, reason_id, refund_reason, group_id ,group_name, category_id, category_name, sub_category_id, sub_category_name, class_id , class_name ,
        subclass_id, subclass_name, credit_memo_id, sale_date, sku, item_id, tran_gl_date)
        select 365, 82, 'Product', scm.updated_at, 'order', scm.order_id, scmi.order_item_id, scm.increment_id, scm.customer_id,1, scmi.qty_refunded,
        (scmi.qty_refunded * scmi.base_price) - coalesce(scmi.base_discount_amt,0) + coalesce(scmi.base_tax_amt,0),
        (scmi.qty_refunded * scmi.base_price) - coalesce(scmi.discount_amt,0), coalesce(scmi.tax_amt,0),  current_timestamp, 0,'Short Shipment', ci.group_id, ci.group_name,
        ci.item_category_id, ci.item_category_name, ci.subcategory_id, ci.subcategory_name, ci.class_id, ci.class_name, ci.subclass_id, ci.subclass_name, scm.credit_id, sc.sale_date,
        od.sku, ci.item_id, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        left outer join staging.stg_credit_memo_items_v1 scmi on scm.credit_id = scmi.credit_id
        left outer join ods.order_detail od on scmi.order_item_id = od.item_id
        left outer join ods.ns_fc_xref nfx on od.assignment_fc = nfx.ns_fc_id
        left outer join ods.curr_items ci on scmi.sku = ci.item_name and coalesce(nfx.ods_fc_id,2)  = ci.fc_id
        """,
    "17 - create short shipment transaction refund payment pre-shipment":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_amt, tran_full_price, tran_tax_amt,
        cc_auth,  cc_type, created_at, increment_id, reason_id, refund_reason, credit_memo_id, sale_date, tran_gl_date)
        select 365, 38, 'Refund Capture' , scm.updated_at, 'order', scm.order_id, scm.customer_id,1, scmr.base_pay_amt, 
        scm.base_total_product - scm.base_total_gift_cards + scm.base_donations + scm.ppe_fee + (scm.base_shipment - coalesce(scm.base_frozen_shipment, 0) - coalesce(scm.base_wine_shipment, 0)) + scm.base_total_discount + base_adjustment - total_thrive_cash, scm.base_total_tax, 
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, scm.increment_id, 0,'Short Shipment', scm.credit_id, 
        sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where scmr.base_pay_amt > 0 
        """,
    "18 - create short shipment transaction refund gift card pre-shipment":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_full_price,  tran_tax_amt, tran_amt,
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select 365, 36, 'Gift Card', scm.updated_at, 'order', scm.order_id, oh.customer_id,1,
        scmgc.gift_card_amt - coalesce(scm.base_total_tax,0),  scm.base_total_tax, scmgc.gift_card_amt,
         current_timestamp, oh.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        inner join ods.order_header oh on scm.order_id = oh.order_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0
        """,
    "19 - insert processed cm into processed work table":
        """
        insert into staging.ship_cm_processed
        select *
        from staging.ship_cm
        """,
    "20 - Truncate ship_cm":
        "truncate staging.ship_cm"
    ,
    "21 -insert records to work table post-shipment credits":
        """
        insert into staging.ship_cm
        select credit_id, oh.sale_date
        from staging.stg_credit_memo_v1 scm
        inner join ods.order_header oh on scm.order_id = oh.order_id
        where  (order_has_shipped = 'true' or coalesce(scm.base_total_product,0) = 0) 
        and scm.order_id >= (select int_value from ods.control where control_name = 'pre_ods_order_id')
        """,
    "22 - process post-shipment returns to credit card":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_full_price,  tran_tax_amt, tran_amt,
        cc_auth,  cc_type, created_at, increment_id,  credit_memo_id, sale_date, tran_gl_date)
        select 360, case when scm.base_total_product > 0 then 81 else 78 end,
        case when scm.base_total_product > 0 then 'Refund Capture' else 'Appeasement Capture' end, scm.updated_at, 'order', scm.order_id, oh.customer_id,1,
        scmr.base_pay_amt - coalesce(scm.base_total_tax,0),  scm.base_total_tax, scmr.base_pay_amt,
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, oh.increment_id, scm.credit_id, sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join ods.order_header oh on scm.order_id = oh.order_id
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where  scmr.base_pay_amt > 0 
        """,
    "23 - process post-shipment returns to gift card":
        """
         insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_full_price,  tran_tax_amt, tran_amt,
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select 360, case when scm.base_total_product > 0 then 79 else 80 end, 
        case when scm.base_total_product > 0 then 'Gift Card' else 'Gift Card Appeasement' end, scm.updated_at, 'order', scm.order_id, oh.customer_id,1,
        scmgc.gift_card_amt - coalesce(scm.base_total_tax,0),  scm.base_total_tax, scmgc.gift_card_amt,
         current_timestamp, oh.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         sc.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        inner join ods.order_header oh on scm.order_id = oh.order_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0
        """,
    "24 - update gift_card table with new gift cards":
        """
        insert into ods.gift_cards (card_id, card_type, initial_amount, current_balance, issue_type, order_id, order_increment_id,  created_at, updated_at, source_uuid)
        select scmgc.gift_card_id::int, 1, scmgc.gift_card_amt, scmgc.gift_card_amt, 'refund', scm.order_id, scm.increment_id, scm.updated_at, current_timestamp, scm.message_uuid
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm sc on scm.credit_id = sc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        left outer join ods.gift_cards gc on sgca.card_id::int = gc.card_id
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0 and gc.card_id is NULL AND 
        (scmgc.gift_card_id NOT IN (SELECT CARD_ID a FROM ods.GIFT_CARDS))
        """,
    "24 - insert processed cm into processed work table":
        """
        insert into staging.ship_cm_processed
        select *
        from staging.ship_cm
        """,
    "25 - Truncate ship_cm":
        "truncate staging.member_cm"
    ,
    "26 - insert records to work table for membership credit":
        """
        insert into staging.member_cm
        select credit_id
        from staging.stg_credit_memo_v1 scm
        inner join ods.membership_header mh on scm.order_id = mh.order_id
        where scm.order_id >= (select int_value from ods.control where control_name = 'pre_ods_order_id')
        """,
    "27 - process membership returns to credit card":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_full_price,  tran_tax_amt, tran_amt,
         cc_auth,  cc_type, created_at, increment_id,  credit_memo_id, sale_date, tran_gl_date)
        select 380, case when scm.base_total_product > 0 then 84 else 83 end,
        case when scm.base_total_product > 0 then 'Refund Capture' else 'Appeasement Capture' end, scm.updated_at, 'order', scm.order_id, mh.customer_id,1,
        scmr.base_pay_amt - coalesce(scm.base_total_tax,0)- coalesce(scm.base_total_gift_cards,0) + scm.base_donations + scm.base_shipment,  scm.base_total_tax, scmr.base_pay_amt,
        scmr.gateway_tran_id, scmr.cc_type, current_timestamp, mh.increment_id, scm.credit_id, mh.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join ods.membership_header mh on scm.order_id = mh.order_id
        inner join staging.member_cm mc on scm.credit_id = mc.credit_id
        left outer join staging.stg_credit_memo_refunds_v1 scmr on scm.credit_id = scmr.credit_id
        where  scmr.base_pay_amt > 0
        """,
    "28 - process mebership returns to gift card":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date, order_type, order_id, customer_id, company_id,  tran_full_price,  tran_tax_amt, tran_amt,
         created_at, increment_id,  credit_memo_id, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, sale_date, tran_gl_date)
        select 380, case when scm.base_total_product > 0 then 23 else 92 end, 
        case when scm.base_total_product > 0 then 'Gift Card' else 'Gift Card Appeasement' end, scm.updated_at, 'order', scm.order_id, mh.customer_id,1,
        scmgc.gift_card_amt - coalesce(scm.base_total_tax,0),  scm.base_total_tax, scmgc.gift_card_amt,
         current_timestamp, mh.increment_id, scm.credit_id, scmgc.gift_card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'refund', 
         mh.sale_date, convert_timezone('UTC', 'America/Los_Angeles', scm.updated_at)::date
        from staging.stg_credit_memo_v1 scm
        inner join staging.member_cm mc on scm.credit_id = mc.credit_id
        inner join ods.membership_header mh on scm.order_id = mh.order_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0
        """,
    "29 - update gift_card table with new gift cards":
        """
        insert into ods.gift_cards (card_id, card_type, initial_amount, current_balance, issue_type, order_id, order_increment_id,  created_at, updated_at, source_uuid)
        select scmgc.gift_card_id::int, 2, scmgc.gift_card_amt, scmgc.gift_card_amt, 'refund', scm.order_id, scm.increment_id, scm.updated_at, current_timestamp, scm.message_uuid
        from staging.stg_credit_memo_v1 scm
        inner join staging.member_cm mc on scm.credit_id = mc.credit_id
        left outer join staging.stg_credit_memo_gift_cards_v1 scmgc on scm.credit_id = scmgc.credit_id
        left outer join staging.stg_gift_card_adjustments_v1 sgca on scmgc.gift_card_id = sgca.card_id and 'Created' = sgca.action_label
        where  scmgc.gift_card_id::int > 0 and scmgc.gift_card_amt > 0 AND 
        (scmgc.gift_card_id NOT IN (SELECT CARD_ID a FROM ods.GIFT_CARDS))
        """,
    "30 - insert processed cm into processed work table":
        """
        insert into staging.ship_cm_processed
        select *, null
        from staging.member_cm
        """,
    "31 - update processed refunds into credit_memo_refunds":
        """
        insert into ods.credit_memo_refunds
        select scmr.*
        from staging.stg_credit_memo_refunds_v1 scmr
        inner join staging.ship_cm_processed scp on scmr.credit_id = scp.credit_id
        """,
    "32 - update processed refunds into credit_memo_gift_cards":
        """
        insert into ods.credit_memo_gift_cards
        select scmgc.*
        from staging.stg_credit_memo_gift_cards_v1 scmgc
        inner join staging.ship_cm_processed scp on scmgc.credit_id = scp.credit_id
        """,
    "33 - update processed refunds into credit_memo_items":
        """
        insert into ods.credit_memo_items
        select scmi.*
        from staging.stg_credit_memo_items_v1 scmi
        inner join staging.ship_cm_processed scp on scmi.credit_id = scp.credit_id
        """,
    "34 - update processed refunds into credit_memo":
        """
        insert into ods.credit_memo
        select current_timestamp as load_date, scm.*
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm_processed scp on scm.credit_id = scp.credit_id
        """,
    "35 - copy stg_credit_memo_refunds_v1 to processed":
        """
        insert into staging.stg_credit_memo_refunds_v1_processed
        select scmr.*
        from staging.stg_credit_memo_refunds_v1 scmr
        inner join staging.ship_cm_processed scp on scmr.credit_id = scp.credit_id
        """,
    "36 - delete processed from stg_credit_memo_refunds_v1":
        """
        delete from staging.stg_credit_memo_refunds_v1
        where credit_id in (select credit_id from staging.ship_cm_processed)
        """,
    "37 - copy stg_credit_memo_refunds_v1 to processed":
        """
        insert into staging.stg_credit_memo_gift_cards_v1_processed
        select scmgc.*
        from staging.stg_credit_memo_gift_cards_v1 scmgc
        inner join staging.ship_cm_processed scp on scmgc.credit_id = scp.credit_id
        """,
    "38 - delete processed from stg_credit_memo_gift_cards_v1":
        """
        delete from staging.stg_credit_memo_gift_cards_v1
        where credit_id in (select credit_id from staging.ship_cm_processed)
        """,
    "39 - copy staging.stg_credit_memo_items_v1 processed":
        """
        insert into staging.stg_credit_memo_items_v1_processed
        select scmi.*
        from staging.stg_credit_memo_items_v1 scmi
        inner join staging.ship_cm_processed scp on scmi.credit_id = scp.credit_id
        """,
    "40 - delete processed from stg_credit_memo_items_v1":
        """
        delete from staging.stg_credit_memo_items_v1
        where credit_id in (select credit_id from staging.ship_cm_processed)
        """,
    "41 - copy staging.stg_credit_memo_v1 processed":
        """
        insert into staging.stg_credit_memo_v1_processed
        select scm.*
        from staging.stg_credit_memo_v1 scm
        inner join staging.ship_cm_processed scp on scm.credit_id = scp.credit_id
        """,
    "42 - delete processed from stg_credit_memo_v1":
        """
        delete from staging.stg_credit_memo_v1
        where credit_id in (select credit_id from staging.ship_cm_processed)
        """
}

spUpdateGiftCardAdjustments = {
    "1 - Truncate staging.temp_gc_adjustments":
        """
        truncate staging.temp_gc_adjustments
        """,
    "2 - Insert Pre ODS pre-shipment credits to work table":
        """
        insert into staging.temp_gc_adjustments
        select sgca.message_uuid, sgca.card_id
        from staging.stg_gift_card_adjustments_v1 sgca
        """,
    "3 - Create Membership Redemption Transactions":
        """
        insert into ods.transactions (tran_type, tran_sub_type_id, tran_sub_type, tran_date,  customer_id, company_id, tran_amt, 
         created_at, gift_card_id, gc_reason_id, gc_reason_name, gc_group_name, gc_group_code, gc_sub_type, tran_gl_date, start_date, end_date)
        select 500, 94, 'Membership Redemption', sgca.updated_at::timestamp, sgca.customer_id, 1, sgca.card_delta * -1,  
        current_timestamp, sgca.card_id::int, sgca.reason_id, sgca.reason_label, sgca.group_label, sgca.group_code, 'membership', 
         convert_timezone('UTC', 'America/Los_Angeles',sgca.updated_at::timestamp)::date, subscription_start_date, subscription_end_date
        from staging.stg_gift_card_adjustments_v1 sgca
        inner join staging.temp_gc_adjustments tgc on sgca.message_uuid = tgc.message_uuid and sgca.card_id = tgc.card_id
        where  sgca."action"::int = 2 and sgca.subscription_start_date is not null
        """,
    "4 - Create new Gift Card Adjustment Records":
        """
        INSERT into ods.GIFT_CARD_ADJUSTMENTS
        SELECT gca.CARD_ID, oh.billing_state, cm.CREDIT_ID, cm.CREDIT_INCREMENT_ID AS cm_increment_id, cm.increment_id,
        gca.UPDATED_AT, CASE WHEN dateadd(DAY,20, gca.UPDATED_AT::date) < gca.expire_at::date THEN gca.EXPIRE_AT END AS expire_at, 
        gca."action", gca.ACTION_LABEL, op.increment_id AS redeeming_increment_id,
        op.customer_id AS redeeming_cust_id, gcf.INCREMENT_ID::varchar(50) AS purchasing_increment_id, 
        oh.CUSTOMER_ID AS customer_id, gct.ISSUE_TYPE, gca.GROUP_CODE, gca.GROUP_LABEL,
        gca.REASON_ID, gca.REASON_LABEL, gca.CARD_DELTA , gca.CARD_AMT AS card_amt,
        CASE WHEN cm.GIFT_CARD_ID is not null THEN 1 else gcf.CARD_TYPE END AS card_type
        FROM STAGING.STG_GIFT_CARD_ADJUSTMENTS_V1  gca
        inner join staging.temp_gc_adjustments tgc on gca.message_uuid = tgc.message_uuid and gca.card_id = tgc.card_id
        LEFT  JOIN ods.GIFT_CARD_TYPES gct ON COALESCE(gca.GROUP_CODE,'') = gct.GROUP_CODE
        LEFT OUTER JOIN STAGING.STG_GIFTCARD_FULFILLMENT_V1_PROCESSED gcf ON gca.CARD_ID = gcf.CARD_ID 
        LEFT OUTER JOIN (
                            SELECT GIFT_CARD_ID, min(gc.CREDIT_ID) AS credit_id, min(cm.CREDIT_INCREMENT_ID) AS credit_increment_id, 
                            min(gc.ORDER_ID) AS order_id, min(cm.INCREMENT_ID) AS increment_id 
                            FROM ods.CREDIT_MEMO_GIFT_CARDS gc
                            LEFT OUTER JOIN ods.CREDIT_MEMO cm ON gc.CREDIT_ID = cm.CREDIT_ID
                            GROUP BY GIFT_CARD_ID ) cm ON gca.CARD_ID = cm.gift_card_id
        LEFT OUTER JOIN ods.ORDER_HEADER oh ON COALESCE(gcf.INCREMENT_ID::varchar(50),cm.increment_id::varchar(50)) = oh.INCREMENT_ID::varchar(50)
        left OUTER JOIN (
                            SELECT op.GIFT_CARD_ID, op.ORDER_ID, oh.CREATED_AT, op.PAY_AMT, 
                            dateadd(SECOND,30,oh.CREATED_AT) AS max_created_at, oh.CUSTOMER_ID, oh.INCREMENT_ID 
                            FROM ods.ORDER_PAYMENTS op
                            INNER JOIN ods.ORDER_HEADER oh ON op.ORDER_ID = oh.ORDER_ID ) op ON gca.CARD_ID = op.gift_card_id AND gca.UPDATED_AT between op.created_at AND op.max_created_at AND round(gca.CARD_DELTA, 2) = round(op.pay_amt,2)
        ORDER BY gca.updated_at DESC
        """,
    "5 - Copy to staging.stg_gift_card_adjustments_v1_processed":
        """
        insert into staging.stg_gift_card_adjustments_v1_processed
        select sgca.*
        from staging.stg_gift_card_adjustments_v1 sgca
        inner join staging.temp_gc_adjustments tgc on sgca.message_uuid = tgc.message_uuid and sgca.card_id = tgc.card_id
        """,
    "6 - Delete processed recodes from staging":
        """
        delete from staging.stg_gift_card_adjustments_v1 del
        where exists (	select 1
            from staging.temp_gc_adjustments tgc
            where tgc.message_uuid = del.message_uuid and tgc.card_id = del.card_id)
        """
}

spCheckOutOfBalance = {
    "1 - check for order payment errors":
        """
        insert into ods.order_payment_errors
                select current_timestamp as created_at, *
        from (
                select oh.order_id, oh.increment_id, convert_timezone('UTC', 'America/Los_Angeles',oh.created_at)::date as sale_date, 
                 coalesce(od.total_product, 0) +  coalesce(oh.grocery_base_shipment, 0) + coalesce(oh.frozen_base_shipment, 0) + coalesce(oh.wine_base_shipment, 0) + 
                coalesce(oh.grocery_base_shipment_tax, 0) + coalesce(oh.frozen_base_shipment_tax, 0) + coalesce(oh.wine_base_shipment_tax, 0) + coalesce(oh.base_donations, 0) + 
                coalesce(oh.ppe_fee, 0) + coalesce(oh.ppe_fee_tax, 0) + coalesce(od.product_tax, 0) as expected_payment,  coalesce(op.pay_amt,0) + coalesce(gc.gift_card_amt,0) + coalesce(tc.tc_amt, 0) as total_paid,
                coalesce(op.pay_amt,0) as pay_amt, coalesce(gc.gift_card_amt,0) as gift_card_amt, coalesce(tc.tc_amt,0) as tc_amt,
                coalesce(oh.total_order, 0) as total_order, coalesce(od.total_product, 0) as total_product, coalesce(oh.grocery_base_shipment, 0) as grocery_base_shipment, 
                coalesce(oh.frozen_base_shipment, 0) as frozen_base_shipment, coalesce(oh.wine_base_shipment, 0) as wine_base_shipment, 
                coalesce(oh.grocery_base_shipment_tax, 0) as grocery_base_shipment_tax, coalesce(oh.frozen_base_shipment_tax, 0) as frozen_base_shipment_tax, 
                coalesce(oh.wine_base_shipment_tax, 0) as wine_base_shipment_tax, coalesce(oh.base_donations, 0) as base_donations , coalesce(oh.ppe_fee, 0) as ppe_fee, 
                coalesce(oh.ppe_fee_tax, 0) as ppe_fee_tax, coalesce(od.product_tax, 0) as product_tax, 'order' as order_type
                from (
                select od.order_id, sum(base_row_total) as total_product, sum(od.tax_amt) as product_tax
                from staging.stg_order_capture_items_v1 od
                group by od.order_id) od 
                inner join staging.stg_order_capture_v1 oh on od.order_id = oh.order_id
                left outer join staging.stg_order_capture_payments_v1 op on oh.order_id = op.order_id
                left outer join (
                                select order_id, sum(gift_card_amt) as gift_card_amt
                                from staging.stg_order_capture_gift_cards_v1 
                                group by order_id) gc on oh.order_id = gc.order_id
                left outer join (
                                select order_id, sum(tc_amt) as tc_amt
                                from staging.stg_order_capture_thrive_cash_v1 
                                where tc_amt > 0
                                group by order_id) tc on oh.order_id = tc.order_id) det 
        where expected_payment <> total_paid
        """,
    "2 - check for membership payment errors":
        """
        insert into ods.order_payment_errors
        select current_timestamp as created_at, mh.order_id, increment_id, convert_timezone('UTC', 'America/Los_Angeles',mh.updated_at)::date as sale_date, coalesce(total_order, 0) as expecxted_payment, 
        coalesce(mp.pay_amt, 0) as total_paid, coalesce(mp.pay_amt, 0) as pay_amt, 0 as gift_card_amt, 0 as tc_amt, 
        coalesce(mh.total_order, 0) as total_order, coalesce(mh.total_order, 0) - coalesce(mh.total_tax, 0) as total_product, 0 as grocery_base_shipment, 0 as frozen_base_shipment, 0 as wine_base_shipment, 
        0 as grocery_base_shipment_tax, 0 as frozen_base_shipment_tax, 0 as wine_base_shipment_tax, 0 as base_donations, 0 as ppe_fee, 0 as ppe_fee_tax, coalesce(mh.total_tax, 0) as product_tax, 
        'membership' as order_type
        from staging.stg_membership_capture_v1 mh
        left outer join staging.stg_membership_capture_payments_v1 mp on mh.order_id = mp.order_id
        where coalesce(total_order, 0) <> coalesce(mp.pay_amt, 0)
        """,
    "3 - Check for detail discounts greater then item level discounts":
        """
        insert into ods.order_discount_errors
        select order_id, increment_id, item_id,sale_date, base_discount_amt, disc_tpa_amt, disc_coupon_amt, disc_total_amt, 
        disc_total_amt  - base_discount_amt as var, 1 as error_code, current_timestamp as created_at, product_type, disc_tiered_pricing_amt
        from (
        select od.order_id, oh.increment_id, od.item_id, convert_timezone('UTC', 'America/Los_Angeles',oh.created_at)::date as sale_date, round(od.base_discount_amt * od.qty_ordered,2) as base_discount_amt, 
        odd.disc_tpa_amt, odd.disc_coupon_amt, odd.disc_total_amt, odd.disc_tiered_pricing_amt, case when coalesce(od.parent_item_id,'0') <> '0' then 'component' else od.product_type end as product_type
        from staging.stg_order_capture_items_v1 od
        inner join staging.stg_order_capture_v1 oh on od.order_id = oh.order_id
        left outer join staging.stg_order_capture_payments_v1 op on od.order_id = op.order_id
        left outer join(
                    select od.item_id, sum(case when odd.discount_type = 'tpa' then base_amt_off_applied else 0 end) as disc_tpa_amt, 
                    sum(case when odd.discount_type = 'coupon' then base_amt_off_applied else 0 end) as disc_coupon_amt, 
                    sum(case when odd.discount_type = 'tiered_pricing' then base_amt_off_applied else 0 end) as disc_tiered_pricing_amt,
                    sum(coalesce(amt_off_applied,0)) as disc_total_amt
                    from staging.stg_order_capture_items_v1 od
                    left outer join staging.stg_order_capture_detail_discounts_v1 odd on od.item_id = odd.item_id
                    group by od.item_id) odd on od.item_id = odd.item_id
                    where  od.is_gwp = false and op.payment_gateway <> 'thrive_zero_charge') disc 
        where (disc_total_amt  - base_discount_amt)::int > 0 and base_discount_amt >= 0
        """,
    "4 - Check for item level greater then detail discount":
        """
        insert into ods.order_discount_errors
        select order_id, increment_id, item_id,sale_date, base_discount_amt, disc_tpa_amt, disc_coupon_amt, disc_total_amt,  
         disc_total_amt  - base_discount_amt as var, 2 as error_code, current_timestamp as created_at, product_type, disc_tiered_pricing_amt
        from (
        select od.order_id, oh.increment_id, od.item_id, convert_timezone('UTC', 'America/Los_Angeles',oh.created_at)::date as sale_date, round(od.base_discount_amt * od.qty_ordered,2) as base_discount_amt, 
        odd.disc_tpa_amt, odd.disc_coupon_amt, odd.disc_total_amt, odd.disc_tiered_pricing_amt, case when coalesce(od.parent_item_id,'0') <> '0' then 'component' else od.product_type end as product_type
        ,base_price
        from staging.stg_order_capture_items_v1 od
        inner join staging.stg_order_capture_v1 oh on od.order_id = oh.order_id
        left outer join staging.stg_order_capture_payments_v1 op on od.order_id = op.order_id
        left outer join(
                    select od.item_id, sum(case when odd.discount_type = 'tpa' then base_amt_off_applied else 0 end) as disc_tpa_amt, 
                    sum(case when odd.discount_type = 'coupon' then base_amt_off_applied else 0 end) as disc_coupon_amt,
                    sum(case when odd.discount_type = 'tiered_pricing' then base_amt_off_applied else 0 end) as disc_tiered_pricing_amt,
                    sum(coalesce(amt_off_applied,0)) as disc_total_amt
                    from staging.stg_order_capture_items_v1 od
                    left outer join staging.stg_order_capture_detail_discounts_v1 odd on od.item_id = odd.item_id
                    group by od.item_id) odd on od.item_id = odd.item_id
                    where  od.is_gwp = false and op.payment_gateway <> 'thrive_zero_charge') disc 
        where (base_discount_amt - disc_total_amt)::int > 0 and base_discount_amt >= 0  AND base_price <> 0
        """,
    "5 - Check for negative item level discount":
        """
        insert into ods.order_discount_errors
        select order_id, increment_id, item_id,sale_date, base_discount_amt, disc_tpa_amt, disc_coupon_amt, disc_total_amt, 
         disc_total_amt  - base_discount_amt as var, 3 as error_code, current_timestamp as created_at, product_type, disc_tiered_pricing_amt
        from (
        select od.order_id, oh.increment_id, od.item_id, convert_timezone('UTC', 'America/Los_Angeles',oh.created_at)::date as sale_date, round(od.base_discount_amt * od.qty_ordered,2) as base_discount_amt, 
        odd.disc_tpa_amt, odd.disc_coupon_amt, odd.disc_total_amt, odd.disc_tiered_pricing_amt, case when coalesce(od.parent_item_id,'0') <> '0' then 'component' else od.product_type end as product_type
        from staging.stg_order_capture_items_v1 od
        inner join staging.stg_order_capture_v1 oh on od.order_id = oh.order_id
        left outer join staging.stg_order_capture_payments_v1 op on od.order_id = op.order_id
        left outer join(
                    select od.item_id, sum(case when odd.discount_type = 'tpa' then base_amt_off_applied else 0 end) as disc_tpa_amt, 
                    sum(case when odd.discount_type = 'coupon' then base_amt_off_applied else 0 end) as disc_coupon_amt, 
                    sum(case when odd.discount_type = 'tiered_pricing' then base_amt_off_applied else 0 end) as disc_tiered_pricing_amt,
                    sum(coalesce(amt_off_applied,0)) as disc_total_amt
                    from staging.stg_order_capture_items_v1 od
                    left outer join staging.stg_order_capture_detail_discounts_v1 odd on od.item_id = odd.item_id
                    group by od.item_id) odd on od.item_id = odd.item_id
                    where  od.is_gwp = false and op.payment_gateway <> 'thrive_zero_charge') disc 
        where base_discount_amt < 0
        """,
    "6 - Check for Credit Memo out of Balance":
        """
        insert into ods.credit_memo_errors
        select cm.credit_id, credit_increment_id, cm.order_id, increment_id,  convert_timezone('UTC', 'America/Los_Angeles',updated_at)::date as credit_date, 
        coalesce(base_total_credit, 0) as base_total_credit, coalesce(base_total_refund, 0) as base_total_refund, coalesce(base_total_gift_cards,0) as base_total_gift_cards,
        coalesce(base_total_thrive_cash, 0) as base_total_thrive_cash, coalesce(base_total_product, 0) as base_total_product, coalesce(base_total_discount, 0) as base_total_discount, 
        coalesce(base_adjustment, 0) as base_adjustment, coalesce(base_total_tax, 0) as base_total_tax, coalesce(base_donations, 0) as base_donations, coalesce(base_shipment, 0) as base_shipment,
        coalesce(base_shipment_tax, 0) as base_shipment_tax, coalesce(base_frozen_shipment, 0) as base_frozen_shipment, coalesce(base_frozen_shipment_tax, 0) as base_frozen_shipment_tax,
        coalesce(base_wine_shipment, 0) as base_wine_shipment, coalesce(base_wine_shipment_tax, 0) as base_wine_shipment_tax, coalesce(ppe_fee, 0) as ppe_fee,coalesce(ppe_fee_tax, 0) as ppe_fee_tax, 
        coalesce(gc.gc_amt,0) as gc_amt, coalesce(cmr.pay_amt,0) as pay_amt, coalesce(base_total_product, 0) + coalesce(base_total_discount, 0) + coalesce(base_adjustment, 0) + coalesce(base_total_tax, 0) + 
        coalesce(base_donations, 0) + coalesce(base_shipment, 0) + coalesce(base_frozen_shipment, 0) + coalesce(base_wine_shipment, 0) + coalesce(ppe_fee, 0)  as calc_refund, 
        coalesce(gc.gc_amt,0) + coalesce(cmr.pay_amt,0) + coalesce(total_thrive_cash,0) as calc_credit, current_timestamp as created_at
        from staging.stg_credit_memo_v1 cm
        left outer join staging.stg_credit_memo_refunds_v1 cmr on cm.credit_id = cmr.credit_id
        left outer join (
                            select credit_id, sum(gift_card_amt) as gc_amt
                            from staging.stg_credit_memo_gift_cards_v1 cmgc
                            group by credit_id) gc on cm.credit_id = gc.credit_id
        where coalesce(gc.gc_amt,0) + coalesce(cmr.pay_amt,0) + coalesce(total_thrive_cash,0) <> coalesce(base_total_product,0) + 
        coalesce(base_total_discount,0) + coalesce(base_adjustment,0) + coalesce(base_total_tax,0) + coalesce(base_donations,0) + coalesce(base_shipment,0) + coalesce(base_frozen_shipment,0) + coalesce(base_wine_shipment,0) + coalesce(ppe_fee, 0) 
        """,
    "7 - Update transacions missing item data":
    """
    UPDATE ods."TRANSACTIONS" 
    SET 
    TRANSACTIONS.GROUP_ID = upd.group_id, 
    TRANSACTIONS.GROUP_NAME  = upd.group_name, 
    TRANSACTIONS.CATEGORY_ID  = upd.item_category_id, 
    TRANSACTIONS.CATEGORY_NAME  = upd.item_category_name,
    TRANSACTIONS.SUB_CATEGORY_ID  = upd.subcategory_id, 
    TRANSACTIONS.SUB_CATEGORY_NAME  = upd.subcategory_name, 
    TRANSACTIONS.CLASS_ID  = upd.class_id, 
    TRANSACTIONS.CLASS_NAME  = upd.class_name, 
    TRANSACTIONS.SUBCLASS_ID  = upd.subclass_id, 
    TRANSACTIONS.SUBCLASS_NAME  = upd.subclass_name, 
    TRANSACTIONS.ITEM_ID  = upd.item_id, 
    TRANSACTIONS.TRAN_COST_AMT  = upd.tran_cost 
    FROM (
    SELECT trn.tran_id, ci.group_id, ci.group_name, ci.item_category_id,
    ci.item_category_name, ci.subcategory_id, ci.subcategory_name, 
    ci.class_id, ci.class_name, ci.subclass_id, 
    ci.subclass_name, ci.item_id, round(trn.TRAN_AMT  * ci.net_unit_cost, 2) AS tran_cost
    FROM ods.TRANSACTIONS trn
    inner JOIN ods.CURR_ITEMS ci ON trn.SKU = ci.ITEM_NAME AND 2 = ci.FC_ID  
    WHERE trn.TRAN_TYPE = 300 AND trn.GROUP_ID IS NULL AND trn.SKU IS NOT NULL) upd
    WHERE TRANSACTIONS.TRAN_ID  = upd.tran_id
    """
}