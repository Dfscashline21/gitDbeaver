# Created by : Amit Joshi
# Description : Contains SQL logic for ODS transaction processing
# Create Date : 08/31/2021
spGiftCardAdjustment = {
                        "1 - Move duplicate messages to error table":
                        """
                        insert into staging.ods_message_queue_error(id, created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid, order_id, credit_id, card_id, customer_id, error_time, error_message)
                        select *, current_timestamp, 'Duplicate'
                        from staging.ods_message_queue omq 
                        where id in (
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join staging.STG_GIFT_CARD_ADJUSTMENTS_V1 sgca on 
                                                                                        json_data:cards:card_id::int = sgca.CARD_ID AND 
                                                                                        json_data:cards:updated_at::timestamp = sgca.UPDATED_AT AND 
                                                                                        round(json_data:cards:card_delta::float, 2) = round(sgca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join staging.STG_GIFT_CARD_ADJUSTMENTS_V1_PROCESSED sgca on 
                                                                                        json_data:cards:card_id::int = sgca.CARD_ID AND 
                                                                                        json_data:cards:updated_at::timestamp = sgca.UPDATED_AT AND 
                                                                                        round(json_data:cards:card_delta::float, 2) = round(sgca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join ods.GIFT_CARD_ADJUSTMENTS gca on 
                                                                                json_data:cards:card_id::int = gca.CARD_ID AND 
                                                                                json_data:cards:updated_at::timestamp = gca.UPDATED_AT AND 
                                                                                round(json_data:cards:card_delta::float, 2) = round(gca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    select id
                                    from (
                                    select id,  
                                    row_number() over (partition by json_data:cards:card_id::int,
                                                                    json_data:cards:updated_at::timestamp,
                                                                    round(json_data:cards:card_delta::float, 2)
                                                                    order by json_data:cards:card_id::int,
                                                                    json_data:cards:updated_at::timestamp,
                                                                    round(json_data:cards:card_delta::float, 2), s3_updated_at) as rn
                                    from staging.ods_message_queue omq
                                    where omq.message_type = 'gca') a     
                                    where rn > 1)
                        """,
                        "2 - Delete duplicates from queue":
                        """
                        delete from staging.ods_message_queue omq 
                        where id in (
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join staging.STG_GIFT_CARD_ADJUSTMENTS_V1 sgca on 
                                                                                        json_data:cards:card_id::int = sgca.CARD_ID AND 
                                                                                        json_data:cards:updated_at::timestamp = sgca.UPDATED_AT AND 
                                                                                        round(json_data:cards:card_delta::float, 2) = round(sgca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join staging.STG_GIFT_CARD_ADJUSTMENTS_V1_PROCESSED sgca on 
                                                                                        json_data:cards:card_id::int = sgca.CARD_ID AND 
                                                                                        json_data:cards:updated_at::timestamp = sgca.UPDATED_AT AND 
                                                                                        round(json_data:cards:card_delta::float, 2) = round(sgca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    SELECT id
                                    FROM STAGING.ODS_MESSAGE_QUEUE omq
                                    inner join ods.GIFT_CARD_ADJUSTMENTS gca on 
                                                                                json_data:cards:card_id::int = gca.CARD_ID AND 
                                                                                json_data:cards:updated_at::timestamp = gca.UPDATED_AT AND 
                                                                                round(json_data:cards:card_delta::float, 2) = round(gca.CARD_DELTA, 2)
                                    WHERE omq.MESSAGE_TYPE = 'gca'
                                    union
                                    select id
                                    from (
                                    select id,  
                                    row_number() over (partition by json_data:cards:card_id::int,
                                                                    json_data:cards:updated_at::timestamp,
                                                                    round(json_data:cards:card_delta::float, 2)
                                                                    order by json_data:cards:card_id::int,
                                                                    json_data:cards:updated_at::timestamp,
                                                                    round(json_data:cards:card_delta::float, 2), s3_updated_at) as rn
                                    from staging.ods_message_queue omq
                                    where omq.message_type = 'gca') a     
                                    where rn > 1)
                        """,
                        "3 - Truncate GCA work table":
                        "truncate staging.gca_work_list",
                        "4 - Create temp list of gift card adjustments":
                        """
                        insert into staging.gca_work_list
                        select json_data:message_uuid::varchar(100) message_uuid, id
                        from  staging.ods_message_queue q 
                        left outer join staging.stg_gift_card_adjustments_v1 sgcav on json_data:message_uuid::text = sgcav.message_uuid 
                        left outer join staging.stg_gift_card_adjustments_v1_processed sgcavp on json_data:message_uuid::text = sgcavp.message_uuid
                        where ( sgcav.message_uuid is null AND sgcavp.message_uuid is null)
                        and q.message_type = 'gca' 
                        order by s3_updated_at 
                        """,
                      "5 - Insert data into staging.stg_gift_card_adjustments_v1 ":
                          """
                            insert into staging.stg_gift_card_adjustments_v1
                            select
                            json_data:message_type::varchar  as message_type ,
                            json_data:message_uuid::varchar  as message_uuid ,
                            json_data:customer_id::varchar  as customer_id ,
                            json_data:cards:updated_at::timestamp as updated_at,
                            json_data:sent_at::timestamp  as sent_at,
                            json_data:cards:card_id::int as card_id,
                            json_data:cards:card_amt::float as card_amt,
                            json_data:cards:card_delta::float as card_delta,
                            json_data:cards:action_label::varchar as action_label,
                            json_data:cards:reason_id::int as reason_id,
                            json_data:cards:reason_label::varchar as reason_label,
                            json_data:cards:action::varchar as action,
                            json_data:cards:group_code::varchar as group_code,
                            json_data:cards:group_label::varchar as group_label,
                            current_timestamp  as insert_utc_datetime,
                            current_timestamp  as update_utc_datetime,
                            (NULLIF(json_data:subscription_start_date,''))::timestamp as  subscription_start_date,
                            (NULLIF(json_data:subscription_end_date,''))::timestamp as  subscription_end_date,
                            (NULLIF(json_data:expire_at,''))::timestamp as  expire_at
                            from   staging.ods_message_queue omq 
                            where message_type = 'gca'
                            and json_data:message_uuid in (select message_uuid from staging.gca_work_list );
                                                  """ ,
                          "6 - Move data into processed queue table ":
                              """
                          INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                          SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data , message_uuid,order_id,credit_id,card_id,customer_id 
                          FROM staging.ods_message_queue
                          WHERE id IN (select id from staging.gca_work_list )
                          """,
                          "7 - delete from message queue table ":
                              """
                            DELETE  FROM staging.ods_message_queue
                            WHERE id IN (select id from staging.gca_work_list );
                            """
                          }

spOrderCancel = {
                        "1 - Move duplicate messages to error table":
                        """
                        insert into staging.ods_message_queue_error(id, created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid, order_id, credit_id, card_id, customer_id, error_time, error_message)
                        select *, current_timestamp, 'Duplicate'
                        from staging.ods_message_queue omq 
                        where id in (
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join staging.stg_order_cancel_v1_processed  soc on omq.json_data:order_id::int = soc.order_id
                                        where omq.message_type = 'ocn'
                                        union 
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join staging.stg_order_cancel_v1  soc on omq.json_data:order_id::int = soc.order_id
                                        where omq.message_type = 'ocn'
                                        union 
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join ods.order_header oh on omq.json_data:order_id::int = oh.order_id
                                        where omq.message_type = 'ocn' and oh.status = 'cancel'
                                        union
                                        select id
                                        from (
                                        select id, omq.json_data:order_id::int as order_id, s3_updated_at, row_number() over (partition by omq.json_data:order_id::int order by omq.json_data:order_id::int, s3_updated_at) as rn
                                        from staging.ods_message_queue omq
                                        where omq.message_type = 'ocn') a     
                                        where rn > 1)
                        """,
                        "2 - Delete duplicates from queue":
                        """
                        delete from staging.ods_message_queue omq 
                        where id in (
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join staging.stg_order_cancel_v1_processed  soc on omq.json_data:order_id::int = soc.order_id
                                        where omq.message_type = 'ocn'
                                        union 
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join staging.stg_order_cancel_v1  soc on omq.json_data:order_id::int = soc.order_id
                                        where omq.message_type = 'ocn'
                                        union 
                                        select id
                                        from staging.ods_message_queue omq
                                        inner join ods.order_header oh on omq.json_data:order_id::int = oh.order_id
                                        where omq.message_type = 'ocn' and oh.status = 'cancel'
                                        union
                                        select id
                                        from (
                                        select id, omq.json_data:order_id::int as order_id, s3_updated_at, row_number() over (partition by omq.json_data:order_id::int order by omq.json_data:order_id::int, s3_updated_at) as rn
                                        from staging.ods_message_queue omq
                                        where omq.message_type = 'ocn') a     
                                        where rn > 1)
				        """,
                        "3- Truncate Order Cancel work table ":
                        "truncate staging.ocn_work_list"
                        ,
                        "4 - Create temp list of order cancel":
                        """
                        insert into staging.ocn_work_list
                        select q.json_data:order_id::int order_id,id
                        from  staging.ods_message_queue q 
                        left outer join staging.stg_order_cancel_v1 oc on q.json_data:order_id::int = oc.order_id
                        left outer join staging.stg_order_cancel_v1_processed ocp on q.json_data:order_id::int = ocp.order_id  
                        where ( oc.order_id is null AND ocp.order_id is NULL)
                        and q.message_type = 'ocn' 
                        order by s3_updated_at
                        """,
                        "5 - Insert into stg_order_cancel_v1 ":
                            """
                            INSERT INTO staging.stg_order_cancel_v1 
                            select
                            json_data:total_discount::numeric(10, 4)  as total_discount ,
                            json_data:shipment::numeric(10, 4)  as shipment ,
                            json_data:donations ::numeric(10, 4) as donations ,
                            json_data:shipment_tax::numeric(10, 4)  as shipment_tax ,
                            json_data:total_payments::numeric(10, 4)  as total_payments ,
                            json_data:message_type::varchar  as message_type ,
                            json_data:message_uuid::varchar  as message_uuid ,
                            json_data:total_tax::numeric(10, 4)  as total_tax ,
                            json_data:total_order::numeric(10, 4)  as total_order ,
                            json_data:refunds::varchar  as refunds,
                            json_data:sent_at::timestamp  as sent_at ,
                            json_data:total_thrive_cash::numeric(10, 4)  as total_thrive_cash ,
                            json_data:updated_at ::timestamp as updated_at,
                            json_data:increment_id::varchar  as increment_id ,
                            json_data:thrive_cash_earned_details::varchar   as thrive_cash_earned_details ,
                            json_data:total_gift_cards::numeric(10, 4)  as total_gift_cards,
                            json_data:customer_id::int  as customer_id ,
                            json_data:order_id::int  as order_id ,
                            json_data:order_type::varchar  as order_type,
                            json_data:base_currency_code::varchar  as base_currency_code ,
                            json_data:order_currency_code::varchar  as base_currency_code,
                            current_timestamp as insert_utc_datetime,
                            current_timestamp as update_utc_datetime,
                            json_data:ms_initiated::boolean as ms_initiated,
                            json_data:is_fraud::boolean as is_fraud
                            from   staging.ods_message_queue omq
                            where message_type = 'ocn'
                            and json_data:order_id::int in (select order_id from staging.ocn_work_list  )
                            """,
                            "6 - Insert into stg_order_cancel_thrive_cash_earned_v1 ":"""
                            INSERT INTO staging.stg_order_cancel_thrive_cash_earned_v1 (order_id, item_id, rule_id, earned_amt, insert_utc_datetime, update_utc_datetime, thrive_id, message_uuid)
                            select json_data:order_id::int order_id,
                            tce.value:item_id::int as item_id ,
                            tce.value:rule_id::int as rule_id ,
                            tce.value:earned_amt::numeric(10,4) as earned_amt,
                            current_timestamp as insert_utc_datetime,
                            current_timestamp as update_utc_datetime,
                            tce.value:thrive_id::int as thrive_id,
                            json_data:message_uuid  as message_uuid
                            from     staging.ods_message_queue omq ,  
                            LATERAL flatten(INPUT => json_data:thrive_cash_earned_details) tce 
                            where message_type = 'ocn'
                            and json_data:order_id::int in (select order_id from staging.ocn_work_list  )
                            """,
                            "10 - Move data into processed queue table ": """
                            INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                            SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data , message_uuid,order_id,credit_id,card_id,customer_id 
                            FROM staging.ods_message_queue
                            WHERE id IN (select id from staging.ocn_work_list );
                            """,
                            "11 - delete from message queue table ": """
                            DELETE  FROM staging.ods_message_queue
                            WHERE id IN (select id from staging.ocn_work_list );
                            """
                        }

spMembershipAdjustment = {   "1- Truncate Membership Adjustment work table ":
                        "truncate staging.ma_work_list"
                        ,
                        "2 - Create temp list of Membership Adjustment ":
                        """
                        insert into staging.ma_work_list
                        select json_data:message_uuid::varchar, id
                        from  staging.ods_message_queue q 
                        left outer join staging.stg_membership_adjustment_v1 ma on json_data:message_uuid::varchar = ma.message_uuid 
                        left outer join staging.stg_membership_adjustment_v1_processed map on json_data:message_uuid::varchar = map.message_uuid
                        where ( ma.message_uuid is null AND map.message_uuid IS NULL)
                        and q.message_type = 'ma' 
                        order by s3_updated_at 
                        """,
                            "3 - Insert into stg_membership_adjustment_v1 ":"""
                            INSERT INTO staging.stg_membership_adjustment_v1 (sent_at, end_status, updated_at, credit_memo_id, message_type, message_uuid, customer_id,
                                                                      membership_type, start_status, insert_utc_datetime, update_utc_datetime)
                            SELECT
                            json_data:sent_at::timestamp  as sent_at ,
                            json_data:end_status::varchar  as end_status ,
                            json_data:updated_at::timestamp  as updated_at ,
                            json_data:credit_memo_id::int  as credit_memo_id ,
                            json_data:message_type::varchar  as message_type ,
                            json_data:message_uuid::varchar  as message_uuid ,
                            json_data:customer_id::varchar  as customer_id ,
                            json_data:membership_type::varchar  as membership_type ,
                            json_data:start_status::varchar  as start_status ,
                            current_timestamp as insert_utc_datetime,
                            current_timestamp as update_utc_datetime
                            FROM   staging.ods_message_queue omq
                            WHERE message_type = 'ma'
                            and message_uuid in (select message_uuid from staging.ma_work_list  )
                            """,
                             "10 - Move data into processed queue table ": """
                               INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                                SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id 
                                FROM staging.ods_message_queue
                                WHERE id IN (select id from staging.ma_work_list )
                                """,
                             "11 - delete from message queue table ": """
                                DELETE  FROM staging.ods_message_queue
                                WHERE id IN (select id from staging.ma_work_list );
                                """

                             }

spGiftCardFulfillment = { "1- Truncate Gift Card Fulfillment work table ":
                        "truncate staging.gf_work_list"
                        ,
                        "2 - Create temp list of Gift Card Fulfillment":
                        """
                        insert into staging.gf_work_list
                        select json_data:message_uuid::varchar, id
                        from  staging.ods_message_queue q 
                        left outer join staging.stg_giftcard_fulfillment_v1 ma on json_data:message_uuid::varchar = ma.message_uuid 
                        left outer join staging.stg_giftcard_fulfillment_v1_processed map on json_data:message_uuid::varchar = ma.message_uuid 
                        where ( ma.message_uuid is null AND map.message_uuid IS NULL)
                        and q.message_type = 'gcf' 
                        order by s3_updated_at 
                        """,

                        "3 - Insert into stg_giftcard_fulfillment_v1 ":"""
                        INSERT INTO staging.stg_giftcard_fulfillment_v1  
                       select
                        json_data:sent_at::timestamp  as sent_at ,
                        json_data:cards:group_code::varchar  as group_code ,
                        json_data:cards:item_id::int  as item_id ,
                        json_data:cards:product_id::int  as product_id ,
                        json_data:cards:group_label::varchar  as group_label ,
                        json_data:cards:sku::varchar as sku,
                        json_data:cards:card_type::int  as card_type ,
                        json_data:cards:card_amt::numeric(10,4)  as card_amt ,
                        json_data:cards:card_id::int  as card_id ,
                        json_data:cards:reason_id::varchar  as reason_id ,
                        json_data:updated_at::timestamp  as updated_at ,
                        json_data:increment_id::int  as increment_id ,
                        json_data:message_type::varchar  as message_type ,
                        json_data:message_uuid::varchar  as message_uuid ,
                        json_data:order_id::int  as order_id ,
                        current_timestamp as insert_utc_datetime,
                        current_timestamp as update_utc_datetime
                        from   staging.ods_message_queue omq
                        where message_type = 'gcf'
                        AND  json_data:message_uuid IN (select message_uuid from staging.gf_work_list ) ;
                        """,
                          "10 - Move data into processed queue table ": """
                       INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                       SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id 
                       FROM staging.ods_message_queue
                       WHERE id IN (select id from staging.gf_work_list )
                       """,
                       "11 - delete from message queue table ": """
                       DELETE  FROM staging.ods_message_queue
                       WHERE id IN (select id from staging.gf_work_list );
                       """
}

spOrderCapture = {  "1- Truncate Order Capture work table ":
                        "truncate staging.oc_work_list"
                        ,
                        "2 - Create temp list of Order Capture":
                        """
                        insert into staging.oc_work_list
                        select json_data:order_id::int order_id,id
                        from  staging.ods_message_queue q
                        left outer join staging.stg_order_capture_v1 oc on json_data:order_id::int = oc.order_id
                        left outer join staging.stg_order_capture_v1_processed ocp on json_data:order_id::int = ocp.order_id
                        where ( oc.order_id is null AND ocp.order_id is NULL)
                        and q.message_type = 'oc'
                        order by s3_updated_at
                        """,
                        "3 - Insert into stg_order_capture_v1 ":"""
                        INSERT INTO staging.stg_order_capture_v1
                        select
                         json_data:message_uuid::varchar  as 	  message_uuid
                        ,json_data:order_id::int  as 	 order_id
                        ,json_data:increment_id::varchar  as 	 increment_id
                        ,json_data:order_type::varchar  as 	 order_type
                        ,json_data:original_order_id::varchar   as 	 original_order_id
                        ,json_data:customer_id::int   as 	 customer_id
                        ,json_data:is_autoship::varchar  as 	 is_autoship
                        ,json_data:sechedule_id::int   as 	 sechedule_id
                        ,json_data:updated_at::timestamp  as 	 updated_at
                        ,json_data:sent_at::timestamp  as 	 sent_at
                        ,json_data:base_currency_code::varchar  as 	 base_currency_code
                        ,json_data:order_currency_code::varchar  as 	 order_currency_code
                        ,json_data:base_total_order::numeric(10,4)  as 	 base_total_order
                        ,json_data:base_total_product ::numeric(10,4)   as 	 base_total_product
                        ,json_data:base_total_discount::numeric(10,4)   as 	 base_total_discount
                        ,json_data:base_total_tax::numeric(10,4)   as 	 base_total_tax
                        ,json_data:base_donations::numeric(10,4)   as 	 base_donations
                        ,json_data:base_shipment ::numeric(10,4)  as 	 base_shipment
                        ,json_data:base_shipment_tax::numeric(10,4)   as 	 base_shipment_tax
                        ,json_data:base_total_payments ::numeric(10,4)  as 	 base_total_payments
                        ,json_data:base_total_gift_cards::numeric(10,4)   as 	 base_total_gift_cards
                        ,json_data:base_total_thrive_cash ::numeric(10,4)  as 	 base_total_thrive_cash
                        ,json_data:total_order::numeric(10,4)   as 	 total_order
                        ,json_data:total_product::numeric(10,4)   as 	 total_product
                        ,json_data:total_discount ::numeric(10,4)  as 	 total_discount
                        ,json_data:total_tax::numeric(10,4)   as 	 total_tax
                        ,json_data:donations::numeric(10,4)   as 	 donations
                        ,json_data:shipment::numeric(10,4)   as 	 shipment
                        ,json_data:shipment_tax::numeric(10,4)   as 	 shipment_tax
                        ,json_data:total_payments::numeric(10,4)   as 	 total_payments
                        ,json_data:total_gift_cards::numeric(10,4)   as 	 total_gift_cards
                        ,json_data:total_thrive_cash::numeric(10,4)   as 	 total_thrive_cash
                        ,json_data:billing_city::varchar  as 	 billing_city
                        ,json_data:billing_state::varchar  as 	 billing_state
                        ,json_data:billing_postal_code::varchar  as 	 billing_postal_code
                        ,json_data:billing_country::varchar  as 	 billing_country
                        ,json_data:shipping_city::varchar  as 	 shipping_city
                        ,json_data:shipping_state::varchar  as 	 shipping_state
                        ,json_data:shipping_postal_code::varchar  as 	 shipping_postal_code
                        ,json_data:shipping_country::varchar  as 	 shipping_country
                        ,json_data:message_state_id::smallint   as 	 message_state_id
                        ,json_data:message_state::varchar  as 	 message_state
                        ,current_timestamp as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        ,json_data:message_type::varchar  as 	 message_type
                        ,json_data:frozen_base_shipment ::numeric(10,4) as 	 frozen_base_shipment
                        ,json_data:frozen_base_shipment_tax::numeric(10,4)  as 	 frozen_base_shipment_tax
                        ,json_data:wine_base_shipment::numeric(10,4)  as 	 wine_base_shipment
                        ,json_data:wine_base_shipment_tax::numeric(10,4)  as 	 wine_base_shipment_tax
                        ,json_data:grocery_base_shipment::numeric(10,4)  as 	 grocery_base_shipment
                        ,json_data:grocery_base_shipment_tax::numeric(10,4)  as 	 grocery_base_shipment_tax
                        ,json_data:frozen_shipment::numeric(10,4)  as 	 frozen_shipment
                        ,json_data:grocery_shipment ::numeric(10,4) as 	 grocery_shipment
                        ,json_data:wine_shipment::numeric(10,4)  as 	 wine_shipment
                        ,json_data:frozen_shipment_tax::numeric(10,4)  as 	 frozen_shipment_tax
                        ,json_data:grocery_shipment_tax::numeric(10,4)  as 	 grocery_shipment_tax
                        ,json_data:wine_shipment_tax::numeric(10,4)  as 	 wine_shipment_tax
                        ,json_data:created_at ::timestamp  as 	 created_at
                        ,json_data:ppe_fee::numeric(10,4)  as 	 ppe_fee
                        ,json_data:ppe_fee_tax::numeric(10,4)  as 	 ppe_fee_tax
                        from   staging.ods_message_queue omq
                        where message_type = 'oc'
                        AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)
                        """,
                        "4 - Insert into stg_order_capture_items_v1 ": """
                        INSERT INTO staging.stg_order_capture_items_v1
                        select
                        (json_data:order_id)::int  as order_id
                        ,items.value:item_id int
                        ,items.value:parent_item_id::varchar
                        ,items.value:product_id::int
                        ,items.value:product_type::varchar
                        ,items.value:is_virtual::varchar
                        ,items.value:sku::varchar
                        ,items.value:is_autoship_subscribed::varchar
                        ,items.value:subscription_qty::int
                        ,items.value:qty_ordered::int
                        ,items.value:base_original_price::numeric(12,4)
                        ,items.value:base_price::numeric(12,4)
                        ,items.value:tax_percent::numeric(12,4)
                        ,items.value:base_tax_amt::numeric(12,4)
                        ,items.value:discount_percent::numeric(12,4)
                        ,items.value:base_discount_amt::numeric(12,4)
                        ,items.value:base_row_total::numeric(12,2)
                        ,items.value:base_row_total_incl_tax::numeric(12,2)
                        ,items.value:original_price::numeric(12,4)
                        ,items.value:price::numeric(12,4)
                        ,items.value:tax_amt::numeric(12,4)
                        ,items.value:discount_amt::numeric(12,4)
                        ,items.value:row_total::numeric(12,2)
                        ,items.value:row_total_incl_tax::numeric(12,2)
                        ,0 fulfillment_qty
                        ,0 short_qty
                        ,0 return_qty
                        ,current_timestamp as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        ,json_data:message_type::varchar  as message_type
                        ,json_data:message_uuid::varchar  as message_uuid
                        ,items.value:is_gwp::varchar
                        FROM staging.ods_message_queue omq , LATERAL flatten(json_data:items) items   
					    WHERE message_type = 'oc'
					    AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)
                        """,
                        "5 - Insert into stg_order_capture_detail_discounts_v1 ":"""
                        INSERT INTO staging.stg_order_capture_detail_discounts_v1
                        select
                        (json_data:order_id)::int  as order_id
                        ,itm.value
                        
                        :item_id::int
                        ,row_number() over (partition by json_data:order_id order by json_data:order_id, itm.value:item_id) as discount_id
                        ,dd.value:discount_type::varchar    
                        ,dd.value:rule_id::int
                        ,dd.value:rule_pct_off::numeric(12,2)
                        ,dd.value:rule_amt_off::numeric(12,2)
                        ,dd.value:pct_off_applied::numeric(12,4)
                        ,dd.value:start_price::numeric(12,2)
                        ,dd.value:base_discount_amt::numeric(12,4)
                        ,dd.value:discount_amt::numeric(12,4)
                        ,dd.value:units_applied::int
                        ,dd.value:base_amt_off_applied::numeric(12,4)
                        ,dd.value:amt_off_applied::numeric(12,4)
                        ,dd.value:base_tax_discount::numeric(12,4)
                        ,dd.value:tax_discount::numeric(12,4)
                        ,dd.value:base_duty_discount::numeric(12,4)
                        ,dd.value:duty_discount::numeric(12,4)
                        ,current_timestamp as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        ,json_data:message_uuid::varchar  as message_uuid
                        ,dd.value:rule_name::varchar
                        ,dd.value:rule_group::varchar
                        ,dd.value:sponsored_by::varchar
                        ,dd.value:sponsored_percentage::float 
                        FROM staging.ods_message_queue omq , LATERAL flatten(input => json_data:items) itm,
                        LATERAL flatten(INPUT => itm.value:discount_detail) dd
                        where message_type = 'oc'
                        AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)
                        """,
                        "6 - Insert into stg_order_capture_payments_v1 ":"""
                        INSERT INTO staging.stg_order_capture_payments_v1
                        select
                        json_data:order_id::int  as order_id
                        ,pay.value:id::varchar as payment_id
                        ,pay.value:payment_gateway::varchar
                        ,pay.value:cc_type::varchar
                        ,pay.value:base_pay_amt::numeric(12,2)
                        ,pay.value:pay_amt::numeric(12,2)
                        ,pay.value:gateway_tran_id::varchar
                        ,pay.value:cc_tran_id::varchar
                        ,pay.value:cc_last_four::varchar
                        ,current_timestamp as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        ,json_data:message_uuid::varchar  as message_uuid
                        FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:payments:payment_detail) pay 
					    WHERE message_type = 'oc'
					    AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)
                        """,
                        "7 - Insert into stg_order_capture_gift_cards_v1 ":"""
                        INSERT INTO staging.stg_order_capture_gift_cards_v1
                        select
                        (json_data:order_id)::int  as order_id
                        ,gc.value:gift_card_id::varchar
                        ,gc.value:gift_card_amt::numeric(12,2)
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp  as update_utc_datetime
                        ,json_data:message_uuid::varchar  as message_uuid
                        FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:payments:gift_card_details) gc 
					    WHERE message_type = 'oc'
					    AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)
                        """,
                        "8 - Insert into stg_order_capture_thrive_cash_v1 ":"""
                         INSERT INTO staging.stg_order_capture_thrive_cash_v1
                        select
                        (json_data:order_id)::int  as order_id
                        ,floor(random()  + 1)::int as tc_id
                        ,tc.value:applied_amt::numeric(12,2) tc_amt
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        ,json_data:message_uuid::varchar  as message_uuid
                        FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:payments:thrive_cash_applied_details) tc 
					    WHERE message_type = 'oc'
					    AND  (json_data:order_id)::int  IN ( select order_id from staging.oc_work_list)""",
                        "10 - Move data into processed queue table ": """
                        INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                        SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id
                        FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.oc_work_list )
                        """,
                        "11 - delete from message queue table ": """
                        DELETE  FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.oc_work_list );
                        """
                        }

spMembershipCapture = {  "1- Truncate Membership Capture work table ":
                        "truncate staging.mc_work_list"
                        ,
                        "2 - Create temp list of Membership Capture":
                        """
                        insert into staging.mc_work_list
                        select json_data:order_id::int order_id,id
                        from  staging.ods_message_queue q
                        left outer join staging.stg_membership_capture_v1 oc on json_data:order_id::int = oc.order_id
                        left outer join staging.stg_membership_capture_v1_processed ocp on json_data:order_id::int = ocp.order_id
                        where ( oc.order_id is null AND ocp.order_id is NULL)
                        and q.message_type = 'mc'
                        order by s3_updated_at
                        """,
                         "3 - Insert into stg_membership_capture_v1 ": """
                         INSERT INTO staging.stg_membership_capture_v1
                          SELECT
                          json_data:end_date::timestamp  as 	end_date
                        , json_data:end_status::varchar  as 	end_status
                        , json_data:donations::numeric(10,4)  as 	donations
                        , json_data:base_total_gift_cards::numeric(10,4)  as 	base_total_gift_cards
                        , json_data:total_payments::numeric(10,4)  as 	total_payments
                        , json_data:base_donations ::numeric(10,4) as 	base_donations
                        , json_data:base_total_thrive_cash::numeric(10,4)  as 	base_total_thrive_cash
                        , json_data:total_order::numeric(10,4)  as 	total_order
                        , json_data:base_total_membership::numeric(10,4)  as 	base_total_membership
                        , json_data:updated_at::timestamp  as 	updated_at
						, json_data:increment_id::int  as 	increment_id
                        , json_data:base_currency_code::varchar  as 	base_currency_code
                        , json_data:original_order_id::varchar  as 	original_order_id
                        , json_data:membership_type::varchar  as 	membership_type
                        , json_data:start_date::timestamp  as 	start_date
                        , json_data:message_type::varchar  as 	message_type
                        , json_data:message_uuid::varchar  as 	message_uuid
                        , json_data:total_tax::numeric(10,4)  as 	total_tax
                        , json_data:start_status::varchar  as 	start_status
                        , json_data:sent_at::timestamp  as 	sent_at
                        , json_data:base_total_order::numeric(10,4)  as 	base_total_order
                        , json_data:base_total_tax::numeric(10,4)  as 	base_total_tax
                        , json_data:total_thrive_cash::numeric(10,4)  as 	total_thrive_cash
                        , json_data:total_gift_cards::numeric(10,4)  as 	total_gift_cards
                        , json_data:customer_id::int  as 	customer_id
                        , (json_data:order_id)::int  as 	order_id
                        , json_data:order_currency_code::varchar  as 	order_currency_code
                        , json_data:base_total_payments::numeric(10,4)  as 	base_total_payments
                        , json_data:total_membership::numeric(10,4)  as 	total_membership
                        , current_timestamp, current_timestamp 
                        , json_data:billing_state::varchar(20)  as 	billing_state
                        from   staging.ods_message_queue omq
                        where message_type = 'mc'
                        AND  (json_data:order_id)::int  IN ( select order_id from staging.mc_work_list) 
                         """,
                         "4 - Insert into stg_membership_capture_payments_v1 ": """
                            INSERT INTO staging.stg_membership_capture_payments_v1
                            select
                            (json_data:order_id)::int  as order_id
                            ,pay.value:id::varchar as payment_id
                            ,pay.value:payment_gateway::varchar
                            ,pay.value:cc_type::varchar
                            ,pay.value:base_pay_amt::numeric(12,4)
                            ,pay.value:pay_amt::numeric(12,4)
                            ,pay.value:gateway_tran_id::varchar
                            ,pay.value:cc_tran_id::varchar
                            ,pay.value:cc_last_four::varchar
                            ,current_timestamp as insert_utc_datetime
                            ,current_timestamp as update_utc_datetime
                            ,json_data:message_uuid::varchar  as message_uuid
                            FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:payments:payment_detail) pay
						    WHERE message_type = 'mc'
						    AND  (json_data:order_id)::int  IN ( select order_id from staging.mc_work_list)
                            """,
                            "7 - Insert into stg_membership_capture_gift_cards_v1 ": """
                            INSERT INTO staging.stg_membership_capture_gift_cards_v1
                            select
                            (json_data:order_id)::int  as order_id
                            ,gc.value:gift_card_id::varchar
                            ,gc.value:gift_card_amt::numeric(12,4)
                            ,current_timestamp as insert_utc_datetime
                            ,current_timestamp as update_utc_datetime
                            ,json_data:message_uuid::varchar  as message_uuid
                            FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:payments:gift_card_details) gc
						    WHERE message_type = 'mc'
						    AND  (json_data:order_id)::int  IN ( select order_id from staging.mc_work_list)
                            """,
                          "10 - Move data into processed queue table ": """
                            INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                            SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data , message_uuid,order_id,credit_id,card_id,customer_id
                            FROM staging.ods_message_queue
                            WHERE id IN (select id from staging.mc_work_list )
                            """,
                           "11 - delete from message queue table ": """
                            DELETE  FROM staging.ods_message_queue
                            WHERE id IN (select id from staging.mc_work_list );
                            """
}

spOrderAssignment = { "1- Truncate Order Assignment work table ":
                        "truncate staging.oa_work_list"
                        ,
                        "2 - Create temp list of order assignment":
                        """
                        insert into staging.oa_work_list
                        select json_data:order_id::int order_id,id
                        from  staging.ods_message_queue q
                        left outer join staging.stg_order_assignment_v1 oc on json_data:order_id::int = oc.order_id
                        left outer join staging.stg_order_assignment_v1_processed ocp on json_data:order_id::int = ocp.order_id
                        where ( oc.order_id is null AND ocp.order_id is NULL)
                        and q.message_type = 'oa'
                        order by s3_updated_at
                        """,
                    "3 - Insert into staging.stg_order_assignment_v1 ": """
                    INSERT INTO staging.stg_order_assignment_v1
                    SELECT
                           json_data:sent_at::varchar  as sent_at ,
                           json_data:updated_at::timestamp  as updated_at ,
                           json_data:increment_id::int  as increment_id ,
                           json_data:message_type::varchar  as message_type ,
                           json_data:message_uuid::varchar  as message_uuid ,
                           json_data:customer_id::int  as customer_id ,
                           json_data:order_id::int  as order_id ,
                           json_data:order_type::varchar  as order_type ,
                           itm.value:item_id::int as item_id ,
                           itm.value:qty_ordered::int as qty_ordered,
                           fa.value:units::int as qty_assigned,
                           (itm.value:qty_ordered - fa.value:units)::int as qty_short,
                           current_timestamp as insert_utc_datetime,
                           current_timestamp as update_utc_datetime,
                           json_data:original_order_id::int  as original_order_id ,
                           fa.value:fc::int as fc
                    FROM staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:items) itm,
				    LATERAL flatten (INPUT => itm.value:fc_assignments) fa
				    WHERE message_type = 'oa'
				    AND  (json_data:order_id)::int  IN ( select order_id from staging.oa_work_list)
                    """,
                      "10 - Move data into processed queue table ": """
                        INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                        SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id
                        FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.oa_work_list )
                        """,
                      "11 - delete from message queue table ": """
                        DELETE  FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.oa_work_list );
                        """
                      }

spOrderFulfillment = {
                        "0 - Move HJ fulfillments that are not last to unprocessed":
                        """
                        INSERT INTO staging.ODS_MESSAGE_QUEUE_UNPROCESSED
                        SELECT ID, CREATED_AT, S3_UPDATED_AT, S3_FILENAME, S3_LINE_NUM, MESSAGE_TYPE, JSON_DATA, 
                        MESSAGE_UUID, ORDER_ID, CREDIT_ID, CARD_ID, CUSTOMER_ID
                        FROM (
                            SELECT omq.*, 
                            json_data:is_last_hj_shipment::boolean  as is_last_hj_shipment,
                            replace(f.value,'"','') AS fc
                            FROM   staging.ODS_MESSAGE_QUEUE  omq,  LATERAL flatten(INPUT => json_data:items) items,
                            LATERAL flatten(INPUT => items.value:fc_detail:fc) f
                            WHERE message_type = 'of' ) dtl
                        INNER JOIN ods.NS_FC_XREF nfx ON dtl.fc = nfx.FC_ID 
                        WHERE nfx.IS_HJ = TRUE  AND dtl.is_last_hj_shipment = FALSE  
                        """,
                        "0.5 - Delete HJ Fulfillments that are not last from queue":
                        """
                        DELETE FROM staging.ODS_MESSAGE_QUEUE
                        WHERE ID IN (	SELECT ID
                                        FROM (
                                            SELECT omq.*, 
                                            json_data:is_last_hj_shipment::boolean  as is_last_hj_shipment,
                                            replace(f.value,'"','') AS fc
                                            FROM   staging.ODS_MESSAGE_QUEUE  omq,  LATERAL flatten(INPUT => json_data:items) items,
                                            LATERAL flatten(INPUT => items.value:fc_detail:fc) f
                                            WHERE message_type = 'of' ) dtl
                                        INNER JOIN ods.NS_FC_XREF nfx ON dtl.fc = nfx.FC_ID 
                                        WHERE nfx.IS_HJ = TRUE  AND dtl.is_last_hj_shipment = FALSE)
                        """,
                        "1- Truncate Order Fulfillment work table ":
                        "truncate staging.of_work_list"
                        ,
                        "2 - Create temp list of  Order Fulfillment":
                        """
                        insert into staging.of_work_list
                        select json_data:order_id::int order_id,id
                        from  staging.ods_message_queue q
                        left outer join staging.stg_order_fulfillment_v1 oc on json_data:message_uuid = oc.message_uuid
                        left outer join staging.stg_order_fulfillment_v1_processed ocp on json_data:message_uuid = ocp.message_uuid
                        where ( oc.order_id is null AND ocp.order_id is NULL)
                        and q.message_type = 'of'
                        order by s3_updated_at
                        """,
                      "3 - Insert into staging.stg_order_fulfillment_v1 " : """
                      INSERT INTO  staging.stg_order_fulfillment_v1
                        SELECT
                               json_data:sent_at::timestamp  as sent_at,
                               json_data:updated_at::timestamp   as updated_at,
                               case when json_data:credit_memo_id not in ('0','[]') then json_data:credit_memo_id else null end  as credit_memo_id,
                               json_data:increment_id::int  as increment_id,
                               json_data:message_type::varchar  as message_type,
                               json_data:message_uuid::varchar  as message_uuid,
                               json_data:customer_id::int  as customer_id,
                               json_data:order_id::int  as order_id,
                               json_data:order_type::varchar  as order_type,
                               current_timestamp as insert_utc_datetime,
                               current_timestamp as update_utc_datetime,
                               json_data:is_last_shipment::boolean  as is_last_shipment
                        FROM   staging.ods_message_queue omq
                        WHERE message_type = 'of'
                        AND    id IN (select id from staging.of_work_list )
                      """,
                      "4 - Insert into staging.stg_order_fulfillment_detail_v1 ": """
                      INSERT INTO   staging.stg_order_fulfillment_detail_v1
                        select det.increment_id, det.customer_id, det.order_id, det.is_virtual, det.units_shorted, det.units_shipped, det.shipment_id,
                        det.fc, det.tracking_id, det.product_type, det.item_id, det.parent_item_id, det.product_id, det.sku, det.insert_utc_datetime, det.update_utc_datetime, det.message_uuid
                        from (
                        SELECT
                        json_data:increment_id::int  as increment_id,
                        json_data:customer_id::int  as customer_id,
                        json_data:order_id::int  as order_id,
                        items.value:is_virtual AS is_virtual,
                        items.value:fc_detail:units_shorted::int as units_shorted,
                        items.value:fc_detail:units_shipped::int as units_shipped,
                        items.value:fc_detail:shipment_id  as shipment_id,
                        items.value:fc_detail:fc as fc,
                        items.value:fc_detail:tracking_id as tracking_id,
                        items.value:product_type AS product_type ,
                        items.value:item_id::int as item_id,
                        items.value:parent_item_id::int as parent_item_id ,
                        items.value:product_id::int as product_id,
                        items.value:sku AS sku,
                        current_timestamp as insert_utc_datetime,
                        current_timestamp as update_utc_datetime,
                        json_data:message_uuid  as message_uuid,
                        row_number() over (partition by items.value:item_id::int order BY items.value:item_id::int ) as rn
                        from   staging.ods_message_queue omq , LATERAL flatten(INPUT => json_data:items) items
                        where message_type = 'of' AND   id IN (select id from staging.of_work_list )) det
                        left outer join staging.stg_order_fulfillment_detail_v1 sofd on det.item_id = sofd.item_id
                        where rn = 1 and sofd.item_id is null
                      """,
                      "10 - Move data into processed queue table ": """
                       INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                        SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id 
                        FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.of_work_list )
                        """,
                      "11 - delete from message queue table ": """
                        DELETE  FROM staging.ods_message_queue
                        WHERE id IN (select id from staging.of_work_list );"""
}
spCreditMemo = {        "1- Truncate Credit memo work table ":
                        "truncate staging.cm_work_list"
                        ,
                        "2 - Create temp list of Credit memo":
                        """
                         insert into staging.cm_work_list
                        select json_data:credit_id::int credit_id,id
                        from  staging.ods_message_queue q
                        left outer join staging.stg_credit_memo_v1 oc on json_data:credit_id::int = oc.credit_id
                        left outer join staging.stg_credit_memo_v1_processed ocp on json_data:credit_id::int = ocp.credit_id
                        where ( oc.credit_id is null AND ocp.credit_id is NULL)
                        and q.message_type = 'cm'
                        order by s3_updated_at
                        """,
                        "3 - Insert stg_credit_memo_v1": """ 
                        INSERT INTO staging.stg_credit_memo_v1
                        SELECT
                          json_data:total_discount::numeric(10,4)  as 	total_discount
                        , json_data:base_shipment::numeric(10,4)  as 	base_shipment
                        , json_data:donations::numeric(10,4) as 	donations
                        , json_data:credit_increment_id:: int  as 	credit_increment_id
                        , json_data:base_total_gift_cards::numeric(10,4)  as 	base_total_gift_cards
                        , json_data:shipment_tax::numeric(10,4)  as 	shipment_tax
                        , json_data:total_refund::numeric(10,4)  as 	total_refund
                        , json_data:base_shipment_tax::numeric(10,4)  as 	base_shipment_tax
                        , json_data:base_donations ::numeric(10,4) as 	base_donations
                        , json_data:base_total_thrive_cash::numeric(10,4)  as 	base_total_thrive_cash
                        , json_data:base_total_refund::numeric(10,4)  as 	base_total_refund
                        , json_data:updated_at::timestamp  as 	updated_at
                        , json_data:credit_id::int   as 	credit_id
                        , json_data:increment_id::varchar  as 	increment_id
                        , json_data:order_type::varchar  as 	order_type
                        , json_data:base_currency_code::varchar  as 	base_currency_code
                        , json_data:original_order_id::varchar  as 	original_order_id
                        , json_data:total_product::numeric(10,4)  as 	total_product
                        , json_data:shipment::numeric(10,4)  as 	shipment
                        , json_data:total_credit ::numeric(10,4) as 	total_credit
                        , json_data:message_type::varchar  as 	message_type
                        , json_data:message_uuid::varchar  as 	message_uuid
                        , json_data:base_adjustment::numeric(10,4)  as 	base_adjustment
                        , json_data:total_tax::numeric(10,4)  as 	total_tax
                        , json_data:base_total_credit::numeric(10,4)  as 	base_total_credit
                        , json_data:base_total_discount::numeric(10,4)  as 	base_total_discount
                        , json_data:sent_at::timestamp  as 	sent_at
                        , json_data:base_total_tax::numeric(10,4)  as 	base_total_tax
                        , json_data:total_thrive_cash::numeric(10,4)  as 	total_thrive_cash
                        , json_data:base_total_product::numeric(10,4)  as 	base_total_product
                        , json_data:adjustment_amount::numeric(10,4)  as 	adjustment_amount
                        , json_data:total_gift_cards::numeric(10,4)  as 	total_gift_cards
                        , json_data:customer_id::int  as 	customer_id
                        , json_data:order_id::int  as 	order_id
                        , json_data:order_currency_code::varchar  as 	order_currency_code
                        , json_data:order_has_shipped::varchar  as 	order_has_shipped
                        ,current_timestamp as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        , json_data:ppe_fee::numeric(10,4)  as 	ppe_fee
                        , json_data:ppe_fee_tax::numeric(10,4)  as 	ppe_fee_tax
                        , json_data:frozen_shipment::numeric(10,4)  as 	frozen_shipment
                        , json_data:frozen_base_shipment::numeric(10,4)  as 	base_frozen_shipment
                        , json_data:frozen_shipment_tax::numeric(10,4)  as 	frozen_shipment_tax
                        , json_data:frozen_base_shipment_tax::numeric(10,4)  as 	base_frozen_shipment_tax
                        , json_data:wine_shipment::numeric(10,4)  as 	wine_shipment
                        , json_data:wine_base_shipment::numeric(10,4)  as 	base_wine_shipment
                        , json_data:wine_shipment_tax::numeric(10,4)  as 	wine_shipment_tax
                        , json_data:wine_base_shipment_tax::numeric(10,4)  as 	base_wine_shipment_tax
                        , json_data:billing_state::varchar(20)  as 	billing_state
                        FROM     staging.ods_message_queue omq
                        WHERE message_type = 'cm'
                        AND  json_data:credit_id::int  IN ( select credit_id from staging.cm_work_list)
                        """,
                        "4 - Insert stg_credit_memo_items_v1": """
                        INSERT INTO staging.stg_credit_memo_items_v1
                        SELECT
                          json_data:credit_id::int  as 	credit_id
                        , (items.value:credit_item_id)::int  as 	credit_item_id
                        , json_data:order_id::int  as 	order_id
                        , (items.value:order_item_id)::int  as 	order_item_id
                        , (items.value:is_virtual)::varchar  as 	is_virtual
                        , (items.value:return_reason)::varchar  as 	return_reason
                        , (items.value:sku)::varchar as 	sku
                        , (items.value:qty_refunded)::int  as 	qty_refunded
                        , (items.value:base_price)::numeric(10,4)  as 	base_price
                        , (items.value:tax_percent)::numeric(10,4)  as 	tax_percent
                        , (items.value:base_tax_amt)::numeric(10,4)  as 	base_tax_amt
                        , (items.value:discount_percent)::numeric(10,4)  as 	discount_percent
                        , (items.value:base_discount_amt)::numeric(10,4)  as 	base_discount_amt
                        , (items.value:base_row_total)::numeric(10,4)  as 	base_row_total
                        , (items.value:row_total_incl_tax)::numeric(10,4)  as 	row_total_incl_tax
                        , (items.value:price)::numeric(10,4)  as 	price
                        , (items.value:tax_amt)::numeric(10,4)  as 	tax_amt
                        , (items.value:discount_amt)::numeric(10,4)  as 	discount_amt
                        , (items.value:row_total)::numeric(10,4)  as 	row_total
                        , (items.value:base_row_total_incl_tax)::numeric(10,4)  as 	base_row_total_incl_tax
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp as update_utc_datetime
                        , json_data:message_uuid::varchar  as 	message_uuid
                        FROM     staging.ods_message_queue omq,  LATERAL flatten(INPUT => omq.json_data:items) items
                        WHERE message_type = 'cm'
                        AND  json_data:credit_id::int  IN ( select credit_id from staging.cm_work_list)
                        """,
                        " 5. Insert into staging.stg_credit_memo_refunds_v1 " : """
                        INSERT INTO staging.stg_credit_memo_refunds_v1
                        select
                        json_data:order_id::int  as order_id
                        ,json_data:refunds:payment_detail:id::varchar as payment_id
                        ,json_data:refunds:payment_detail:payment_gateway::varchar as payment_gateway
                        ,json_data:refunds:payment_detail:cc_type::varchar as cc_type
                        ,json_data:refunds:payment_detail:base_pay_amt::numeric(10,4) as base_pay_amt
                        ,json_data:refunds:payment_detail:pay_amt::numeric(10,4) as pay_amt
                        ,json_data:refunds:payment_detail:gateway_tran_id::varchar as gateway_tran_id
                        ,json_data:refunds:payment_detail:cc_tran_id::varchar as cc_tran_id
                        ,json_data:refunds:payment_detail:cc_last_four::varchar as cc_last_four
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp  as update_utc_datetime
                        ,json_data:credit_id::int  as credit_id
                        ,json_data:message_uuid::varchar  as message_uuid
                        from   staging.ods_message_queue omq
                        where message_type = 'cm' 
                        AND json_data:refunds:payment_detail:id is not null
                        AND  json_data:credit_id::int  IN ( select credit_id from staging.cm_work_list)
                        """,
                        " 6. Insert into staging.stg_order_capture_gift_cards_v1 ": """ 
                        INSERT INTO staging.stg_credit_memo_gift_cards_v1
                        SELECT
                         json_data:order_id::int  as order_id
                        ,gc.value:gift_card_id
                        ,gc.value:gift_card_amt::numeric(10,4) as gift_card_amt
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp  as update_utc_datetime
                        ,json_data:credit_id::int  as credit_id
                        ,json_data:message_uuid  as message_uuid
                        from   staging.ods_message_queue omq,  LATERAL flatten(INPUT => json_data:refunds:gift_card_details) gc
                        where message_type = 'cm'
                        AND  json_data:credit_id::int  IN ( select credit_id from staging.cm_work_list)
                        """,
                        " 7. Insert into staging.stg_order_capture_thrive_cash_v1 ": """ 
                         INSERT INTO staging.stg_credit_memo_thrive_cash_v1
                        select
                        json_data:order_id::int  as order_id
                        ,floor(random() + 1)::int as tc_id
                        ,json_data:refunds:thrive_cash_applied_details:cash_give::numeric(10,4) as tc_amt
                        ,current_timestamp  as insert_utc_datetime
                        ,current_timestamp  as update_utc_datetime
                        ,json_data:credit_id::int  as credit_id
                        ,json_data:message_uuid  as message_uuid
                        from   staging.ods_message_queue omq
                        where message_type = 'cm' AND COALESCE(json_data:refunds:thrive_cash_applied_details:cash_give,0)::numeric(10,4) <> 0
                        AND  json_data:credit_id::int  IN ( select credit_id from staging.cm_work_list)
                        """,
                        "10 - Move data into processed queue table ": """
                          INSERT INTO staging.ods_message_queue_processed (created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id)
                          SELECT  created_at, s3_updated_at, s3_filename, s3_line_num, message_type, json_data, message_uuid,order_id,credit_id,card_id,customer_id 
                          FROM staging.ods_message_queue
                          WHERE id IN (select id from staging.cm_work_list )
                          """,
                                                "11 - delete from message queue table ": """
                          DELETE  FROM staging.ods_message_queue
                          WHERE id IN (select id from staging.cm_work_list );"""
     }