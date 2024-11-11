def get_campaign_period_transactions(data_scientist, campaign_id): 
    print(f"Processing campaign period transactions: {campaign_id}")
    check = client.query(
        f""" 
        DECLARE media_start_date_global_var DATE; 
        DECLARE media_end_date_global_var DATE; 
        SET media_start_date_global_var = (SELECT DISTINCT media_start_date FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info WHERE booking_and_asset_number = '{campaign_id}');
        SET media_end_date_global_var = (SELECT DISTINCT media_end_date FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info WHERE booking_and_asset_number = '{campaign_id}');

        CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_transactions AS 
            SELECT 
                trading.booking_and_asset_number AS campaign_id,
                trading.media_start_date,
                trading.media_end_date,
                ass_campaign_period.SiteNumber AS Site, 
                CASE WHEN test_stores.test_store IS NOT NULL THEN "Test" ELSE "Control" END AS test_or_control, 
                --ass_campaign_period.Article,
                ass_campaign_period.BusinessDate,
                ass_campaign_period.CRN,
                ass_campaign_period.BasketKey,
                SUM(ass_campaign_period.TotalAmountIncludingGST) AS sales_amount, 
                SUM(ass_campaign_period.RetailQuantity) AS total_units

            FROM `gcp-wow-wiq-ca-prod.wiqIN_DataAssets.CustomerBaseTransaction_v` ass_campaign_period
            LEFT JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info trading
                ON ass_campaign_period.BusinessDate >= trading.media_start_date 
                AND ass_campaign_period.BusinessDate <= trading.media_end_date 
            INNER JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.unique_skus_2 skus 
                ON TRIM(skus.sku) = ass_campaign_period.Article 
            LEFT JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.test_stores_2 test_stores 
                ON CAST(test_stores.test_store AS INT64) = CAST(ass_campaign_period.SiteNumber AS INT64)
                
            WHERE trading.booking_and_asset_number = '{campaign_id}' 
            AND LOWER(ass_campaign_period.Channel) = "in store"
            AND ass_campaign_period.BusinessDate >= media_start_date_global_var
            AND ass_campaign_period.BusinessDate <= media_end_date_global_var
            AND ass_campaign_period.SalesOrganisation = '1005'
            AND skus.sku IS NOT NULL
            AND skus.sku <> ""
            GROUP BY ALL
        ;

        CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_units_and_baskets AS 
                SELECT 
                    trading.booking_and_asset_number AS campaign_id,
                    trading.media_start_date,
                    trading.media_end_date,
                    CASE WHEN test_stores.test_store IS NOT NULL THEN "Test" ELSE "Control" END AS test_or_control,
                    ass_campaign_period.SiteNumber AS Site,
                    COUNT(DISTINCT ass_campaign_period.BasketKey) AS total_baskets, 
                    COUNT(DISTINCT CASE WHEN LENGTH(ass_campaign_period.CRN) > 3 THEN ass_campaign_period.BasketKey ELSE NULL END) AS total_scanned_baskets, 
                    SUM(ass_campaign_period.RetailQuantity) AS total_units,
                    SUM(CASE WHEN LENGTH(ass_campaign_period.CRN) > 3 THEN ass_campaign_period.RetailQuantity ELSE 0 END) AS total_scanned_units,
                    COUNT(DISTINCT ass_campaign_period.CRN) AS total_scanned_shoppers, 
                    SUM(CASE WHEN LENGTH(ass_campaign_period.CRN) > 3 THEN  ass_campaign_period.TotalAmountIncludingGST ELSE 0 END) AS total_scanned_spend,
                    SUM(ass_campaign_period.TotalAmountIncludingGST) AS sales_amount

                FROM `gcp-wow-wiq-ca-prod.wiqIN_DataAssets.CustomerBaseTransaction_v` ass_campaign_period
                LEFT JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info trading
                    ON ass_campaign_period.BusinessDate >= trading.media_start_date 
                    AND ass_campaign_period.BusinessDate <= trading.media_end_date 
                INNER JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.unique_skus_2 skus 
                    ON TRIM(skus.sku) = ass_campaign_period.Article 
                LEFT JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.test_stores_2 test_stores 
                    ON CAST(test_stores.test_store AS INT64) = CAST(ass_campaign_period.SiteNumber AS INT64)
                    
                WHERE trading.booking_and_asset_number = '{campaign_id}' 
                AND LOWER(ass_campaign_period.Channel) = "in store"
                AND ass_campaign_period.BusinessDate >= media_start_date_global_var
                AND ass_campaign_period.BusinessDate <= media_end_date_global_var
                AND ass_campaign_period.SalesOrganisation = '1005'
                AND skus.sku IS NOT NULL
                AND skus.sku <> ""
                GROUP BY ALL
        ;

    SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_transactions;
    """
    ).result()
    transactions_df = check.to_dataframe()
    print(transactions_df.head(5))
    chime.success() 
    return transactions_df