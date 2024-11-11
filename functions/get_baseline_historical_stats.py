def get_baseline_historical_stats(data_scientist, campaign_id):
    check = client.query(
        f""" 
        DECLARE media_start_date_global_var DATE;
        DECLARE media_end_date_global_var DATE;
        SET media_start_date_global_var = (SELECT DISTINCT media_start_date FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info WHERE booking_and_asset_number = '{campaign_id}');
        SET media_end_date_global_var = (SELECT DISTINCT media_end_date FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info WHERE booking_and_asset_number = '{campaign_id}');
        CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_baseline_statistics_with_campaign AS 
            with step_one AS (
                SELECT 
                    MIN(BusinessDate) AS earliest_date, 
                    MAX(BusinessDate) AS latest_date, 
                    trading.booking_and_asset_number AS campaign_id,
                    trading.media_start_date,
                    trading.media_end_date,
                    ass_pre_campaign.SiteNumber AS Site, 
                    DATE_TRUNC(ass_pre_campaign.BusinessDate, WEEK(WEDNESDAY)) AS sales_week, 
                    SUM(ass_pre_campaign.TotalAmountIncludingGST) AS total_sales_amount,
                    COUNT(DISTINCT ass_pre_campaign.BasketKey) AS total_baskets

                FROM `gcp-wow-wiq-ca-prod.wiqIN_DataAssets.CustomerBaseTransaction_v` ass_pre_campaign
                
                # TODO cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info
                LEFT JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info trading
                    ON ass_pre_campaign.BusinessDate >= DATE_ADD(trading.media_start_date, INTERVAL -12 WEEK)
                    AND ass_pre_campaign.BusinessDate <= DATE_ADD(trading.media_end_date, INTERVAL -1 WEEK)
                
                INNER JOIN cart-dai-sandbox-nonprod-c3e7.{data_scientist}.unique_skus_2 skus 
                    ON TRIM(skus.sku) = TRIM(ass_pre_campaign.Article)
                    
                WHERE trading.booking_and_asset_number = '{campaign_id}' 
                AND LOWER(ass_pre_campaign.Channel) = "in store"
                AND ass_pre_campaign.BusinessDate >= DATE_ADD(media_start_date_global_var, INTERVAL -12 WEEK)
                AND ass_pre_campaign.BusinessDate <= DATE_ADD(media_end_date_global_var, INTERVAL -1 WEEK)
                AND ass_pre_campaign.SalesOrganisation = '1005'
                AND skus.sku IS NOT NULL
                AND skus.sku <> ""
                GROUP BY ALL
            )  
            SELECT 
                campaign_id, 
                Site, 
                COUNT(DISTINCT sales_week) AS weeks_count,
                AVG(total_sales_amount) AS weekly_avg_sales_amount, 
                STDDEV(total_sales_amount) AS stddev_sales_amount,
                STDDEV(total_sales_amount) / AVG(total_sales_amount) AS coefficient_of_variation
            FROM step_one 
            GROUP BY ALL
        ; 

        
        
        """
    ).result()
    check_df = check.to_dataframe()
    return check_df