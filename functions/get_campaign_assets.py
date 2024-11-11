def get_campaign_assets(data_scientist):
    # Select Assets
    check = client.query(
        f"""          
    CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info AS 
        with booking_count AS (
            SELECT DISTINCT
                booking_number,
                line_name,
                CASE WHEN media_start_date = "2024-09-18" THEN 1 ELSE 2 END AS campaign_id_count
            FROM `gcp-wow-cart-data-prod-d710.cdm.dim_cartology_campaigns`
            WHERE quoteline_sku LIKE '%278258%' 
            AND media_type = 'Digital Screens Supers' 
            AND store_list IS NOT NULL 
        )
        SELECT 
            CONCAT(CONCAT(campaigns.booking_number, "_"), booking_count.campaign_id_count) AS booking_and_asset_number,
            campaigns.*
        FROM `gcp-wow-cart-data-prod-d710.cdm.dim_cartology_campaigns` campaigns
        LEFT JOIN booking_count 
            ON booking_count.booking_number = campaigns.booking_number 
            AND booking_count.line_name = campaigns.line_name
        WHERE quoteline_sku LIKE '%278258%' 
        AND media_type = 'Digital Screens Supers' 
        AND store_list IS NOT NULL   
    ;
    SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info; 
    """
    ).result()
    check_df = check.to_dataframe()
    return check_df