def get_skus_and_stores(data_scientist, campaign_id):
        skus = client.query(
        f"""
            CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.unique_skus AS (
                SELECT DISTINCT 
                    booking_and_asset_number, 
                    sku 
                FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info,
                UNNEST(SPLIT(quoteline_sku, ",")) AS sku 
                WHERE booking_and_asset_number = '{campaign_id}'
                AND sku IS NOT NULL
                AND LOWER(sku) <> "npd"
                AND sku <> ""
            );

            CREATE OR REPLACE TABLE cart-dai-sandbox-nonprod-c3e7.{data_scientist}.test_stores AS (
                SELECT DISTINCT 
                    booking_number, 
                    booking_and_asset_number,
                    test_store 
                # TODO cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info
                FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info,
                UNNEST(SPLIT(store_list, ",")) AS test_store 
                WHERE booking_and_asset_number = '{campaign_id}'
                AND test_store IS NOT NULL
            );

            SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.unique_skus
        """
        ).result()
        skus_df = skus.to_dataframe()

        stores = client.query(f"""SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.test_stores""").result()
        stores_df = stores.to_dataframe()
        return skus_df, stores_df