def get_asset_id_list(data_scientist, asset_table_name): 
    # Campaign IDs to analyze
    check = client.query(
        f"""
        SELECT DISTINCT booking_and_asset_number 
        FROM {asset_table_name} 
        WHERE booking_and_asset_number = "WOW20014359_1"
        """
    ).result()
    campaign_ids_df = check.to_dataframe()
    print("Campaign ids:")
    print(campaign_ids_df)
    # # Campaign Ids to list
    campaign_ids_list = campaign_ids_df['booking_and_asset_number'].tolist()
    return campaign_ids_list