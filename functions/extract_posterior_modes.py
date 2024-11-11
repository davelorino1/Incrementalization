def extract_posterior_modes(merged_df, store_category='Control'):
    """
    Extract posterior modes for baseline sales and uplifts for a given store category.

    Parameters:
    - merged_df (pd.DataFrame): The merged DataFrame containing posterior samples.
    - store_category (str): 'Control' or 'Test'.

    Returns:
    - pd.DataFrame: DataFrame with stores and their corresponding baseline sales and uplifts.
    """
    # Filter stores based on category
    if store_category not in ['Control', 'Test']:
        raise ValueError("store_category must be either 'Control' or 'Test'")
    
    stores = merged_df[merged_df['test_or_control'] == store_category]['store'].unique()
    
    data = []
    for store in stores:
        # Extract posterior samples for baseline sales
        baseline_samples = merged_df[merged_df['store'] == store]['weekly_avg_sales_amount'].values
        baseline_mode = calculate_mode(baseline_samples)
        
        # Extract posterior samples for uplift
        uplift_samples = merged_df[merged_df['store'] == store]['standardized_campaign_week_uplift'].values
        stddev_sales = merged_df[merged_df['store'] == store]['stddev_sales_amount'].values[0]
        uplift_dollars = uplift_samples * stddev_sales
        uplift_mode = calculate_mode(uplift_dollars)
        
        data.append({
            'store': store,
            'baseline_sales_mode': baseline_mode,
            'uplift_mode': uplift_mode
        })
    
    return pd.DataFrame(data)