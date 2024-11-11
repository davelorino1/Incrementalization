def compute_total_sales_and_percentage_diff(data_scientist, matched_pairs, control_df, test_df):

    summary_data = []

    units_and_baskets_stats = client.query(f"""SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_units_and_baskets""").result()
    units_and_baskets_stats = units_and_baskets_stats.to_dataframe() 
    units_and_baskets_stats['total_scanned_units'] = units_and_baskets_stats['total_scanned_units'].astype(float)
    units_and_baskets_stats['total_scanned_spend'] = units_and_baskets_stats['total_scanned_spend'].astype(float)
    units_and_baskets_stats['total_scanned_shoppers'] = units_and_baskets_stats['total_scanned_shoppers'].astype(float)
    # Iterate through each pair in matched_pairs
    for _, pair in matched_pairs.iterrows():
        control_store = pair['control_store']
        test_store = pair['test_store']
        
        # Retrieve control store data
        control_row = control_df[control_df['store'] == control_store]
        control_units_and_baskets = units_and_baskets_stats[units_and_baskets_stats['Site'].astype(int) == control_store]
        if control_row.empty:
            print(f"Control store {control_store} not found in control_df. Skipping.")
            continue
        control_baseline = control_row['baseline_sales_mode'].values[0]
        control_uplift = control_row['uplift_mode'].values[0]
        control_total = control_baseline + control_uplift
        control_units_per_shopper = control_units_and_baskets['total_scanned_units'].values[0] / control_units_and_baskets['total_scanned_shoppers'].values[0]
        control_spend_per_shopper = control_units_and_baskets['total_scanned_spend'].values[0] / control_units_and_baskets['total_scanned_shoppers'].values[0]
        control_spend_per_unit = control_units_and_baskets['total_scanned_spend'].values[0] / control_units_and_baskets['total_scanned_units'].values[0]
        control_uplift_shoppers = control_uplift / control_spend_per_shopper
        control_uplift_units = control_uplift.astype(float) / control_spend_per_unit.astype(float)

        # Retrieve test store data
        test_row = test_df[test_df['store'] == test_store]
        test_units_and_baskets = units_and_baskets_stats[units_and_baskets_stats['Site'].astype(int) == test_store]
        if test_row.empty:
            print(f"Test store {test_store} not found in test_df. Skipping.")
            continue
        test_baseline = test_row['baseline_sales_mode'].values[0]
        test_uplift = test_row['uplift_mode'].values[0]
        test_total = test_baseline + test_uplift
        test_units_per_shopper = test_units_and_baskets['total_scanned_units'].values[0] / test_units_and_baskets['total_scanned_shoppers'].values[0]
        test_spend_per_shopper = test_units_and_baskets['total_scanned_spend'].values[0] / test_units_and_baskets['total_scanned_shoppers'].values[0]
        test_spend_per_unit = test_units_and_baskets['total_scanned_spend'].values[0] / test_units_and_baskets['total_scanned_units'].values[0]
        test_uplift_shoppers = test_uplift.astype(float) / test_spend_per_shopper.astype(float)
        test_uplift_units = test_uplift.astype(float) / test_spend_per_unit.astype(float)

        
        # Compute percentage difference in total sales
        if control_total == 0:
            print(f"Control store {control_store} has zero total sales. Skipping percentage difference calculation.")
            percentage_diff_total_sales = np.nan
            percentage_diff_uplift_sales = np.nan
        else:
            percentage_diff_total_sales = ((test_total - control_total) / control_total) * 100
            percentage_diff_uplift_sales = ((test_uplift - control_uplift) / np.abs(control_uplift)) * 100
            percentage_diff_uplift_shoppers = ((test_uplift_shoppers - control_uplift_shoppers) / np.abs(control_uplift_shoppers)) * 100 
            percentage_diff_uplift_units = ((test_uplift_units - control_uplift_units) / np.abs(control_uplift_shoppers)) * 100
        
        summary_data.append({
            'control_store': control_store,
            'test_store': test_store,
            'control_baseline_sales': control_baseline,
            'control_uplift': control_uplift,
            'control_uplift_shoppers': control_uplift_shoppers, 
            'control_uplift_units': control_uplift_units,
            'control_total_sales': control_total,
            'test_baseline_sales': test_baseline,
            'test_uplift': test_uplift,
            'test_uplift_shoppers': test_uplift_shoppers, 
            'test_uplift_units': test_uplift_units,
            'test_total_sales': test_total,
            'uplift_dollars_raw_diff': test_uplift - control_uplift,
            'total_dollars_raw_diff': test_total - control_total, 
            'percentage_diff_total_sales': percentage_diff_total_sales, 
            'percentage_diff_uplift_sales': percentage_diff_uplift_sales, 
            'percentage_diff_uplift_shoppers': percentage_diff_uplift_shoppers,
            'percentage_diff_uplift_units': percentage_diff_uplift_units
        })
    
    summary_df = pd.DataFrame(summary_data)
    
    if summary_df.empty:
        print("No valid store pairs found. Aggregate metrics will be NaN.")
    
    # Compute aggregate metrics
    aggregate_metrics = {
        'Average Control Baseline Sales ($)': summary_df['control_baseline_sales'].mean(),
        'Average Control Uplift ($)': summary_df['control_uplift'].mean(),
        'Average Control Total Sales ($)': summary_df['control_total_sales'].mean(),
        'Average Test Baseline Sales ($)': summary_df['test_baseline_sales'].mean(),
        'Average Test Uplift ($)': summary_df['test_uplift'].mean(),
        'Average Test Total Sales ($)': summary_df['test_total_sales'].mean(),
        'Average Uplift Dollar Difference ($)': (summary_df['test_uplift'].mean() - summary_df['control_uplift'].mean()) / np.abs(summary_df['control_uplift'].mean()),
        'Average Uplift Shoppers Difference': (summary_df['test_uplift_units'].mean() - summary_df['control_uplift_units'].mean()) / np.abs(summary_df['control_uplift_units'].mean()),
        'Average Uplift Units Difference': (summary_df['test_uplift_shoppers'].mean() - summary_df['control_uplift_shoppers'].mean()) / np.abs(summary_df['control_uplift_shoppers'].mean()),
        'Percentage Difference in Average Total Sales (%)': (summary_df['test_total_sales'].mean() - summary_df['control_total_sales'].mean()) / np.abs(summary_df['control_total_sales'].mean())
        #'95% Credible Interval for Percentage Difference (%)': az.hdi(summary_df['percentage_diff_total_sales'].dropna(), hdi_prob=0.95)
    }
    
    aggregate_df = pd.DataFrame([aggregate_metrics])
    
    return summary_df, aggregate_df