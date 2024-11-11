def main_workflow(data_scientist, merged_df, matched_pairs, trace, summary_model_df, unique_pairs, metric="zscore_perc_diff"):

    # Step 1: Extract Posterior Modes for Control and Test Stores
    control_modes_df = extract_posterior_modes(merged_df, store_category='Control')
    test_modes_df = extract_posterior_modes(merged_df, store_category='Test')
    
    # Step 2: Compute Total Sales and Percentage Differences
    summary_store_df, aggregate_metrics_df = compute_total_sales_and_percentage_diff(
        data_scientist=data_scientist,
        matched_pairs=matched_pairs,
        control_df=control_modes_df,
        test_df=test_modes_df
    )
    
    # Step 3: Calculate Dollar Impact Using mu_pop
    # Step 3A: Calculate the average control uplift mode
    average_control_uplift_mode = control_modes_df['uplift_mode'].mean()
    print(f"\nAverage Control Store Uplift (Mean of Modes): ${average_control_uplift_mode:.2f}")
    
    # Step 3B: Extract mu_pop samples from the trace and calculate the mean
    mu_pop_samples = trace.posterior['mu_pop'].values.flatten()
    mu_pop_mean = mu_pop_samples.mean()
    mu_pop_hdi = az.hdi(mu_pop_samples, hdi_prob=0.95)
    print(f"Mean of mu_pop: {mu_pop_mean:.4f} ({mu_pop_mean * 100:.2f}%)")
    print(f"95% Credible Interval for mu_pop: ({mu_pop_hdi[0]:.4f}, {mu_pop_hdi[1]:.4f})")
    
    # Step 3C: Calculate the dollar impact
    avg_incremental_dollars_per_test_store = mu_pop_mean * average_control_uplift_mode
    print(f"Dollar Impact (mu_pop mean * average control uplift mode): ${dollar_impact:.2f}")
    
    return summary_model_df, aggregate_metrics_df, avg_incremental_dollars_per_test_store