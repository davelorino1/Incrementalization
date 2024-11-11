# Map store pairs to store_pair_effects for easier interpretation
    store_pair_mapping = {f"store_pair_effects[{i}]": unique_pairs[i] for i in range(len(unique_pairs))}

    #summary_df = optimal_summary

    # Replace the index in summary_df with the store pair labels
    summary_df = summary_df.rename(index=store_pair_mapping)

    # Convert 'mu_pop' and 'store_pair_effects' to percentages
    summary_df[['mean', 'sd', 'hdi_2.5%', 'hdi_97.5%']] *= 100

    # Since store pair labels are tuples, format them as strings for readability
    summary_df.index = summary_df.index.map(lambda x: f"{int(x[0])}_{int(x[1])}" if isinstance(x, tuple) else x)

    # Multiply store_pair_effects by 100 to get percentages
    store_pair_indices = [i for i in summary_df.index if i not in ['mu_pop', 'sigma_pop', 'sigma_obs']]
    #summary_df.loc[store_pair_indices, ['mean', 'sd', 'hdi_2.5%', 'hdi_97.5%']] *= 100

    # Output the summary DataFrame with relevant columns
    print(summary_df[['mean', 'sd', 'hdi_2.5%', 'hdi_97.5%']])

    # Convert the index into a DataFrame for easier handling
    summary_df = summary_df.reset_index()

    # Separate rows where the index is not a store pair (e.g., 'mu_pop', 'sigma_pop')
    non_store_rows = summary_df[summary_df['index'].isin(['mu_pop', 'sigma_pop', 'sigma_obs'])]

    # Filter out only the store pair rows for processing
    store_pair_rows = summary_df[~summary_df['index'].isin(['mu_pop', 'sigma_pop', 'sigma_obs'])]

    # Extract test_store and control_store from the store pair rows
    store_pair_rows[['test_store', 'control_store']] = store_pair_rows['index'].str.extract(r'(\d+\.?\d*)\_(\d+\.?\d*)')

    # Convert store IDs to integers
    store_pair_rows['test_store'] = store_pair_rows['test_store'].astype(float).astype(int)
    store_pair_rows['control_store'] = store_pair_rows['control_store'].astype(float).astype(int)

    # Drop the original 'index' column from store pair rows
    store_pair_rows.drop(columns=['index'], inplace=True)

    # Combine the population-level rows back with the processed store pair rows
    summary_df_processed = pd.concat([non_store_rows, store_pair_rows], ignore_index=True)

    # Display the processed DataFrame
    print(summary_df_processed.head(20))
    chime.success() 

    print(f"Adding empirical calculation of percentage difference in z-scores for comparison: {campaign_id}")
    check = client.query(
        f"""
        with campaign_period_sum_of_sales AS (
            SELECT 
                Site, 
                test_or_control,
                SUM(sales_amount) AS total_campaign_period_sales_amount 
            FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_transactions
            WHERE test.campaign_id = '{campaign_id}'
            GROUP BY 1,2
        ), 
        historical_performance AS (
            SELECT 
                baseline.Site, 
                baseline.test_or_control,
                baseline.weekly_avg_sales_amount, 
                baseline.stddev_sales_amount,
                campaign_period_sum_of_sales.total_campaign_period_sales_amount, 
                (campaign_period_sum_of_sales.total_campaign_period_sales_amount - baseline.weekly_avg_sales_amount) / baseline.stddev_sales_amount AS campaign_z_score_empirical
            FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_baseline_statistics_with_campaign baseline
            LEFT JOIN campaign_period_sum_of_sales 
                ON baseline.Site = campaign_period_sum_of_sales.Site
            WHERE booking_id = '{campaign_id}'
        )
        SELECT 
            '{campaign_id}' AS campaign_id, 
            test.Site AS test_store, 
            control.Site AS control_store, 
            test.total_campaign_period_sales_amount AS test_store_campaign_period_sales,
            control.total_campaign_period_sales_amount AS control_store_campaign_period_sales, 
            test.weekly_avg_sales_amount AS test_store_weekly_avg_sales_amount, 
            control.weekly_avg_sales_amount AS control_store_weekly_avg_sales_amount, 
            test.stddev_sales_amount AS test_store_stddev_sales_amount, 
            control.stddev_sales_amount AS control_store_stddev_sales_amount, 
            test.campaign_z_score_empirical AS test_store_empirical_sales_amount_z_score,
            control.campaign_z_score_empirical AS control_store_empirical_sales_amount_z_score
        FROM historical_performance test 
        LEFT JOIN historical_performance control
            ON test.Site <> control.Site 
        WHERE test.campaign_id = '{campaign_id}'
        AND control.campaign_id = '{campaign_id}'
        AND test.test_or_control = "Test" 
        AND control.test_or_control = "Control" 
        AND test.weeks_count = 12
        AND control.weeks_count = 12
        ) 
        SELECT 
            *, 
            (test_store_empirical_sales_amount_z_score - control_store_empirical_sales_amount_z_score) / ABS(control_store_empirical_sales_amount_z_score) AS z_score_perc_diff_empirical
        FROM step_one
    """
    ).result()
    check_df = check.to_dataframe()
    chime.success() 
    result_df = check_df.merge(matched_pairs, on=['test_store', 'control_store'], how='inner')
    print(result_df)

    print(f"Saving final results: {campaign_id}")
    # Ensure that 'test_store' and 'control_store' in merged_df are integers for joining
    result_df['test_store'] = result_df['test_store'].astype(int)
    result_df['control_store'] = result_df['control_store'].astype(int)
    result_df['z_score_perc_diff_empirical'] = result_df['z_score_perc_diff_empirical'] * 100
    pd.set_option('display.float_format', '{:,.2f}'.format)
    # Join the processed summary dataframe with the merged_df on test_store and control_store
    comparison_df = pd.merge(
        summary_df_processed[['test_store', 'control_store', 'mean']],
        result_df[['test_store', 'control_store', 'z_score_perc_diff_empirical']],  # Keep the relevant columns from merged_df
        on=['test_store', 'control_store'],
        how='left'
    )

    # Rename the columns for clarity
    comparison_df = comparison_df.rename(columns={
        'mean': 'pymc_percentage_diff',
        'z_score_perc_diff_empirical': 'z_score_perc_diff_empirical'
    })

    # Display the merged dataframe for comparison
    comparison_df['pymc_percentage_diff'] = comparison_df['pymc_percentage_diff'].round(2)
    # Adding the campaign_id column to the DataFrame
    comparison_df.insert(0, 'campaign_id', campaign_id)

    # Display the updated DataFrame
    print(comparison_df)