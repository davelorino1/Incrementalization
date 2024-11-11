def fit_posterior_sum_of_campaign_week_sales(data_scientist, campaign_id, matched_pairs, transactions_df):
        combined_stores = pd.concat([matched_pairs['test_store'], matched_pairs['control_store']])
        filtered_transactions = transactions_df[transactions_df['Site'].isin(combined_stores)]
        
        # print("Filtered Tranactions: ")
        # print(filtered_transactions)
        # print(f"Processing posterior distributions of sum_of_sales: {campaign_id}")

        # Begin the process of calculating posterior distributions for total sum of sales in each store
        df = filtered_transactions

        # # Convert sales_amount to numeric, forcing any errors to NaN and dropping them
        df['sales_amount'] = pd.to_numeric(df['sales_amount'], errors='coerce')
        df.dropna(subset=['sales_amount'], inplace=True)

        # # Group transactions by store (Site) 
        grouped = df.groupby(['Site', 'test_or_control'])

        # # Initialize an empty dictionary to store full posterior samples for each store
        posterior_samples = {}
        count = 0

        # # # Iterate through each group (store-level) and model the sum of sales as a posterior distribution
        for store, group in grouped:
            sales = group['sales_amount'].values  # extract individual transaction sales for the store
            count = count + 1

            # Check if sales is empty or non-numeric (additional guard)
            if len(sales) == 0 or not np.issubdtype(sales.dtype, np.number):
                
                print(f"Skipping store {store} due to invalid sales data")
                continue

            # Fitting the model for the posterior distribution of the sum of sales
            with pm.Model() as model:

                # Total number of transactions
                n_sales = len(sales)

                # Flat prior for the mean of the total sales across all transactions (meaning no prior belief that any value on the number line is more likely than any other value)
                total_sum_sales = pm.Uniform("total_sum_sales", lower=n_sales * sales.min(), upper=n_sales * sales.max())

                # Per-transaction mean derived from the total sum
                per_transaction_mu = total_sum_sales / n_sales
                
                # Flat prior for the standard deviation of the sales per transaction (meaning no prior belief that any value on the number line is more likely than any other value)
                sigma = pm.Uniform("sigma", lower=0, upper=sales.std() * 2)
                
                # Likelihood of observing sales per transaction
                sales_obs = pm.Normal("sales_obs", mu=per_transaction_mu, sigma=sigma, observed=sales)
                
                # Sampling from the posterior with increased tuning and sample size
                trace = pm.sample(2000, tune=2000, target_accept=0.95, return_inferencedata=True, progressbar=False)

                # Store the full posterior samples for the total sum of sales
                posterior_samples[store] = trace.posterior['total_sum_sales'].to_dataframe()
                print(f"{count} stores fit with posteriors\n")

        # Convert the posterior samples into a dictionary for each store
        # The key will be the store and the value will be a DataFrame of posterior samples
        posterior_samples_dict = {store: df.reset_index(drop=True) for store, df in posterior_samples.items()}

        # Example output for a specific store (the first one in the dictionary)
        store_name = list(posterior_samples_dict.keys())[0]
        chime.success() 
        print(f"Posterior samples for store {store_name}:\n", posterior_samples_dict[store_name].head())

        # Flatten the posterior_samples_dict into a DataFrame
        # Stacking the store IDs and their corresponding posterior samples

        flattened_samples = []
        for store, posterior_df in posterior_samples.items():
            store_id = store[0]  # Extract the store ID
            test_or_control = store[1]  # Extract test or control group info
            posterior_df = posterior_df.reset_index(drop=True)  # Reset the index of the posterior samples
            posterior_df['store'] = store_id  # Add the store ID as a column
            posterior_df['test_or_control'] = test_or_control  # Add the test or control info
            flattened_samples.append(posterior_df)

        # # Concatenate all store samples into a single DataFrame
        flattened_samples_df = pd.concat(flattened_samples, ignore_index=True)

        # # Merging posterior samples of sum_of_sales with historical performance in order to produce z-score distributions from the raw sales distributions
        # # flattened_samples_df: Contains store, test_or_control, total_sum_sales
        # # campaign_data_df: Contains campaign_id, Site (store), stddev_sales_amount, weekly_avg_sales_amt
        check = client.query(
        f"""
            SELECT 
                baseline.*, 
                campaign.test_or_control 
            FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_baseline_statistics_with_campaign baseline
            LEFT JOIN (SELECT DISTINCT campaign_id, Site, test_or_control FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_transactions) campaign
                ON baseline.campaign_id = campaign.campaign_id 
                AND baseline.Site = campaign.Site
            WHERE baseline.campaign_id = '{campaign_id}'
            AND weeks_count = 12
        """
        ).result()
        campaign_data_df = check.to_dataframe()
        campaign_data_df['Site'] = campaign_data_df['Site'].astype(int)
        # Join the historical metrics to the posterior sum of sales distributions by store
        merged_df = pd.merge(flattened_samples_df, 
                            campaign_data_df[['campaign_id', 'Site', 'stddev_sales_amount', 'weekly_avg_sales_amount', 'coefficient_of_variation']],
                            left_on='store', right_on='Site', how='left')

        # Drop the redundant 'Site' column 
        merged_df = merged_df.drop(columns=['Site'])
        # Output the merged DataFrame just to have visibility that everything went fine with the join
        print(merged_df.head(5))

        print(f"Processing z-scores from posterior distribution of sum_of_sales: {campaign_id}")
        # Convert 'total_sum_sales', 'weekly_avg_sales_amt', and 'stddev_sales_amount' to float
        merged_df['total_sum_sales'] = merged_df['total_sum_sales'].astype(float)
        merged_df['weekly_avg_sales_amount'] = merged_df['weekly_avg_sales_amount'].astype(float)
        merged_df['stddev_sales_amount'] = merged_df['stddev_sales_amount'].astype(float)
        merged_df['coefficient_of_variation'] = merged_df['coefficient_of_variation'].astype(float)
        merged_df['campaign_week_uplift'] = (merged_df['total_sum_sales'] - merged_df['weekly_avg_sales_amount']) / merged_df['weekly_avg_sales_amount']
        merged_df['standardized_campaign_week_uplift'] = merged_df['campaign_week_uplift'] / merged_df['coefficient_of_variation']

        # # Calculate the campaign_z_score posterior distributions
        merged_df['campaign_z_score'] = (merged_df['total_sum_sales'] - merged_df['weekly_avg_sales_amount']) / merged_df['stddev_sales_amount']
        merged_df['transformed_campaign_week_uplift'] = merged_df['standardized_campaign_week_uplift'] * merged_df['stddev_sales_amount']
        # # Output the result
        print(merged_df.head(5))
        chime.success() 

        # Ensure 'merged_df' is sorted by 'store' and 'test_or_control'
        merged_df = merged_df.sort_values(by=['store', 'test_or_control'])
        
        return merged_df