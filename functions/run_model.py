def run_model(merged_df, matched_pairs, metric):
    if metric == "zscore_perc_diff":
        sigma_value = .02 
    if metric == "zscore_delta_times_sd": 
        sigma_value = 500 

    # Initialize a list to store the posterior samples of percentage differences
    posterior_incremental_differences = []

    # Set a small constant to handle division by zero in percentage difference calculation
    epsilon = 1e-6

    print(f"Processing posterior distribution of percentage difference in z-scores: {campaign_id}")
    # Iterate through each pair of test and control stores in 'matched_pairs'
    for _, pair in matched_pairs.iterrows():
        test_store = pair['test_store']
        control_store = pair['control_store']
        
        # Extract posterior samples of z-scores for test and control stores
        test_store_posteriors = merged_df[merged_df['store'] == test_store]['standardized_campaign_week_uplift'].values
        control_store_posteriors = merged_df[merged_df['store'] == control_store]['standardized_campaign_week_uplift'].values

        test_store_std_dev = merged_df[merged_df['store'] == test_store]['stddev_sales_amount'].values
        
        # Ensure that the number of posterior samples matches between test and control stores
        if len(test_store_posteriors) != len(control_store_posteriors):
            raise ValueError(f"Mismatch in posterior sample sizes between test store {test_store} and control store {control_store}")
        
        # Compute the percentage difference using the original formula
        # Handle small denominators by adding epsilon
        with np.errstate(divide='ignore', invalid='ignore'):
            if metric == "zscore_perc_diff": 
                transformed_incremental_uplift = (test_store_posteriors - control_store_posteriors) / np.abs(control_store_posteriors)
            if metric == "zscore_delta_times_sd":
                transformed_incremental_uplift = (test_store_posteriors - control_store_posteriors) * test_store_std_dev
        
        # Store the percentage difference samples along with the test and control store ids
        posterior_incremental_differences.append(pd.DataFrame({
            'test_store': test_store,
            'control_store': control_store,
            'transformed_incremental_uplift': transformed_incremental_uplift
        }))

    # Concatenate all the percentage difference DataFrames
    posterior_incremental_differences_df = pd.concat(posterior_incremental_differences, ignore_index=True)

    # Create an index that identifies which pair each percentage difference belongs to
    pair_labels = posterior_incremental_differences_df.apply(lambda row: (row['test_store'], row['control_store']), axis=1)

    # Create a categorical variable to get codes for each unique pair
    pair_categories = pd.Categorical(pair_labels)
    pair_indices = pair_categories.codes  # This will map each observation to a store pair index
    unique_pairs = pair_categories.categories        

    with pm.Model() as model:
        test_store = pair['test_store']
        control_store = pair['control_store']
        
        # Prior for the global mean percentage difference (mu_pop)
        # Normal Centered at 0.0 to allow for both positive and negative effects
        # mu_pop = pm.Normal('mu_pop', mu=.0, sigma=500)
        if metric == "zscore_delta_times_sd":
            mu_pop = pm.Uniform('mu_pop', lower=-1000, upper=1000)
        if metric == "zscore_perc_diff":
            mu_pop = pm.Normal('mu_pop', mu=0, sigma=0.025)

        # current trial sigma_pop 
        sigma_pop = pm.HalfNormal('sigma_pop', sigma=sigma_value)

        # Non-centered parameterization
        #z = pm.Normal('z', mu=0, sigma=1, shape=len(unique_pairs))
        #store_pair_effects = mu_pop + z * sigma_pop
        #store_pair_effects_repeated = store_pair_effects[pair_indices]

        # store-pair-specific effects (using centered parameterization)
        store_pair_effects = pm.Normal('store_pair_effects', mu=mu_pop, sigma=sigma_pop, shape=len(unique_pairs))
        
        # assign the correct store_pair_effect to each percentage difference using pair_indices
        store_pair_effects_repeated = store_pair_effects[pair_indices]
        
        # prior for the observational standard deviation (sigma_obs)
        if metric == "zscore_delta_times_sd": 
            sigma_obs = pm.HalfNormal('sigma_obs', sigma=250)
        if metric == "zscore_perc_diff":
            sigma_obs = pm.HalfNormal('sigma_obs', sigma=.005)
    
        # observed_perc_diff = pm.StudentT(
        #     'observed_perc_diff',
        #     nu=3,  # Degrees of freedom; adjust based on data characteristics
        #     mu=store_pair_effects_repeated,
        #     sigma=sigma_obs,
        #     observed=posterior_incremental_differences_df['transformed_incremental_uplift'].values
        # )
        
        # Likelihood: Using normal distribution
        observed_perc_diff = pm.Normal(
            'observed_perc_diff',
            mu=store_pair_effects_repeated,
            sigma=sigma_obs,
            observed=posterior_incremental_differences_df['transformed_incremental_uplift'].values
        )
        
        # Sample from the posterior distribution
        trace = pm.sample(
            draws=2000,
            tune=2000,
            target_accept=0.95,
            return_inferencedata=True
        )
    
    # Summarize and analyze the trace
    summary_df = az.summary(
        trace,
        var_names=['mu_pop', 'sigma_pop', 'sigma_obs', 'store_pair_effects'],
        hdi_prob=0.95
    )

    return trace, summary_df, unique_pairs