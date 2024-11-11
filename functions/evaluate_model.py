def evaluate_model(summary_df):
    population_mean = summary_df.loc['mu_pop', 'mean']
    group_estimates = summary_df.loc[summary_df.index.str.contains('store_'), 'mean']
    
    # Calculate the percentage of group estimates within ±0.05 of the population mean
    within_range = np.abs(group_estimates - population_mean) <= 0.05
    percentage_within_range = np.mean(within_range) * 100
    
    # Calculate the percentage of estimates exactly equal to the population mean (underfitting)
    percentage_exact = np.mean(np.abs(group_estimates - population_mean) < 1e-6) * 100
    
    return percentage_within_range, percentage_exact