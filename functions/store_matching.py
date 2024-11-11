# Match Maker Python Implementation (replaces the C++ version)
def store_matching(df):
    # Helper function for greedy matching
    def greedy_matching(store_pairs):
        matched_pairs = []
        used_test_stores = set()
        used_control_stores = set()

        for _, row in store_pairs.iterrows():
            if row['test_store'] not in used_test_stores and row['control_store'] not in used_control_stores:
                matched_pairs.append(row)
                used_test_stores.add(row['test_store'])
                used_control_stores.add(row['control_store'])

        return pd.DataFrame(matched_pairs)

    # Helper function for global matching
    def global_matching(store_pairs):
        store_pairs_sorted = store_pairs.sort_values('abs_perc_diff')
        return greedy_matching(store_pairs_sorted)

    # Calculate total difference
    def calculate_total_difference(matched_pairs):
        return matched_pairs['abs_perc_diff'].sum()

    # Perform greedy matching
    greedy_result = greedy_matching(df)
    greedy_total_diff = calculate_total_difference(greedy_result)

    # Perform global matching
    global_result = global_matching(df)
    global_total_diff = calculate_total_difference(global_result)

    # Print results
    print(f"Greedy Matching Total Difference: {greedy_total_diff}")
    print(f"Global Matching Total Difference: {global_total_diff}")

    # Return the global matching result as a DataFrame
    return global_result