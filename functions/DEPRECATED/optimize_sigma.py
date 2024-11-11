def optimize_sigma(merged_df, matched_pairs, metric, max_iterations=80):
    if metric == "zscore_perc_diff":
        initial_sigma = .02 
        initial_step = .01 
    if metric == "zscore_delta_times_sd": 
        initial_sigma = 500 
        initial_step = 100
    current_sigma = round(initial_sigma, 5)
    step_size = round(initial_step, 5)
    results = []
    tested_sigmas = set()
    previous_direction = None
    optimal_trace = None
    optimal_summary = None
    unique_pairs = None

    for iteration in range(max_iterations):
        print(f"\nIteration {iteration + 1}/{max_iterations}")
        print(f"Testing sigma: {current_sigma:.5f}")

        if current_sigma in tested_sigmas:
            print("Arrived at a previously tested sigma. Taking a half step in the opposite direction.")
            step_size = round(step_size / 2, 5)
            if previous_direction == 'loosen':
                current_sigma = round(current_sigma - step_size, 5)
            else:
                current_sigma = round(current_sigma + step_size, 5)
            print(f"New sigma: {current_sigma:.5f}, New step size: {step_size:.5f}")
            continue

        tested_sigmas.add(current_sigma)

        trace, summary_df, unique_pairs = run_model(current_sigma, merged_df, matched_pairs, metric)
        #percentage_within_range, percentage_close = evaluate_model(summary_df)  # Corrected variable names
        percentage_within_range, percentage_exact = evaluate_model(summary_df)  # Corrected variable names
        print(summary_df)
        break
        results.append({
            'iteration': iteration + 1,
            'sigma': current_sigma,
            'percentage_within_range': percentage_within_range,
            'percentage_exact': percentage_exact  # Updated variable name
        })

        print(f"Percentage within range: {percentage_within_range:.2f}%")
        print(f"Percentage exact to mean: {percentage_exact:.2f}%")  # Updated print statement

        if percentage_within_range >= 46 and percentage_exact < 5:  # Adjusted criteria
            print("Acceptance criteria met. Stopping optimization.")
            optimal_trace = trace
            optimal_summary = summary_df
            break

        is_underfitting = percentage_exact >= 5  # Updated variable name
        is_overfitting = percentage_within_range < 46

        if is_underfitting and is_overfitting:
            print("Both underfitting and overfitting detected. This is an unexpected state.")
            break

        current_direction = 'loosen' if is_underfitting else 'tighten'

        if previous_direction is not None and ((current_direction != previous_direction) or (current_direction == "tighten" and current_sigma <= 0.005)):
            step_size = round(step_size / 2, 5)
            print(f"Direction changed. New step size: {step_size:.5f}")

        if (current_direction == "tighten" and current_sigma <= 0.005):
            step_size = round(step_size / 2, 5)
            print(f"Decreasing from .0005% - new step size: {step_size:.5f}")

        if is_underfitting:
            current_sigma = round(current_sigma + step_size, 5)
            print(f"Underfitting. Increasing sigma to {current_sigma:.5f} for next iteration.")
            previous_direction = 'loosen'
        else:
            current_sigma = round(max(0, current_sigma - step_size), 5)
            print(f"Overfitting. Decreasing sigma to {current_sigma:.5f} for next iteration.")
            previous_direction = 'tighten'

        if current_sigma <= 0:
            print("Sigma reached zero or became negative. Stopping optimization.")
            break

    results_df = pd.DataFrame(results)

    if len(results_df) == max_iterations:
        print("Reached maximum iterations without finding optimal sigma.")
    elif optimal_trace is None:
        print("No optimal sigma found.")
    else:
        optimal_sigma = results_df.loc[results_df['iteration'].idxmax(), 'sigma']
        print(f"\nOptimization complete.")
        print(f"Optimal sigma: {optimal_sigma:.5f}")
        print(f"Final percentage within range: {results_df['percentage_within_range'].iloc[-1]:.2f}%")
        print(f"Final percentage close to mean: {results_df['percentage_exact'].iloc[-1]:.2f}%")  # Updated print statement

    return results_df, optimal_trace, optimal_summary, summary_df, unique_pairs