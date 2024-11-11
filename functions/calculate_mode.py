def calculate_mode(posterior_samples):
    """
    Calculate the mode of a set of posterior samples using KDE.
    If KDE fails due to data being on a lower-dimensional subspace, use a histogram-based method.
    
    Parameters:
    - posterior_samples (np.array): Array of posterior samples.
    
    Returns:
    - float: Mode of the posterior samples.
    """
    try:
        kde = gaussian_kde(posterior_samples)
        x_min, x_max = posterior_samples.min(), posterior_samples.max()
        x_grid = np.linspace(x_min, x_max, 1000)
        kde_values = kde(x_grid)
        mode = x_grid[np.argmax(kde_values)]
    except np.linalg.LinAlgError:
        print("gaussian_kde failed due to singular covariance matrix. Falling back to histogram-based mode.")
        counts, bin_edges = np.histogram(posterior_samples, bins=30)
        bin_centers = 0.5 * (bin_edges[1:] + bin_edges[:-1])
        mode = bin_centers[np.argmax(counts)]
    except Exception as e:
        print(f"An unexpected error occurred in calculate_mode: {e}")
        mode = np.nan
    return mode
