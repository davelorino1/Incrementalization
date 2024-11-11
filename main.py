#!/usr/bin/env python3

# run with command /path_to_file/filename.py bigquery_username (dlorino, mcheng, dpoznic) metric (zscore_perc_diff, zscore_delta_times_sd) 

from google.cloud import bigquery
import pandas as pd 
import chime   
import seaborn as sns
import pymc as pm
import numpy as np
import arviz as az
import subprocess # uses the subprocess module to call the C++ 'global vs greedy' matching process 
import os
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import sys 

# locally defined data access and processing / ETL functions
from functions.calculate_mode import calculate_mode 
from functions.get_campaign_assets import get_campaign_assets 
from functions.get_asset_id_and_list import get_asset_id_list 
from functions.get_skus_and_stores import get_skus_and_stores 
from functions.get_baseline_historical_stats import get_baseline_historical_stats 
from functions.percentage_of_stores_with_fewer_than_12_weeks_historical_sales import percentage_of_stores_with_fewer_than_12_weeks_historical_sales
from functions.get_campaign_period_transactions import get_campaign_period_transactions
from functions.get_test_stores import get_test_stores
from functions.get_historical_data_for_store_matching import get_historical_data_for_store_matching
from functions.store_matching import store_matching
from functions.exclude_test_stores_from_control_group import exclude_test_stores_from_control_group
from functions.filter_campaign_period_transactions_to_only_test_and_control_stores import filter_campaign_period_transactions_to_only_test_and_control_stores

# locally defined analysis functions 
from functions.calculate_store_level_posterior_distributions_of_sum_of_sales import calculate_store_level_posterior_distributions_of_sum_of_sales
from functions.run_model import run_model
from functions.evaluate_model import evaluate_model 
from functions.optimize_sigma import optimize_sigma 

client = bigquery.Client("cart-dai-sandbox-nonprod-c3e7")
data_scientist = sys.argv[1]
metric = sys.argv[2]

campaign_assets = get_campaign_assets(data_scientist)
campaign_ids_list = get_asset_id_list(data_scientist, asset_table_name = f"cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info")
 
# Begin loop over each asset
for campaign_id in campaign_ids_list:
    print(f"Processing campaign_id: {campaign_id}")

    # Bakery Campaign Digital Screen Assets 
    check = client.query(
        f"""          
        SELECT * FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_asset_info ORDER BY media_start_date;
         """
    ).result()
    check_df = check.to_dataframe()
    print("Campaign Asset Info: ")
    print(check_df[['booking_number', 'booking_and_asset_number', 'line_name', 'media_start_date']])

    skus_df, stores_df = get_skus_and_stores(data_scientist, campaign_id)
    

    get_baseline_historical_stats(data_scientist, campaign_id)
    transactions_df = get_campaign_period_transactions(data_scientist, campaign_id)

    # # Check if any group has less than 90% stores with 12 weeks of historical sales data
    #if any(check_df['perc_of_stores_with_12_wks_historical_sales'] < 0.9):
    #    print(f"Skipping campaign_id {campaign_id} as less than 90% of stores in either group have 13 weeks of historical sales data.")
    #    continue

    historical_performance_df = get_combinatorial_historical_sales_for_matching(data_scientist, campaign_id)

    print(f"Creating matched pairs: {campaign_id}")
    matched_pairs = store_matching(historical_performance_df)
    merged_df = fit_posterior_sum_of_campaign_week_sales(data_scientist, campaign_id, matched_pairs, transactions_df)

    # merged_df.to_csv(f"../Match Maker/new_metric/{campaign_id}_merged_df.csv")
    # matched_pairs.to_csv(f"../Match Maker/new_metric/{campaign_id}_merged_df.csv_matched_pairs.csv")
    # merged_df = pd.read_csv("../Match Maker/new_metric/{campaign_id}_merged_df.csv")
    # matched_pairs = pd.read_csv("../Match Maker/new_metric/{campaign_id}_merged_df.csv_matched_pairs.csv")
    # matched_pairs = matched_pairs[matched_pairs['abs_perc_diff'] <= 0.06]

    trace, summary_df, unique_pairs = run_model(merged_df, matched_pairs, metric="zscore_perc_diff")
    summary_model_df, aggregate_metrics_df, avg_incremental_dollars_per_test_store = main_workflow(
        data_scientist, 
        merged_df, 
        matched_pairs, 
        trace, 
        summary_model_df, 
        unique_pairs, 
        metric="zscore_perc_diff"
    )

    print("Summary model df: ")
    print(summary_model_df)
    print("Aggregate metrics df: ")
    print(aggregate_metrics_df)
    
    summary_model_df.to_csv(f"./outputs/{campaign_id}_summary_model_df.csv", index=False)
    aggregate_metrics_df.to_csv(f"./outputs/{campaign_id}_aggregate_metrics_df.csv", index=False)
    chime.success() 
    chime.success() 
    chime.success() 

