def get_combinatorial_historical_sales_for_matching(data_scientist, campaign_id): 
    print(f"Processing baseline historical data: {campaign_id}")
    check = client.query(
    f"""
        with step_one AS (
        SELECT baseline.*, campaign.test_or_control 
        FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_baseline_statistics_with_campaign baseline
        LEFT JOIN (SELECT DISTINCT campaign_id, Site, test_or_control FROM cart-dai-sandbox-nonprod-c3e7.{data_scientist}.regularization_process_campaign_period_transactions) campaign
            ON baseline.campaign_id = campaign.campaign_id 
            AND baseline.Site = campaign.Site
        WHERE baseline.campaign_id = '{campaign_id}'
        AND weeks_count = 12
        ) 
        SELECT 
            test.campaign_id AS study_id, 
            test.Site AS test_store, 
            control.Site AS control_store, 
            ABS(test.weekly_avg_sales_amount / control.weekly_avg_sales_amount - 1) + ABS(test.stddev_sales_amount / control.stddev_sales_amount - 1) AS abs_perc_diff
        FROM step_one test
        LEFT JOIN step_one control 
            ON test.Site <> control.Site 
        WHERE test.test_or_control = "Test" 
        AND control.test_or_control = "Control" 
    """
    ).result()
    historical_performance_df = check.to_dataframe()
    return historical_performance_df