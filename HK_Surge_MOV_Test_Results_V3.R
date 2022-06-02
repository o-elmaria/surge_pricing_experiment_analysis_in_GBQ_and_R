# IMPORTANT NOTE: Search for ***FUNCTION CALLING*** to see the lines where you call functions
# V3 has variant instead of experiment_variant

# Step 1: Load libraries
pacman::p_load(dplyr, ggplot2, bigrquery, reshape2, formattable, stringr, FedData, tibble, ggpubr)

# Step 1.2: Define the input variables that are going to be used throughout the code
target_group_names <- c("TG1", "TG2") # Names of the target groups that should be filtered for in the SQL query. Always follow a chronological order

# Define the column order for the business KPIs --> Structure (Grp_Control, Grp_Var1, Grp_Delta_V1_C, Grp_Var2, Grp_Delta_V2_C, Grp_Var3, Grp_Delta_V3_C, etc.). USED IN df_overall_reshaped
col_order <- c("entity_id", "experiment_id", "variable", "TG1_TG2_Control", "TG1_TG2_Var9", "Delta_Pct_V9_C_TG1_TG2", "Delta_Abs_V9_C_TG1_TG2") # Group = TG1 & TG2 combined

# Combined target groups
combined_tg <- "TG1_TG2" # If target groups need to be combined, specify the new target group designation here

# Variation(s) of concern
var_of_concern = "Var9"
variation_not_considered <- paste0(substr(var_of_concern, 1, 3), "iation", substr(var_of_concern, nchar(var_of_concern), nchar(var_of_concern))) # Used in the pre-post test part as well as the GFV distribution plot

# Number of cvr_metrics calculated in df_cvr_dwnld
num_cvr_metric = 3 # 3 for CVR2, CVR3, and mCVR4

# Surge event, Non-surge event, or both
surge_event_indicator <- "surge_event" # Can only take three values --> "surge_event", "non_surge_event", and "both"

# BQ table name
bq_tbl_name <- "business_kpis_smov_vs_sdf_hk_surge_mov"

# Experiment IDs
exp_id_var = 15 # 15: Central (Pre-Post), 16: TSW (Pre), 17: TSW (Post) - Used for the AB test part of the code
central_pre_post_exp_id = 15 # Used in the pre-post test part of the code
tsw_pre_exp_id = 16 # Used in the pre-post test part of the code
tsw_post_exp_id = 17 # Used in the pre-post test part of the code

# Dates (used in the pre-post test part)
transition_day <- as.Date("2022-04-20") # Transition day of the variation's post period experiment (i.e., TSW-post) that needs to be dropped
start_date_post_control_experiment <- as.Date("2022-04-21") # End of the pre-period in the control experiment (i.e., Central)
end_date_pre_control_experiment <- as.Date("2022-04-19") # End of the pre-period in the control experiment (i.e., Central)

# Exclusions for the pre-post test (used in the pre-post test part)
date_exclusion <- as.Date("2022-05-01") # Due to outliers

##-----------------------------------------------------END OF STEP 1-----------------------------------------------------##

# Step 2: Downloading the test's datasets
options(gargle_oauth_email = "omar.elmaria@deliveryhero.com")

# Define the project ID and dataset's name
project_id_data <- "dh-logistics-product-ops"
project_id_billing <- "logistics-data-staging-flat"
data_set <- "pricing"

bq_conn <- dbConnect(bigquery(),
                     project = project_id_data,
                     dataset = data_set,
                     billing = project_id_billing,
                     use_legacy_sql = FALSE)

# Orders data
df_ord <- tbl(bq_conn, "ab_test_individual_orders_cleaned_hk_surge_mov") # Contains business and logistical KPIs of "Central", "TSW-Pre", and "TSW-Post"

# CVR data
df_cvr_overall_with_surge_flag <- tbl(bq_conn, "ab_test_cvr_data_cleaned_with_surge_flag_hk_surge_mov_overall") # Contains conversion and user count data of ALL zones with the surge flag (Overall)
df_cvr_per_day_with_surge_flag <- tbl(bq_conn, "ab_test_cvr_data_cleaned_with_surge_flag_hk_surge_mov_per_day") # Contains conversion and user count data of ALL zones with the surge flag (Per Day)

df_cvr_overall_wout_surge_flag <- tbl(bq_conn, "ab_test_cvr_data_cleaned_wout_surge_flag_hk_surge_mov_overall") # Contains conversion and user count data of ALL zones without the surge flag (Overall)
df_cvr_per_day_wout_surge_flag <- tbl(bq_conn, "ab_test_cvr_data_cleaned_wout_surge_flag_hk_surge_mov_per_day") # Contains conversion and user count data of ALL zones without the surge flag (Per Day)

##-----------------------------------------------------END OF STEP 2 AND INPUT SECTION-----------------------------------------------------##

# Step 3: Calculation of the correction factors based on the user counts

# Create a function to extract the user count data from df_cvr_overall_central_pre_post
corr_factor_func <- function(per_day, surge_flag) {
  # Select the right CVR table for the correction factor
  if ((per_day == "No" & surge_flag == "surge_event") | (per_day == "No" & surge_flag == "non_surge_event")) {
    cvr_df <<- df_cvr_overall_with_surge_flag # Global assignment
  } else if ((per_day == "Yes" & surge_flag == "surge_event") | (per_day == "Yes" & surge_flag == "non_surge_event")) {
    cvr_df <<- df_cvr_per_day_with_surge_flag # Global assignment
  } else if (per_day == "No" & surge_flag == "both") {
    cvr_df <<- df_cvr_overall_wout_surge_flag # Global assignment
  } else if (per_day == "Yes" & surge_flag == "both") {
    cvr_df <<- df_cvr_per_day_wout_surge_flag # Global assignment
  }
  
  df_corr_factor <- cvr_df %>%
    group_by(experiment_id) %>% 
    {if (surge_flag == "both") select(., everything()) else if (surge_flag == "surge_event") filter(., surge_event_flag == "surge_event") else if (surge_flag == "non_surge_event") filter(., surge_event_flag == "non_surge_event")} %>%
    {if (per_day == "Yes") select(., entity_id, experiment_id, created_date, target_group, variant, users) else select(., entity_id, experiment_id, target_group, variant, users)} %>% 
    {if (per_day == "Yes") group_by(., entity_id, experiment_id, created_date, target_group, variant) else group_by(., entity_id, experiment_id, target_group, variant)} %>% 
    collect() %>% 
    as.data.frame()
  
  # Isolate the user count of the control group
  users_control_grp <- df_corr_factor %>%
    filter(variant == "Control")
  
  # Join the user count of the control group to df_corr_factor as a new column and calculate the correction factor
  df_corr_factor <- df_corr_factor %>% 
    {if (per_day == "Yes") 
      left_join(., users_control_grp %>% select(-variant), by = c("entity_id", "experiment_id", "target_group", "created_date"), suffix = c("_variant", "_control")) else 
      left_join(., users_control_grp %>% select(-variant), by = c("entity_id", "experiment_id", "target_group"), suffix = c("_variant", "_control"))
    } %>% 
    mutate(corr_factor = users_control / users_variant - 1) %>% 
    {if (per_day == "Yes") arrange(., entity_id, experiment_id, created_date, target_group, variant) else arrange(., entity_id, experiment_id, target_group, variant)}
  
  return(df_corr_factor)
}

# ***FUNCTION CALLING***
# Central
df_corr_factor_overall <- corr_factor_func("No", surge_event_indicator)
df_corr_factor_per_day <- corr_factor_func("Yes", surge_event_indicator)

##-----------------------------------------------------END OF STEP 3-----------------------------------------------------##

# Step 4: Create a function that does the data aggregation for the different target groups
results_func <- function(raw_ord_df, tg_incl_filter_var, tg, per_day, corr_factor_overall, corr_factor_per_day, surge_flag, exp_id) {
  df_temp <- raw_ord_df %>%
    {if (surge_flag == "both") select(., everything()) else if (surge_flag == "surge_event") filter(., surge_event_flag == "surge_event") else if (surge_flag == "non_surge_event") filter(., surge_event_flag == "non_surge_event")} %>% 
    filter(experiment_id == exp_id & target_group %in% tg_incl_filter_var) %>% 
    mutate(target_group = tg)

  df_temp <- df_temp %>% 
    {if (per_day == "Yes") group_by(., entity_id, experiment_id, created_date, target_group, variant) else group_by(., entity_id, experiment_id, target_group, variant)} %>% 
    summarise(tot_orders = n_distinct(order_id),
              
              tot_df_local = sum(actual_df_paid_by_customer, na.rm = TRUE),
              avg_df_local = sum(actual_df_paid_by_customer, na.rm = TRUE) / n_distinct(order_id),
              
              tot_commission_local = sum(commission_local, na.rm = TRUE),
              avg_commission_local = sum(commission_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_joker_vendor_fee_local = sum(joker_vendor_fee_local, na.rm = TRUE),
              avg_joker_vendor_fee_local = sum(joker_vendor_fee_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_gmv_local = sum(gmv_local, na.rm = TRUE),
              avg_gmv_local = sum(gmv_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_gfv_local = sum(gfv_local, na.rm = TRUE),
              avg_gfv_local = sum(gfv_local, na.rm = TRUE) / n_distinct(order_id),
              
              avg_mov_local = mean(mov_local, na.rm = TRUE),
              
              tot_sof_local = sum(sof_local, na.rm = TRUE),
              avg_sof_local = sum(sof_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_surge_fee_local = sum(dps_surge_fee_local, na.rm = TRUE),
              avg_surge_fee_local = sum(dps_surge_fee_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_service_fee_local = sum(service_fee_local, na.rm = TRUE),
              avg_service_fee_local = sum(service_fee_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_rev_local = sum(revenue_local, na.rm = TRUE),
              avg_rev_local = sum(revenue_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_del_costs_local = sum(delivery_costs_local, na.rm = TRUE),
              avg_del_costs_local = sum(delivery_costs_local, na.rm = TRUE) / n_distinct(order_id),
              
              tot_profit_local = sum(gross_profit_local, na.rm = TRUE),
              avg_profit_local = sum(gross_profit_local, na.rm = TRUE) / n_distinct(order_id),
              
              avg_travel_time = mean(dps_travel_time, na.rm = TRUE),
              avg_delivery_dist = mean(delivery_distance_m / 1000, na.rm = TRUE),
              avg_fleet_delay = mean(dps_mean_delay, na.rm = TRUE)) %>%
    {if (per_day == "Yes") arrange(., entity_id, experiment_id, created_date, target_group, variant) else arrange(., entity_id, experiment_id, target_group, variant)} %>% 
    collect()
  
  # Incorporate the correction factor
  df_temp <- df_temp %>%
    {if (per_day == "Yes") 
      left_join(., corr_factor_per_day %>% select(entity_id, experiment_id, created_date, target_group, variant, corr_factor), by = c("entity_id", "experiment_id", "created_date", "target_group", "variant")) else
      left_join(., corr_factor_overall %>% select(entity_id, experiment_id, target_group, variant, corr_factor), by = c("entity_id", "experiment_id", "target_group", "variant"))
    } %>% 
    {if (per_day == "Yes") group_by(., entity_id, experiment_id, created_date, target_group, variant) else group_by(., entity_id, experiment_id, target_group, variant)} %>%
    mutate(tot_orders_adj = tot_orders * (1 + corr_factor),
           tot_df_local_adj = avg_df_local * tot_orders_adj,
           tot_commission_local_adj = avg_commission_local * tot_orders_adj,
           tot_joker_vendor_fee_local_adj = avg_joker_vendor_fee_local * tot_orders_adj,
           tot_gmv_local_adj = avg_gmv_local * tot_orders_adj,
           tot_gfv_local_adj = avg_gfv_local * tot_orders_adj,
           tot_sof_local_adj = avg_sof_local * tot_orders_adj,
           tot_surge_fee_local_adj = avg_surge_fee_local * tot_orders_adj ,
           tot_service_fee_local_adj = avg_service_fee_local * tot_orders_adj,
           tot_rev_local_adj = avg_rev_local * tot_orders_adj,
           tot_del_costs_local_adj = avg_del_costs_local * tot_orders_adj,
           tot_profit_local_adj = avg_profit_local * tot_orders_adj)
  
  # Re-organize the columns to be in the correct order
  kpis <- c(# Total KPIs
            "tot_orders", "tot_orders_adj", "corr_factor",
            "tot_df_local", "tot_df_local_adj",
            "tot_commission_local", "tot_commission_local_adj",
            "tot_joker_vendor_fee_local", "tot_joker_vendor_fee_local_adj",
            "tot_gmv_local", "tot_gmv_local_adj",
            "tot_gfv_local", "tot_gfv_local_adj",
            "tot_sof_local", "tot_sof_local_adj",
            "tot_surge_fee_local", "tot_surge_fee_local_adj",
            "tot_service_fee_local", "tot_service_fee_local_adj",
            "tot_rev_local", "tot_rev_local_adj",
            "tot_del_costs_local", "tot_del_costs_local_adj",
            "tot_profit_local", "tot_profit_local_adj",
            
            # Per-order KPIs
            "avg_df_local", "avg_commission_local", "avg_joker_vendor_fee_local", "avg_gmv_local", "avg_gfv_local",
            "avg_mov_local", "avg_sof_local", "avg_surge_fee_local", "avg_service_fee_local",
            "avg_rev_local", "avg_del_costs_local", "avg_profit_local",
            
            # Logistical KPIs
            "avg_travel_time", "avg_delivery_dist", "avg_fleet_delay")
  
  df_temp <- df_temp %>% 
    {if (per_day == "Yes") select(., entity_id, experiment_id, created_date, target_group, variant, all_of(kpis)) else select(., entity_id, experiment_id, target_group, variant, all_of(kpis))} 
  
  # Adjust the formatting of the columns
  df_temp <- df_temp %>%
    mutate_at(c("tot_orders", "tot_orders_adj"), as.integer) %>% 
    mutate_if(is.numeric, round, 4)
}

##-----------------------------------------------------END OF STEP 4-----------------------------------------------------##

# Step 5: Create a dataset of aggregated KPIs for the different target groups

# Overall (i.e. no grouping by created_date) with target groups
# ***FUNCTION CALLING***
df_overall <- results_func(df_ord, target_group_names, combined_tg, "No", df_corr_factor_overall, df_corr_factor_per_day, surge_event_indicator, exp_id_var)

# Change the structure of the df_overall table such that it shows the KPIs as rows and the variant + target groups as columns
df_overall_reshaped <-  recast(df_overall, entity_id + experiment_id + variable ~ target_group + variant, id.var = c("entity_id", "experiment_id", "target_group", "variant"))

# Change the formatting of the cells so that the numbers have only four decimal places
df_overall_reshaped <- df_overall_reshaped %>%
  mutate_if(is.numeric, round, 4)

##-----------------------------------------------------END OF STEP 5-----------------------------------------------------##

# Step 6: Calculate the deltas for df_overall_reshaped and shortening "Variation" to "Var"

# Get all columns that have the word "Variation" in their names
delta_calc_func <- function(df) {
  variation_col_names <- colnames(df)[grep("Variation|Var", colnames(df))]

  for (i in variation_col_names) {
    # Create new columns for the delta between variation (x) and control
    df <- df %>%
      dplyr::mutate(Delta_Pct = round(!!as.name(i) / TG1_TG2_Control - 1, 4),
                    Delta_Abs = round(!!as.name(i) - TG1_TG2_Control, 4))
    
    # Get the variation number
    last_string_character <- substr_right(tail(strsplit(i, "_")[[1]], 1), 1) # If the string = "TG1_TG2_Variation1", this will return "1"
    
    # Get the target group associated with the column
    tg_associtated_with_col <- str_extract_all(i, "TG[0-9]")
    if (length(tg_associtated_with_col[[1]]) > 1) {
      tg_associtated_with_col <- paste(tg_associtated_with_col[[1]], collapse = "_")
    } # If more than one target group exists (e.g., TG1 TG2), collapse them into a single string whose components are separated by "_"
    
    # Add the correct suffixes to the columns
    colnames(df)[length(colnames(df))] <- paste0("Delta_Abs", "_V", last_string_character, "_C_", tg_associtated_with_col)
    colnames(df)[length(colnames(df)) - 1] <- paste0("Delta_Pct", "_V", last_string_character, "_C_", tg_associtated_with_col)
  }
  
  return(df)
}

df_overall_reshaped <- delta_calc_func(df_overall_reshaped)

# Shortening "Variation" to "Var"
colnames(df_overall_reshaped) <- str_replace_all(colnames(df_overall_reshaped), "Variation", "Var")

# Re-order the columns and choose the relevant ones
df_overall_reshaped <- df_overall_reshaped[, col_order]

##-----------------------------------------------------END OF STEP 6-----------------------------------------------------##

# Step 7: Calculate significance
# Step 7.1: Create a data frame with the data grouped by (entity_id, experiment_id, created_date, target_group, variant)

# Per Day with target groups
# ***FUNCTION CALLING***
df_per_day <- results_func(df_ord, target_group_names, combined_tg, "Yes", df_corr_factor_overall, df_corr_factor_per_day, surge_event_indicator, exp_id_var)

# Replace "Variation" with "Var" to be consistent with the nomenclature above
df_per_day$variant <- gsub("Variation", "Var", df_per_day$variant) # Must access the column using the "$" operator

# Arrange the newly formed data frame and change the target_group and variant columns into ordered factors
df_per_day <- df_per_day %>% 
  mutate_at(.vars = vars(target_group), ~ factor(., levels = combined_tg, ordered = TRUE)) %>% 
  mutate_at(.vars = vars(variant), ~ factor(., levels = sort(unique(df_per_day$variant)), ordered = TRUE))

df_per_day <- df_per_day %>% 
  arrange(entity_id, experiment_id, created_date, target_group, variant)

##-----------------------------------------------------END OF STEP 7.1-----------------------------------------------------##

# Step 7.2: Create intermediary data frames to calculate significance

colnames_tot <- colnames(df_per_day)[grep("tot", colnames(df_per_day))] # Column names starting with "tot"
colnames_avg <- colnames(df_per_day)[grep("avg", colnames(df_per_day))] # Column names starting with "avg"

# Create a data frame that will contain the p-values for all KPIs, variants, target groups for the V(x)_C pair
pval_tbl_biz_kpi_func <- function(df_per_day, df_overall_reshaped){
  # Create an empty data frame
  df_pval_tbl <- data.frame(matrix(ncol = nlevels(df_per_day$target_group) + 3, nrow = nrow(df_overall_reshaped))) # of columns is the # of target groups + 3 for entity_id, exp_id, and var_name. # of rows is the # of KPIs
  
  # Get the col numbers of the relevant columns
  index_kpis_col <- grep("entity_id|experiment_id|variable", colnames(df_overall_reshaped)) # The column number that contains the KPIs
  
  # Name the columns
  colnames(df_pval_tbl) <- c(colnames(df_overall_reshaped)[index_kpis_col], levels(df_per_day$target_group)) # Levels gives us the number of factors
  
  # Populate the columns
  df_pval_tbl$entity_id <- df_overall_reshaped$entity_id
  df_pval_tbl$experiment_id <- df_overall_reshaped$experiment_id
  df_pval_tbl$variable <- df_overall_reshaped$variable
  
  return(df_pval_tbl)
}

# ***FUNCTION CALLING***
df_pval_v9_c <- pval_tbl_biz_kpi_func(df_per_day, df_overall_reshaped)

##-----------------------------------------------------END OF STEP 7.2-----------------------------------------------------##

# Step 7.3: Calculate significance for all KPIs and target groups

target_groups <- levels(df_per_day$target_group) # Used in the for loop to filter for different target groups

# Create a function that calculates the p-values of all KPIs for any variation pair
pval_calc_biz_kpi_func <- function(df_per_day, variation, df_pval, var_pair_num) {
  j <- 4 # Helper counter for the Wilcoxon test command in the "else" section
  
  for (x in 1:length(levels(df_per_day$target_group))) { # Loops over the different df_pval columns
    for (i in 1:ncol(df_per_day)) { # Loops over the different columns in df_per_day
      if (colnames(df_per_day[, i]) %in% c("entity_id", "experiment_id", "created_date", "variant", "target_group", "corr_factor") | any(is.na(df_per_day[, i]))){ # HERE: Conditions could be changed
        next
      } else {
        pval_stg <- df_per_day %>% 
          filter(variant %in% c(variation, "Control"))
        
        df_pval[i-5, j] <- round(wilcox.test( # "i-5" because the first 3 columns in df_per_day will be skipped
          na.omit(df_per_day[df_per_day$variant == variation & df_per_day[, "target_group"] == target_groups[x], i])[[1]], # You must have [[1]]
          na.omit(df_per_day[df_per_day$variant == "Control" & df_per_day[, "target_group"] == target_groups[x], i])[[1]],  # You must have [[1]]
          paired = TRUE, alternative = "two.sided", conf.int = FALSE, exact = FALSE, correct = FALSE)$p.value, 4)
      }
    }
    j <- j + 1 # Goes from one column to the next in df_pval
  }
  
  df_pval <- df_pval %>% 
    mutate(Var_Pair = paste0("V", var_pair_num, "_Control"))
  
  return(df_pval)
}

# x refers to the target group
# i refers to the KPI
# j refers to the columns in df_pval_v1_c (i.e. which target group will be used as a filter in the df_per_day data frame)
# Note: We decrease "i" by 3 because the first 3 elements of df_per_day are skipped (i.e. i becomes 4) and we need to start with i = 1 to populate df_pval_v1_c

# ***FUNCTION CALLING***
df_pval_v9_c <- pval_calc_biz_kpi_func(df_per_day, var_of_concern, df_pval_v9_c, substr_right(var_of_concern, 1))

##-----------------------------------------------------END OF STEP 7.3-----------------------------------------------------##

# Step 8: Combine the p-values of V1_C, V2_C, etc. and display the final result (p-values of the business KPIs)
df_pval_biz_kpis_all <- df_pval_v9_c # Use rbind if you have multiple data frames

# Re-order the columns
df_pval_biz_kpis_all <- df_pval_biz_kpis_all[, c("Var_Pair", colnames(df_pval_v9_c)[1:(length(df_pval_v9_c)-1)])]

# Rename the last column to P-Val instead of TG!_TG2
colnames(df_pval_biz_kpis_all)[ncol(df_pval_biz_kpis_all)] = "P_Val"

##-----------------------------------------------------END OF STEP 8-----------------------------------------------------##

# Step 9: Display the CVR data frame in the same format as the business KPIs data frame

# Retrieve the CVR data frame
contingency_tbl_func <- function(per_day, surge_flag, tg, exp_id) {
  # Select the right CVR table
  if ((per_day == "No" & surge_flag == "surge_event") | (per_day == "No" & surge_flag == "non_surge_event")) {
    cvr_df <<- df_cvr_overall_with_surge_flag # Global assignment
  } else if ((per_day == "Yes" & surge_flag == "surge_event") | (per_day == "Yes" & surge_flag == "non_surge_event")) {
    cvr_df <<- df_cvr_per_day_with_surge_flag # Global assignment
  } else if (per_day == "No" & surge_flag == "both") {
    cvr_df <<- df_cvr_overall_wout_surge_flag # Global assignment
  } else if (per_day == "Yes" & surge_flag == "both") {
    cvr_df <<- df_cvr_per_day_wout_surge_flag # Global assignment
  }
  
  # Filter the CVR table for the right experiment ID and target group. Add a condition to filter for the right surge_event_flag if it exists
  df_cvr_dwnld <- cvr_df %>% 
    filter(experiment_id == exp_id, target_group %in% tg) %>%
    {if (surge_flag != "both") filter(., surge_event_flag == surge_flag) else select(., everything())} %>% 
    collect()
  
  # Shorten all columns with the word "Variation" to "Var"
  df_cvr_dwnld$variant <- gsub("Variation", "Var", df_cvr_dwnld$variant)
  
  df_cvr_conting <- df_cvr_dwnld %>% # Contingency table for significance calculation
    select(entity_id, experiment_id, target_group, variant, transactions, shop_list_sessions, shop_menu_sessions, checkout_sessions, checkout_transaction) # These columns can be used to calculate CVR2, CVR3, and mCVR4
  
  return(list(df_cvr_dwnld, df_cvr_conting))
}

# Call the function
# ***FUNCTION CALLING***
df_cvr_dwnld <- contingency_tbl_func("No", surge_event_indicator, combined_tg, exp_id_var)[[1]] # We need the "overall" here, not "per_day" cuz we are not calculating significance, so use "No"
df_cvr_conting <- contingency_tbl_func("No", surge_event_indicator, combined_tg, exp_id_var)[[2]] # We need the "overall" here, not "per_day" cuz we are not calculating significance, so use "No"

# Change the structure of the df_overall table such that it shows the KPIs as rows and the variant + target groups as columns
df_cvr_dwnld_reshaped <- recast(df_cvr_dwnld, entity_id + experiment_id + variable ~ target_group + variant, id.var = c("entity_id", "experiment_id", "target_group", "variant"))

# Drop the surge_event_flag column so that it does not affect the calculation of the deltas (we'll re-add it later), then convert all columns after the grouping columns to numeric
df_cvr_dwnld_reshaped <- df_cvr_dwnld_reshaped %>% 
  filter(variable != "surge_event_flag") %>% 
  mutate_at(colnames(.)[grep("Variation|Var|Control", colnames(.))], as.numeric)

# Calculate the deltas for df_cvr_dwnld
df_cvr_dwnld_reshaped <- delta_calc_func(df_cvr_dwnld_reshaped)

# Re-order the columns and choose the relevant ones
df_cvr_dwnld_reshaped <- df_cvr_dwnld_reshaped[, col_order]

##-----------------------------------------------------END OF STEP 9-----------------------------------------------------##

# Step 10: Calculate the significance of CVR2, CVR3, and mCVR4. Create a function that calculates the p-values of CVR2 and CVR3 (several steps involved)

# First, change the target_group column to a factor column
df_cvr_conting <- df_cvr_conting %>% 
  mutate_at(.vars = vars(target_group), ~ factor(., levels = combined_tg, ordered = TRUE))

# Second, create a function that calculates the p-values based on the entity_id, experiment_id, target group, variant, and CVR metric of interest
pval_calc_cvr_func <- function(tg, variation, cvr_metric, cvr_df, exp_id) {
  cvr_df <- cvr_df %>% 
    filter(target_group == tg, variant %in% c("Control", variation), experiment_id == exp_id) %>% 
    {if (cvr_metric == "CVR2") 
      select(., entity_id, experiment_id, target_group, variant, transactions, shop_list_sessions) 
      else if (cvr_metric == "CVR3") 
        select(., entity_id, experiment_id, target_group, variant, transactions, shop_menu_sessions)
      else
        select(., entity_id, experiment_id, target_group, variant, checkout_transaction, checkout_sessions)
    }
  
  if (cvr_metric == "CVR2") {
    return(round(prop.test(x = cvr_df$transactions, n = cvr_df$shop_list_sessions, alternative = "two.sided", correct = FALSE)$p.value, 4)[[1]])
  } else if (cvr_metric == "CVR3") {
    return(round(prop.test(x = cvr_df$transactions, n = cvr_df$shop_menu_sessions, alternative = "two.sided", correct = FALSE)$p.value, 4)[[1]])
  } else if (cvr_metric == "mCVR4") {
    return(round(prop.test(x = cvr_df$checkout_transaction, n = cvr_df$checkout_sessions, alternative = "two.sided", correct = FALSE)$p.value, 4)[[1]])
  }
}

# Third, write a function that creates empty data frames that will later be populated with p-values for a specific variant
pval_tbl_cvr_func <- function(cvr_conting, cvr_dwnld, num_of_cvr_metrics, variation, var_pair_num, exp_id){
  # Create an empty dataframe
  df_pval_tbl <- data.frame(matrix(ncol = nlevels(cvr_conting$target_group) + 3, nrow = num_of_cvr_metrics)) # of columns is the # of target groups + 3 for entity_id, experiment_id, and var_name. # of rows is the # of KPIs

  # Get the col numbers of the relevant columns
  index_kpis_col <- grep("entity_id|experiment_id", colnames(cvr_conting)) # The column number that contains the KPIs
  
  # Name the columns
  colnames(df_pval_tbl) <- c(colnames(df_overall_reshaped)[index_kpis_col], "variable", levels(cvr_conting$target_group)) # Levels gives us the number of factors
  
  # Populate the columns
  df_pval_tbl$entity_id <- unique(cvr_conting$entity_id)
  df_pval_tbl$experiment_id <- unique(cvr_conting$experiment_id)
  df_pval_tbl$variable <- colnames(cvr_dwnld)[grep("CVR", colnames(cvr_dwnld))]
  
  # Call the row names CVR(x) (i.e., make them similar to the row values of the first column)
  row.names(df_pval_tbl) <- df_pval_tbl[, "variable"]
  
  # Populate the empty table with p-values calculated through the Z-test for proportions (http://www.sthda.com/english/wiki/two-proportions-z-test-in-r)
  for (j in levels(cvr_conting$target_group)) {
    for (i in row.names(df_pval_tbl)) {
      df_pval_tbl[i,j] <- pval_calc_cvr_func(j, variation, i, df_cvr_conting, exp_id) # df_cvr_conting is the actual dataframe
    }
  }
  
  df_pval_tbl <- df_pval_tbl %>% 
    mutate(Var_Pair = paste0("V", var_pair_num, "_Control"))
  
  return(df_pval_tbl)
}

# ***FUNCTION CALLING***
df_pval_cvr_v9_c <- pval_tbl_cvr_func(df_cvr_conting, df_cvr_dwnld, num_cvr_metric, var_of_concern, substr_right(var_of_concern, 1), exp_id_var) # 2 for CVR2 and CVR3. If you have more CVRs or proportions, increase the number

##-----------------------------------------------------END OF STEP 10-----------------------------------------------------##

# Step 11: Combine the p-values of V1_C, V2_C, etc. and display the final result (p-values of the business KPIs)
df_pval_cvr_all <- df_pval_cvr_v9_c # Use rbind if you have multiple dataframes

# Re-order the columns
df_pval_cvr_all <- df_pval_cvr_all[, c("Var_Pair", colnames(df_pval_v9_c)[1:(length(df_pval_v9_c)-1)])]

# Rename the last column to P-Val instead of TG!_TG2
colnames(df_pval_cvr_all)[ncol(df_pval_cvr_all)] = "P_Val"

# Delete the row names as we no longer need them
row.names(df_pval_cvr_all) <- NULL

# Add the p-values to df_cvr_dwnld_reshaped
df_cvr_dwnld_reshaped <- df_cvr_dwnld_reshaped %>% 
  left_join(df_pval_cvr_all %>% select(-Var_Pair), by = c("entity_id", "experiment_id", "variable"))

##-----------------------------------------------------END OF STEP 11-----------------------------------------------------##

# Step 12: Display the final results (business KPIs + CVRs + deltas)

# Pick the columns that you want to display in the final output
# Step 1: Remove the unnecessary rows from df_overall_reshaped
row_selection_biz_kpis <- df_overall_reshaped$variable[grep("adj|corr_factor|avg", df_overall_reshaped$variable)]

full_results <- df_overall_reshaped %>% 
  filter(variable %in% row_selection_biz_kpis) %>% # Choose the desired rows
  left_join(df_pval_biz_kpis_all %>% select(-Var_Pair), by = c("entity_id", "experiment_id", "variable")) %>% 
  rbind(df_cvr_dwnld_reshaped)

# Add a column that specifies the type of event we are filtering for
full_results <- add_column(full_results, surge_event_indicator, .before = "variable")

# Add a column counter that you can order by when you upload the data frame to BQ
full_results$counter <- seq(1, nrow(full_results), 1)

##-----------------------------------------------------END OF STEP 12-----------------------------------------------------##

# Step 12.1: Upload the data frames containing the results to GBQ

# Full results of the business KPIs
bq_perform_upload(paste0(project_id_data, ".", data_set, ".", bq_tbl_name), values = full_results, fields = full_results, 
                  write_disposition = "WRITE_TRUNCATE", create_disposition = "CREATE_IF_NEEDED")

##-----------------------------------------------------END OF STEP 12.1-----------------------------------------------------##

# Step 12.2: Plot the order distribution by distance (V9 Vs. Control During Surge Events)
## Create a function that pulls the raw orders
raw_orders_func <- function(raw_ord_df, tg_incl_filter_var, tg, surge_flag, exp_id) {
  df_temp <- raw_ord_df %>%
    {if (surge_flag == "both") select(., everything()) else if (surge_flag == "surge_event") filter(., surge_event_flag == "surge_event") else if (surge_flag == "non_surge_event") filter(., surge_event_flag == "non_surge_event")} %>% 
    filter(experiment_id == exp_id & target_group %in% tg_incl_filter_var & variant %in% c("Control", variation_not_considered)) %>% 
    mutate(target_group = tg) %>% 
    collect()
}

df_raw_orders_for_plot <- raw_orders_func(df_ord, target_group_names, combined_tg, surge_event_indicator, exp_id_var)

## Create the plot
### Distance bins
df_raw_orders_for_plot$delivery_distance_m_bins <- cut(x = df_raw_orders_for_plot$delivery_distance_m, 
                                                       breaks = seq(from = 0, to = ceiling(max(df_raw_orders_for_plot$delivery_distance_m)), by = 50),
                                                       ordered_result = TRUE,
                                                       right = FALSE,
                                                       include.lowest = TRUE)

### Create a data frame containing the distance bins, AFV, and variant
df_agg_for_plot <- df_raw_orders_for_plot %>%
  select(variant, gfv_local, delivery_distance_m_bins, dps_delivery_fee_local, order_id) %>% 
  group_by(variant, delivery_distance_m_bins) %>% 
  summarise(AFV_by_distance_bin = sum(gfv_local, na.rm = TRUE) / n_distinct(order_id),
            Num_Orders = n_distinct(order_id),
            Avg_Df_by_distance_bin = sum(dps_delivery_fee_local) / n_distinct(order_id)) %>% 
  filter(!is.na(delivery_distance_m_bins))

corr_coeff_df_afv <- cor(df_agg_for_plot$Avg_Df_by_distance_bin, df_agg_for_plot$AFV_by_distance_bin)

gfv_distribution_by_distance <- ggplot(data = df_agg_for_plot, aes(x = delivery_distance_m_bins, y = AFV_by_distance_bin, fill = variant, alpha = 0.4)) +
  geom_col(position = "identity") +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  labs(title = "AFV Distribution by Distance V9 (Surge MOV) Against Control (Surge DF) During Surge Events")

order_distribution_by_distance <- ggplot(data = df_agg_for_plot, aes(x = delivery_distance_m_bins, y = Num_Orders, fill = variant, alpha = 0.4)) +
  geom_col(position = "identity") +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  labs(title = "Order Distribution by Distance V9 (Surge MOV) Against Control (Surge DF) During Surge Events")

ggpubr::ggarrange(gfv_distribution_by_distance, order_distribution_by_distance, align = "v", nrow = 2, ncol = 1)

##-----------------------------------------------------END OF STEP 12.2-----------------------------------------------------##

# TSW Analysis
# Step 13: Create a function that does the data aggregation for the different target groups (remove "variant" from the grouping variables)

pre_post_test_results_func <- function(raw_ord_df, tg_incl_filter_var, tg, per_day, surge_flag, exp_id, period) { # period refers to "Central" --> Can take either "pre" or "post"
  # Filter for the orders under the right experiment and drop the transition day
  df_temp <- raw_ord_df %>%
    {if (surge_flag == "both") select(., everything()) else if (surge_flag == "surge_event") filter(., surge_event_flag == "surge_event") else if (surge_flag == "non_surge_event") filter(., surge_event_flag == "non_surge_event")} %>% 
    filter(experiment_id == exp_id & target_group %in% tg_incl_filter_var & variant != variation_not_considered, !(created_date %in% c(transition_day, date_exclusion))) %>% 
    mutate(target_group = tg)
  
  # Filter for the right dates if it is the control group's experiment (i.e., Central) in the pre-post test
  if (exp_id == central_pre_post_exp_id) {
    df_temp <- df_temp %>% 
      {
        if (period == "pre") filter(., created_date <= end_date_pre_control_experiment) 
        else if (period == "post") filter(., created_date >= start_date_post_control_experiment)
      }
  }
  
  df_temp <- df_temp %>% 
    {if (per_day == "Yes") group_by(., entity_id, experiment_id, created_date, target_group) else group_by(., entity_id, experiment_id, target_group)} %>% 
    summarise(tot_orders = n_distinct(order_id),
              avg_travel_time = mean(dps_travel_time, na.rm = TRUE),
              avg_delivery_dist = mean(delivery_distance_m / 1000, na.rm = TRUE),
              avg_fleet_delay = mean(dps_mean_delay, na.rm = TRUE),
              avg_delivery_time = mean(actual_DT, na.rm = TRUE)) %>%
    {if (per_day == "Yes") arrange(., entity_id, experiment_id, created_date, target_group) else arrange(., entity_id, experiment_id, target_group)} %>% 
    collect()
  
  # Re-organize the columns to be in the correct order
  kpis <- c(
    # Total KPIs
    "tot_orders",
    
    # Logistical KPIs
    "avg_travel_time", "avg_delivery_dist", "avg_fleet_delay", "avg_delivery_time")
  
  df_temp <- df_temp %>% 
    {if (per_day == "Yes") select(., entity_id, experiment_id, created_date, target_group, all_of(kpis)) else select(., entity_id, experiment_id, target_group, all_of(kpis))} 
  
  # Adjust the formatting of the columns
  df_temp <- df_temp %>%
    mutate_at("tot_orders", as.integer) %>% 
    mutate_if(is.numeric, round, 4) %>% 
    mutate(period = period)
}

##-----------------------------------------------------END OF STEP 13-----------------------------------------------------##

# Step 14: Overall Results Pre vs. Post
## Aggregated
control_pre_post_overall_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "both", central_pre_post_exp_id, "pre"), # Central (Pre)
                                      pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "both", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "both", tsw_pre_exp_id, "pre"),
                                        pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "both", tsw_post_exp_id, "post")) # TSW (Pre)

control_variation_pre_post_overall_agg <- rbind(control_pre_post_overall_agg, variation_pre_post_overall_agg)

## Per Day
control_pre_post_overall_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "both", central_pre_post_exp_id, "pre"), # Central (Pre)
                                          pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "both", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "both", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                            pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "both", tsw_post_exp_id, "post")) # TSW (Post)

control_variation_pre_post_overall_per_day <- rbind(control_pre_post_overall_per_day, variation_pre_post_overall_per_day)

##-----------------------------------------------------END OF STEP 14-----------------------------------------------------##

# Step 15: Overall Results Pre vs. Post (During Surge Events)
## Aggregated
control_pre_post_overall_surge_event_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "surge_event", central_pre_post_exp_id, "pre"), # Central (Pre)
                                                  pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "surge_event", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_surge_event_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                    pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "surge_event", tsw_post_exp_id, "post")) # TSW (Post)

control_variation_pre_post_overall_surge_event_agg <- rbind(control_pre_post_overall_surge_event_agg, variation_pre_post_overall_surge_event_agg)

## Per Day
control_pre_post_overall_surge_event_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "surge_event", central_pre_post_exp_id, "pre"), # Central (Pre)
                                                      pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "surge_event", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_surge_event_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                        pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "surge_event", tsw_post_exp_id, "post")) # TSW (Post)

control_variation_pre_post_overall_surge_event_per_day <- rbind(control_pre_post_overall_surge_event_per_day, variation_pre_post_overall_surge_event_per_day)

##-----------------------------------------------------END OF STEP 15-----------------------------------------------------##

# Step 16: Overall Results Pre vs. Post (During Non-Surge Events)
## Aggregated
control_pre_post_overall_non_surge_event_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", central_pre_post_exp_id, "pre"), # Central (Pre)
                                                      pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_non_surge_event_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                        pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", tsw_post_exp_id, "post")) # TSW (Post)

control_variation_pre_post_overall_non_surge_event_agg <- rbind(control_pre_post_overall_non_surge_event_agg, variation_pre_post_overall_non_surge_event_agg)

## Per Day
control_pre_post_overall_non_surge_event_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", central_pre_post_exp_id, "pre"), # Central (Pre)
                                                          pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", central_pre_post_exp_id, "post")) # Central (Post)

variation_pre_post_overall_non_surge_event_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                            pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", tsw_post_exp_id, "post")) # TSW (Post)

control_variation_pre_post_overall_non_surge_event_per_day <- rbind(control_pre_post_overall_non_surge_event_per_day, variation_pre_post_overall_non_surge_event_per_day)

##-----------------------------------------------------END OF STEP 16-----------------------------------------------------##

# Step 17: Overall Results Pre vs. Post (Treatment Only - Surge vs. Non-Surge)
variation_pre_post_overall_non_surge_event_agg <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                        pre_post_test_results_func(df_ord, target_group_names, combined_tg, "No", "non_surge_event", tsw_post_exp_id, "post")) # TSW (Post)

variation_pre_post_overall_non_surge_event_per_day <- rbind(pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", tsw_pre_exp_id, "pre"), # TSW (Pre)
                                                            pre_post_test_results_func(df_ord, target_group_names, combined_tg, "Yes", "non_surge_event", tsw_post_exp_id, "post")) # TSW (Post)

##-----------------------------------------------------END OF STEP 17-----------------------------------------------------##

# Step 18: Plot the results

avg_delivery_time_plot <- ggplot(data = control_variation_pre_post_overall_surge_event_per_day, aes(x = created_date, y = avg_delivery_time, color = paste0(experiment_id, " | ", period))) +
  geom_line() +
  geom_point() +
  labs(color = "Exp ID | Period", title = "Overall Results During Surge Events Only - Pre Vs. Post - Avg Delivery Time")
  
avg_p_d_dist_plot <- ggplot(data = control_variation_pre_post_overall_surge_event_per_day, aes(x = created_date, y = avg_delivery_dist, color = paste0(experiment_id, " | ", period))) +
  geom_line() +
  geom_point() +
  labs(color = "Exp ID | Period", title = "Overall Results During Surge Events Only - Pre Vs. Post - Avg P_D Distance")

avg_tt_plot <- ggplot(data = control_variation_pre_post_overall_surge_event_per_day, aes(x = created_date, y = avg_travel_time, color = paste0(experiment_id, " | ", period))) +
  geom_line() +
  geom_point() +
  labs(color = "Exp ID | Period", title = "Overall Results During Surge Events Only - Pre Vs. Post - Avg Travel Time")


##-----------------------------------------------------END OF STEP 18-----------------------------------------------------##

# Create a function to change the variable name to a string
chng_var_name_to_string <- function(x) {
  x <- deparse(substitute(x))
  return(x)
}

# Upload Results to BQ
bq_perform_upload(paste0(project_id_data, ".", data_set, ".", chng_var_name_to_string(control_variation_pre_post_overall_agg)), 
                  values = control_variation_pre_post_overall_agg, 
                  fields = control_variation_pre_post_overall_agg, 
                  write_disposition = "WRITE_TRUNCATE", create_disposition = "CREATE_IF_NEEDED")

bq_perform_upload(paste0(project_id_data, ".", data_set, ".", chng_var_name_to_string(control_variation_pre_post_overall_surge_event_agg)), 
                  values = control_variation_pre_post_overall_surge_event_agg, 
                  fields = control_variation_pre_post_overall_surge_event_agg, 
                  write_disposition = "WRITE_TRUNCATE", create_disposition = "CREATE_IF_NEEDED")

bq_perform_upload(paste0(project_id_data, ".", data_set, ".", chng_var_name_to_string(control_variation_pre_post_overall_non_surge_event_agg)), 
                  values = control_variation_pre_post_overall_non_surge_event_agg, 
                  fields = control_variation_pre_post_overall_non_surge_event_agg, 
                  write_disposition = "WRITE_TRUNCATE", create_disposition = "CREATE_IF_NEEDED")

bq_perform_upload(paste0(project_id_data, ".", data_set, ".", chng_var_name_to_string(variation_pre_post_overall_non_surge_event_agg)), 
                  values = variation_pre_post_overall_non_surge_event_agg, 
                  fields = variation_pre_post_overall_non_surge_event_agg, 
                  write_disposition = "WRITE_TRUNCATE", create_disposition = "CREATE_IF_NEEDED")
