if (interactive()) {
  setwd(gsub("src(.*)?", "", rstudioapi::getSourceEditorContext()$path)) 
} 

# Setup ------------------------------------------------------------------------
if (!require("pacman")) install.packages("pacman"); library(pacman)

pacman::p_load(tidytable, lubridate, tictoc, tibble, assertr)

source("../R/utils.R")

hds_clean = fread("data/hds_clean.csv")
cpd_nm = fread("data/cpd_nm.csv")
arrest_charges = fread("data/arrests_charges.csv")

# Create analysis data: one row per arrest id; corresponds to each cluster_id arrest stint
analysis_df_arrest_level = cpd_nm %>% 
  left_join(
    arrest_charges %>% select(arrest_id, arrest_date, drug_ch_not_exclusively_cannabis),
    by = "arrest_id"
  ) %>% 
  select(
    arrest_id,
    cluster_id,
    arrest_date,
    drug_ch_not_exclusively_cannabis
  )

analysis_df_arrest_level = analysis_df_arrest_level %>% 
  mutate(
    arrest_date = as.Date(arrest_date),
    arrest_date_1yr_post = arrest_date + days(365),
    arrest_date_1yr_prior = arrest_date - days(365)
  )

hds_clean = hds_clean %>%
  mutate(
    status_date = as.Date(status_date),
    status_date_1yr_post = status_date + days(365),
    status_date_1yr_prior = status_date - days(365)
  ) %>% 
  arrange(cluster_id, status_date)

# Filter arrests to table sample -----------------------------------------------
hds_clean_ever_homeless <- hds_clean %>% 
  select(cluster_id, ever_homeless) %>% 
  group_by(cluster_id) %>%
  slice(1) %>%
  ungroup()

analysis_df_arrest_level = analysis_df_arrest_level %>%
  left_join(hds_clean_ever_homeless, by = "cluster_id")

print(paste(length(unique(analysis_df_arrest_level$cluster_id)), "arrestees prior to filtering to drugnotxcannabis"))

# Only keep drug arrests not exclusively cannabis ----
analysis_df_arrest_level = analysis_df_arrest_level %>%
  filter(drug_ch_not_exclusively_cannabis == 1)

print(paste(length(unique(analysis_df_arrest_level$cluster_id)), "arrestees after filtering to drugnotxcannabis"))

# Filter to time ranges of the homelessness data ----
# Start the sample of arrests one year after the HDS data begins and end the sample
# one year before the HDS data ends
start_date_table = min(hds_clean$status_date) + days(365)
end_date_table = max(hds_clean$status_date) - days(365)

print("Filtering arrests by date:")
print(paste("First date kept in sample of arrests for table:", start_date_table))
print(paste("Last date kept in sample of arrests for table:", end_date_table))

analysis_df_arrest_level = analysis_df_arrest_level %>% 
  filter(arrest_date >= start_date_table & arrest_date <= end_date_table)

print(paste(length(unique(analysis_df_arrest_level$cluster_id)), "arrestees after filtering by HDS dates"))

# Generate ever homeless within one year of arrest ----
analysis_df_arrest_level = analysis_df_arrest_level %>%
  mutate(in_hds = ifelse(cluster_id %in% hds_clean$cluster_id, 1, 0))

hds_clean_arrestees_homeless = hds_clean %>% 
  filter(cluster_id %in% analysis_df_arrest_level$cluster_id) %>% 
  filter(Literally_Homeless_dedup == 1)

arrests_in_hds = analysis_df_arrest_level %>% filter(in_hds == 1)

tic("Generating ever homeless within one year indicators")

arrests_in_hds = arrests_in_hds %>%
  group_by(cluster_id, arrest_date) %>% 
  mutate(
    ever_homeless_1yr_post = return_homeless_between_date_range(
      hds_df = hds_clean_arrestees_homeless,
      cluster_id_for_lookup = cluster_id,
      # start the post-period the day of arrest
      start_date = arrest_date - days(1),
      # end the post-period one year after arrest
      end_date = arrest_date_1yr_post + days(1)
    )
  ) %>% 
  ungroup()

arrests_in_hds = arrests_in_hds %>% 
  group_by(cluster_id, arrest_date) %>% 
  mutate(    
    ever_homeless_1yr_prior = return_homeless_between_date_range(
      hds_df = hds_clean_arrestees_homeless,
      cluster_id_for_lookup = cluster_id,
      # start the pre-period one year before arrest
      start_date = arrest_date_1yr_prior - days(1),
      # end the pre-period the day before arrest
      end_date = arrest_date
    )
  )

analysis_df_arrest_level = analysis_df_arrest_level %>% 
  full_join(
    arrests_in_hds %>% 
      select(cluster_id, arrest_date, ever_homeless_1yr_prior, ever_homeless_1yr_post),
    by = c("cluster_id", "arrest_date")
  )

toc()

# Save the sample
arrow::write_parquet(analysis_df_arrest_level, "output/analysis_df_arrest_level.parquet")

# Generate table ---------------------------------------------------------------
# One row per arrest timing (pre- and post- arrest)
# One column with N, one column with % homeless prior to arrest, one column
# with % homeless after arrest
n_analysis = nrow(analysis_df_arrest_level)

arrests_hds_table = tibble(
  arrest_timing = c("prior_arrest", "post_arrest"),
  n_homeless = c(
    sum(analysis_df_arrest_level$ever_homeless_1yr_prior, na.rm = TRUE),
    sum(analysis_df_arrest_level$ever_homeless_1yr_post, na.rm = TRUE)
  ),
  n_total = rep(n_analysis, 2),
  pct_homeless = c(
    sum(analysis_df_arrest_level$ever_homeless_1yr_prior, na.rm = TRUE)/n_analysis,
    sum(analysis_df_arrest_level$ever_homeless_1yr_post, na.rm = TRUE)/n_analysis
  )
)

# Save table
readr::write_csv(arrests_hds_table, "output/arrests_hds_table.csv")
