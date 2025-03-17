# Setup ------------------------------------------------------------------------
if (interactive()) {
  setwd(gsub("src(.*)?", "", rstudioapi::getSourceEditorContext()$path)) 
} 

if (!require("pacman")) install.packages("pacman"); library(pacman)
pacman::p_load(tidytable, lubridate, arrow, ggplot2, tictoc)

source("../R/utils.R")

hds_clean = fread("data/hds_clean.csv")
analysis_df_arrest_level = arrow::read_parquet("output/analysis_df_arrest_level.parquet")

hds_table_sample_df = hds_clean %>% 
  filter(cluster_id %in% analysis_df_arrest_level$cluster_id)

# Create analysis df -----------------------------------------------------------
# Generate features for date 1 month after arrest, 2 months after arrest, ..., 
# 12 months after arrest
# Due to the syntax of the generate_monthly_arrest_dates function, t = 11 means
# the date window 11-12 months after arrest
analysis_df_arrest_level = generate_monthly_arrest_dates(
  arrest_level_df = analysis_df_arrest_level,
  after = TRUE,
  number_of_months = 12
)

analysis_df_arrest_level = generate_monthly_arrest_dates(
  arrest_level_df = analysis_df_arrest_level,
  after = FALSE,
  number_of_months = 12
)

analysis_df_arrest_level = analysis_df_arrest_level %>% 
  mutate(arrest_date_event_time_0mo = arrest_date)

# Generate ever homeless 1 month after arrest, 2 months after arrest, ..., 
# 12 months after arrest
arrests_in_hds = analysis_df_arrest_level %>% filter(in_hds == 1)

tic("Generating ever homeless in 1 month intervals before and after arrest")

arrests_in_hds = arrests_in_hds %>% group_by(cluster_id, arrest_date)

for (i in -11:11) {
  
  if (i >= 0) {
    arrest_date_lower_bound = sym(paste0("arrest_date_event_time_", i, "mo"))
    arrest_date_upper_bound = sym(paste0("arrest_date_event_time_", i + 1, "mo")) 
  } else {
    arrest_date_lower_bound = sym(paste0("arrest_date_event_time_", i - 1, "mo"))
    arrest_date_upper_bound = sym(paste0("arrest_date_event_time_", i, "mo"))
  }
  
  ever_homeless_month_var = sym(paste0("ever_homeless_event_time_", i, "mo"))
  
  if (i < 11) {
    arrests_in_hds = arrests_in_hds %>%
      mutate(
        !!ever_homeless_month_var := return_homeless_between_date_range(
          hds_df = hds_table_sample_df,
          cluster_id_for_lookup = cluster_id,
          # subtract 1 day from lower and upper bounds because the return_homeless_between_date_range
          # function does not include endpoints of a date interval when looking up
          # homelessness
          start_date = !!arrest_date_lower_bound - days(1),
          end_date = !!arrest_date_upper_bound + days(1)
        )
      )
  } else {
    # If at t = 11, also include the day of 12 months after arrest day in the
    # ending interval
    arrests_in_hds = arrests_in_hds %>%
      mutate(
        !!ever_homeless_month_var := return_homeless_between_date_range(
          hds_df = hds_table_sample_df,
          cluster_id_for_lookup = cluster_id,
          start_date = !!arrest_date_lower_bound - days(1),
          end_date = !!arrest_date_upper_bound + days(1)
        )
      )
  }
}

toc()

arrests_in_hds = arrests_in_hds %>% ungroup()

analysis_df_arrest_level = analysis_df_arrest_level %>% 
  left_join(
    arrests_in_hds %>%
      group_by(arrest_id) %>% 
      slice(1) %>% 
      ungroup() %>%
      select(arrest_id, starts_with("ever_homeless_event_time_")),
    by = c("arrest_id")
  ) %>% 
  select(-`arrest_date_event_time_-12mo`, -`arrest_date_event_time_12mo`)

# Save
arrow::write_parquet(analysis_df_arrest_level, "output/analysis_df_arrest_level_for_event_study.parquet")

# Pivot longer to create final analysis df ----
# One row per arrest and event time
arrest_level_df_long = analysis_df_arrest_level %>%
  pivot_longer(
    cols = starts_with("arrest_date_event_time_"),
    names_to = "event_time",
    values_to = "date"
  )

arrest_level_df_long = arrest_level_df_long %>% 
  # Generate event time; event_time = 0 means 0-1 months after arrest
  mutate(
    event_time = gsub("arrest_date_event_time_", "", event_time),
    event_time = gsub("mo", "", event_time),
    event_time = as.integer(event_time)
  )

arrest_level_df_long = arrest_level_df_long %>% 
  mutate(
    ever_homeless_event_time = case_when(
      event_time == -11 ~ `ever_homeless_event_time_-11mo`,
      event_time == -10 ~ `ever_homeless_event_time_-10mo`,
      event_time == -9 ~ `ever_homeless_event_time_-9mo`,
      event_time == -8 ~ `ever_homeless_event_time_-8mo`,
      event_time == -7 ~ `ever_homeless_event_time_-7mo`,
      event_time == -6 ~ `ever_homeless_event_time_-6mo`,
      event_time == -5 ~ `ever_homeless_event_time_-5mo`,
      event_time == -4 ~ `ever_homeless_event_time_-4mo`,
      event_time == -3 ~ `ever_homeless_event_time_-3mo`,
      event_time == -2 ~ `ever_homeless_event_time_-2mo`,
      event_time == -1 ~ `ever_homeless_event_time_-1mo`,
      event_time == 0  ~ `ever_homeless_event_time_0mo`,
      event_time == 1  ~ `ever_homeless_event_time_1mo`,
      event_time == 2  ~ `ever_homeless_event_time_2mo`,
      event_time == 3  ~ `ever_homeless_event_time_3mo`,
      event_time == 4  ~ `ever_homeless_event_time_4mo`,
      event_time == 5  ~ `ever_homeless_event_time_5mo`,
      event_time == 6  ~ `ever_homeless_event_time_6mo`,
      event_time == 7  ~ `ever_homeless_event_time_7mo`,
      event_time == 8  ~ `ever_homeless_event_time_8mo`,
      event_time == 9  ~ `ever_homeless_event_time_9mo`,
      event_time == 10 ~ `ever_homeless_event_time_10mo`,
      event_time == 11 ~ `ever_homeless_event_time_11mo`
    )
  ) %>% 
  select(-starts_with("ever_homeless_event_time_"))

# Plot -------------------------------------------------------------------------
arrest_level_df_plot = arrest_level_df_long %>% 
  group_by(event_time) %>%
  summarize(
    n_total = n(),
    homelessness_total = sum(ever_homeless_event_time, na.rm = TRUE)
  ) %>% 
  mutate(homelessness_rate = homelessness_total / n_total)

# Raw counts
ggplot(arrest_level_df_plot) + 
  geom_point(aes(x = event_time, y = homelessness_total)) + 
  ylim(0, NA) + 
  scale_x_continuous(breaks = seq(-10, 10, 2)) + 
  labs(
    x = "Months After Arrest",
    y = "Homelessness (Count)"
  ) + 
  theme_bw()

ggsave("output/event_study_homelessness_counts.png", width = 8, height = 6)

# Homelessness %
ggplot(arrest_level_df_plot) + 
  geom_point(aes(x = event_time, y = homelessness_rate)) + 
  ylim(0, NA) + 
  scale_x_continuous(breaks = seq(-10, 10, 2)) + 
  labs(
    x = "Months After Arrest",
    y = "Homelessness (%)"
  ) + 
  theme_bw()

ggsave("output/event_study_homelessness_pct.png", width = 8, height = 6)


