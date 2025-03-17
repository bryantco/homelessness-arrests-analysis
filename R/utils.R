#' Function that returns "ever homeless" for a given cluster_id between a 
#' specified date range
#'
#' @param hds_df 
#' @param cluster_id_lookup 
#' @param start_date 
#' @param end_date 
#'
#' @return NA if cluster_id not observed in HDS data for the given date range, 
#' otherwise "ever homeless" status for the cluster_id
#' @export
#'
#' @examples
return_homeless_between_date_range = function(
    hds_df,
    cluster_id_for_lookup,
    start_date, 
    end_date
) {
  hds_dt = data.table::setDT(hds_df)
  
  hds_df_filtered = hds_dt[cluster_id == cluster_id_for_lookup & status_date > start_date & status_date < end_date]
  
  if (nrow(hds_df_filtered) == 0) {
    return(NA_real_)
  } else {
    return(max(hds_df_filtered$literally_homeless))
  }
}

#' Function that generates dates for months before or after the arrest date
#'
#' @param arrest_level_df 
#' @param after boolean; if TRUE, generate dates for months after the arrest date,
#' if FALSE, generate dates for months before the arrest date
#' @param number_of_months 
#'
#' @return arrest_level_df with new columns for each month before or after the arrest
#' @export
#'
#' @examples
generate_monthly_arrest_dates = function(
    arrest_level_df,
    after = TRUE,
    number_of_months
) {
  if (after) {
    for (i in 1:number_of_months) {
      arrest_date_event_time_var_str = paste0("arrest_date_event_time_", i, "mo")
      arrest_level_df = arrest_level_df %>%
        mutate(
          !!sym(arrest_date_event_time_var_str) := arrest_date + days(30 * i)
        )
    }
  } else {
    for (i in 1:number_of_months) {
      arrest_date_event_time_var_str = paste0("arrest_date_event_time_", -i, "mo")
      arrest_level_df = arrest_level_df %>%
        mutate(
          !!sym(arrest_date_event_time_var_str) := arrest_date - days(30 * i)
        )
    }
  }
  
  return(arrest_level_df)
}