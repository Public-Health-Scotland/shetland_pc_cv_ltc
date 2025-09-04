make_dt <- function(
  shared_data,
  main_var,
  other_vars = NULL,
  main_name = main_var,
  other_names = other_vars,
  keep_cols = NULL,
  percentage = FALSE
) {
  # Only show relevant columns
  columns_to_show <- c("census_date", "PracticeID", main_var, other_vars, keep_cols)
  indices_to_hide <- which(!(names(shared_data$data()) %in% columns_to_show))

  # Create the DT datatable
  dt <- DT::datatable(
    shared_data,
    # Nicer column names
    colnames = c(
      "Practice ID" = "PracticeID",
      "Month" = "census_date",
      stats::setNames(main_var, main_name),
      stats::setNames(other_vars, other_names)
    ),
    rownames = FALSE,
    filter = "none",
    options = list(
      columnDefs = list(
        # Hide the columns (0-indexed)
        list(visible = FALSE, targets = indices_to_hide - 1)
      )
    )
  )

  if (percentage) {
    dt <- DT::formatPercentage(dt, main_name, digits = 0)
  }

  return(dt)
}
