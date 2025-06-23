make_dt <- function(
  shared_data,
  var,
  name,
  keep_cols = NULL,
  percentage = FALSE
) {
  # Only show relevant columns
  columns_to_show <- c("census_date", "PracticeID", var, keep_cols)
  indices_to_hide <- which(!(names(shared_data$data()) %in% columns_to_show))

  # Create the DT datatable
  dt <- datatable(
    shared_data,
    # Nicer column names
    colnames = c(
      "Practice ID" = "PracticeID",
      "Month" = "census_date",
      setNames(var, name)
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
    dt <- formatPercentage(dt, name, digits = 0)
  }

  return(dt)
}
