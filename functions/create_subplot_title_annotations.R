create_subplot_title_annotations <- function(
  num_rows,
  titles_vector,
  subplot_margin
) {
  num_plots <- length(titles_vector)
  num_cols <- ceiling(num_plots / num_rows)
  annotations <- list()

  # Calculate the effective height of each row, including its internal margin
  # The `subplot_margin` is the fraction of the plot area, so we need to adjust for that.
  # A simpler way is to divide the total grid height by the number of rows.
  height_per_row_effective <- 0.8 / num_rows

  # A small offset to place the title just above the subplot area
  title_y_offset_above_row <- 0 # Adjust this value if titles are too close/far from subplots

  for (i in seq_along(titles_vector)) {
    row_idx <- ceiling(i / num_cols)
    col_idx <- (i - 1) %% num_cols + 1

    # Calculate x paper coordinate (centre of the column)
    x_coord <- (col_idx - 0.5) / num_cols

    # Calculate y paper coordinate for the title
    # Start from the top of the grid and step down by the effective height per row
    # The (row_idx - 1) ensures the first row doesn't step down.
    # Add the title_y_offset_above_row to place it slightly above the row.
    y_coord <- 1 - row_idx * height_per_row_effective + title_y_offset_above_row

    annotations[[length(annotations) + 1]] <- list(
      x = x_coord,
      y = y_coord,
      text = paste("<b>Practice", titles_vector[i], "</b>"),
      showarrow = FALSE,
      xref = "paper",
      yref = "paper",
      xanchor = "center",
      yanchor = "bottom",
      font = list(size = 12)
    )
  }
  return(annotations)
}
