library(ggplot2)
library(stringr)

data_to_plot <- quarterly_event_type |>
  filter(quarter_start >= as.Date("2021-11-01")) |>
  mutate(PracticeID = factor(PracticeID)) |>
  complete(PracticeID, quarter_start, DerivedEventType, fill = list(event_type_proportion = 0.0)) |>
  mutate(show_legend_flag = !duplicated(DerivedEventType))

# This is the plot in ggplot, just want to replicate in plotly...
data_to_plot |>
  ggplot(aes(y = event_type_proportion,  x = quarter_start, group = DerivedEventType, colour = DerivedEventType)) +
  geom_line() +
  facet_wrap("PracticeID")


library(plotly)

create_subplot_title_annotations <- function(num_rows, titles_vector, subplot_margin) {
  num_plots <- length(titles_vector)
  num_cols <- ceiling(num_plots / num_rows)
  annotations <- list()

  # Calculate the effective height of each row, including its internal margin
  # The `subplot_margin` is the fraction of the plot area, so we need to adjust for that.
  # A simpler way is to divide the total grid height by the number of rows.
  height_per_row_effective <- 0.85 / num_rows

  # A small offset to place the title just above the subplot area
  title_y_offset_above_row <- 0.02 # Adjust this value if titles are too close/far from subplots

  for (i in seq_along(titles_vector)) {
    row_idx <- ceiling(i / num_cols)
    col_idx <- (i - 1) %% num_cols + 1

    # Calculate x paper coordinate (center of the column)
    x_coord <- (col_idx - 0.5) / num_cols

    # Calculate y paper coordinate for the title
    # Start from the top of the grid and step down by the effective height per row
    # The (row_idx - 1) ensures the first row doesn't step down.
    # Add the title_y_offset_above_row to place it slightly above the row.
    y_coord <- 1 - (row_idx - 1) * height_per_row_effective + title_y_offset_above_row

    annotations[[length(annotations) + 1]] <- list(
      x = x_coord,
      y = y_coord,
      text = paste("Practice", titles_vector[i]),
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

subplot_margin_value <- 0.03
nrows <- 3

subplot_title_annotations <- create_subplot_title_annotations(
  num_rows = nrows, # Pass the number of rows for your subplot grid
  titles_vector = unique(pull(data_to_plot, PracticeID)), # Pass the vector of unique practice IDs
  subplot_margin = subplot_margin_value
)

data_to_plot |>
  group_by(PracticeID) %>%
  group_map(
    \(practice_data, practice_id) {
      practice_id <- paste("Practice ", practice_id)

      plot_ly(
        data = practice_data,
        x = ~quarter_start,
        y = ~event_type_proportion,
        color = ~DerivedEventType,
        type = "scatter",
        mode = "lines+markers",
        hoverinfo = "text",
        text = ~ paste(
          "Practice:", PracticeID,
          "<br>Quarter:", quarter_start,
          "<br>Percentage:", round(event_type_proportion * 100, 1), "%"
        ),
        legendgroup = ~DerivedEventType,
        name = ~DerivedEventType,
        showlegend = ~any(show_legend_flag)
      ) |>
        layout(
          title = list(text = practice_id, font = list(size = 12)),
          hovermode = 'x',
          showlegend = FALSE,
          # Set titles to empty strings to prevent them from showing on individual subplots.
          # The shared titles will be applied by the main layout.
          xaxis = list(title = ""),
          yaxis = list(title = "")
        )
    },
    .keep = TRUE
  ) |>
  subplot(
    nrows = 4,
    shareX = TRUE,
    shareY = TRUE,
    margin = subplot_margin_value
  ) |>
  layout(
    # Overall plot title
    title = list(text = str_wrap("Quarterly Type of Review by Practice", 25)),
    showlegend = TRUE,
    legend = list(
      title = list(text = "Review Type"),
      orientation = "h", # Place legend horizontally.
      x = 0.5, y = 0.1, # Position legend below the plot area.
      xanchor = "center"
    ),
    annotations = c(list(
      # X-axis label
      list(
        x = 0.5, # Centre horizontally
        y = 0.1, # Position below the plots (adjust as needed)
        text = "Quarter",
        showarrow = FALSE,
        xref = "paper",
        yref = "paper",
        xanchor = "center",
        yanchor = "bottom"
      ),
      # Y-axis label
      list(
        x = -0.04, # Position to the left of the plots (adjust as needed)
        y = 0.6, # Centre vertically
        text = "Review Type Percentage",
        showarrow = FALSE,
        xref = "paper",
        yref = "paper",
        xanchor = "right",
        yanchor = "middle",
        textangle = -90 # Rotate text for vertical label
      )),
      subplot_title_annotations
    )
  )

