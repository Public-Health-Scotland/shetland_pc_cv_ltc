library(phsopendata)
library(dplyr)
library(lubridate)
library(fs)
library(nanoparquet)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Read in GP List sizes (aggregated to Shetland cluster level)

shetland_list_sizes <- get_dataset(
  "gp-practice-populations",
  row_filters = list(HSCP = "S37000026"),
  col_select = c("Date", "PracticeCode", "Sex", "AllAges")
) |>
  filter(Sex == "All") |>
  mutate(Date = ymd(Date)) |>
  group_by(Date) |>
  summarise(list_pop = sum(AllAges), .groups = "drop")

write_parquet(
  shetland_list_sizes,
  path(dir, "data", "lookups", "shetland_list_sizes.parquet"),
  compression = "zstd"
)

rm(list = ls())
