library(phsopendata) # Extract Open Data from opendata.nhs.scot
library(dplyr) # A Grammar of Data Manipulation
library(lubridate) # Make Dealing with Dates a Little Easier
library(fs) # Cross-Platform File System Operations Based on 'libuv'
library(nanoparquet) # Read and Write 'Parquet' Files
library(readr)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Read in GP List sizes (aggregated to Shetland cluster level)

shetland_list_sizes <- get_dataset(
  "gp-practice-populations",
  row_filters = list(HSCP = "S37000026"),
  col_select = c("Date", "PracticeCode", "Sex", "AllAges")
) |>
  filter(Sex == "All") |>
  mutate(Date = ymd(Date)) |>
  group_by(Date, PracticeCode) |>
  summarise(list_pop = sum(AllAges), .groups = "drop")

#new#
ShetlandPractices <- read_csv("/conf/LIST_analytics/Shetland/Primary Care/LTC/data/lookups/ShetlandPractices.csv")

joined_data <-left_join(shetland_list_sizes, ShetlandPractices, by='PracticeCode')

write_csv(joined_data,"/conf/LIST_analytics/Shetland/Primary Care/LTC/data/lookups/shetland_list_sizes.csv")

#write_parquet(
  #shetland_list_sizes,
  #path(dir, "data", "lookups", "shetland_list_sizes.parquet"),
  #compression = "zstd"
#)

rm(list = ls())
