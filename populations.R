library(readr) # Read Rectangular Text Data
library(dplyr) # A Grammar of Data Manipulation
library(fs) # Cross-Platform File System Operations Based on 'libuv'
library(nanoparquet) # Read and Write 'Parquet' Files

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Read in population estimates for Shetland
shetland_pops <- read_rds(
  "/conf/linkage/output/lookups/Unicode/Populations/Estimates/HB2019_pop_est_5year_agegroups_1981_2023.rds"
) |>
  filter(hb2019name == "NHS Shetland") |>
  select(pop_year = year, pop) |>
  group_by(pop_year) |>
  summarise(pop = sum(pop))

write_parquet(
  shetland_pops,
  path(dir, "data", "lookups", "shetland_pops.parquet"),
  compression = "zstd"
)

rm(list = ls())
