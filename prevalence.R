library(dplyr)
library(readr)
library(ggplot2)
library(scales)
library(stringr)
library(lubridate)
library(phsopendata)
library(phsstyles)
library(arrow)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

clean_data <- read_parquet(path(dir, "data", "working", "nov_24_clean_data.parquet"))

# Find the first diagnosis date (any condition) for each patient
first_diag <- clean_data |>
  filter(EventDate >= as.Date("2021-11-01")) |>
  filter(str_starts(EventType, "Record of Diagnosis")) |>
  group_by(PatientID) |>
  summarise(
    FirstDiag = min(EventDate),
    DateOfDeath = first(DateOfDeath)
  )

# Read in population estimates for Shetland
shetland_pops <- read_parquet(path(dir, "data", "lookups", "shetland_pops.parquet"))

# Read in GP List sizes (aggregated to Shetland cluster level)
shetland_list_sizes <- read_parquet(path(dir, "data", "lookups", "shetland_list_sizes.parquet"))

# Create a tibble of monthly dates
months <- tibble(
  census_date = seq.Date(
    from = as.Date("2021-11-01"),
    to = as.Date("2024-11-01"),
    by = "month"
  )
) |>
  mutate(census_year = year(census_date))

monthly_summary <- left_join(
  first_diag,
  months,
  # Join on condition that FirstDiag is before or on census_date
  by = join_by(FirstDiag <= census_date)
) |>
  # Exclude records after death
  filter(is.na(DateOfDeath) | census_date <= floor_date(DateOfDeath, unit = "month")) |>
  group_by(census_year, census_date) |>
  summarise(
    count = n_distinct(PatientID),
    .groups = "drop"
  ) |>
  left_join(shetland_pops, by = join_by(closest(census_year >= pop_year))) |>
  left_join(shetland_list_sizes, by = join_by(closest(census_date >= Date))) |>
  mutate(
    pop_prev = count / pop,
    list_prev = count / list_pop
  )
