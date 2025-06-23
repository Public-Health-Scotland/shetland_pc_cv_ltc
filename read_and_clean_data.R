library(dplyr) # A Grammar of Data Manipulation
library(tidyr) # Tidy Messy Data
library(lubridate) # Make Dealing with Dates a Little Easier
library(readr) # Read Rectangular Text Data
library(nanoparquet) # Read and Write 'Parquet' Files
library(fs) # Cross-Platform File System Operations Based on 'libuv'
library(readxl) # Read '.xlxs' files

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Print the files in the folder to check
dir_ls(path(dir, "data", "raw")) |>
  path_file()


#data_file_name <- 2025-04-22 - LIST - CV LTC.csv

raw_data <- read_csv(
  file = path(dir, "data", "raw", "2025-04-22 - LIST - CV LTC.csv"),
  col_types = cols(
    PatientID = col_integer(),
    PracticeID = col_integer(),
    EventCode = col_character(),
    DayOfBirth = col_integer(),
    MonthOfBirth = col_integer(),
    EventDate = col_date(format = "%d/%m/%Y"),
    DateOfDeath = col_date(format = "%d/%m/%Y"),
    EventType = col_character()
  )
)


cleaned_filtered <- raw_data |>
  # 1899 dates are in a different format so will now be NA
  drop_na(EventDate) |>
  mutate(EventYear = year(EventDate)) |>
  # One record is 1900-01-01 (obvious outlier) other records are in the future
  # We only need the latest ~4 years
  filter(between(EventYear, 1901, year(today())))


# Adding a column where EventType = Main Address Off Shetland with a date
cleaned_filtered <- cleaned_filtered %>%
  mutate(
    LeftShetlandDate = if_else(
      EventType == "Main Address Off Shetland",
      EventDate,
      NA
    )
  ) %>%
  group_by(PatientID) %>%
  mutate(
    LeftShetlandDate = max(LeftShetlandDate, na.rm = TRUE),
    LeftShetlandDate = na_if(LeftShetlandDate, as.Date(-Inf))
  ) %>% #clean up when there is no date for LeftShetlandDate
  ungroup() %>%
  mutate(LeftDate = if_else(EventType == "Left Practice", EventDate, NA)) %>%
  group_by(PatientID, PracticeID) %>%
  mutate(
    LeftDate = max(LeftDate, na.rm = TRUE),
    LeftDate = na_if(LeftDate, as.Date(-Inf))
  ) %>% #clean up when there is no date for LeftDate
  ungroup()


write_parquet(
  cleaned_filtered,
  path(dir, "data", "working", "apr_25_clean_data.parquet"), # will need to update every time a new extract
  compression = "zstd"
)

# Clean the environment
rm(list = ls())
