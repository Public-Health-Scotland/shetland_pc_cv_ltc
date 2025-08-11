library(dplyr) # A Grammar of Data Manipulation
library(tidyr) # Tidy Messy Data
library(lubridate) # Make Dealing with Dates a Little Easier
library(readr) # Read Rectangular Text Data
library(nanoparquet) # Read and Write 'Parquet' Files
library(fs) # Cross-Platform File System Operations Based on 'libuv'
library(readxl) # Read '.xlsx' files

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Print the files in the folder to check
dir_ls(path(dir, "data", "raw")) |>
  path_file()

# File name - UPDATE THIS
data_file_name <- "2025-05 - LIST - CV LTC.xlsx"
# File path
data_file_path <- path(dir, "data", "raw", data_file_name)

raw_data <- read_xlsx(
  path = data_file_path,
  col_types = c(
    "numeric",
    "numeric",
    "text",
    "numeric",
    "numeric",
    "date",
    "date",
    "text"
  )
  # code for using csv files
  # raw_data <-read_csv(
  #  path = data_file_path,
  #   cols(
  #   PatientID = col_integer(),
  #   PracticeID = col_integer(),
  #   EventCode = col_character(),
  #   DayOfBirth = col_integer(),
  #   MonthOfBirth = col_integer(),
  #   EventDate = col_date(format = "%d/%m/%Y"),
  #   DateOfDeath = col_date(format = "%d/%m/%Y"),
  #   EventType = col_character()
  # )
)

cleaned_filtered <- raw_data |>
  # 1899 dates are in a different format so will now be NA
  drop_na(EventDate) |>
  mutate(
    across(c(EventDate, DateOfDeath), as.Date),
    EventYear = year(EventDate)
  ) |>
  # One record is 1900-01-01 (obvious outlier) other records are in the future
  # We only need the latest ~4 years
  filter(between(EventYear, 1901, year(today())))

# Adding a colums where EventType = Main Address Off Shetland,Left Practice and Joined Practice with a date
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
  ) %>% # clean up when there is no date for LeftShetlandDate
  ungroup() %>%
  mutate(LeftDate = if_else(EventType == "Left Practice", EventDate, NA)) %>%
  group_by(PatientID, PracticeID) %>%
  mutate(
    LeftDate = max(LeftDate, na.rm = TRUE),
    LeftDate = na_if(LeftDate, as.Date(-Inf))
  ) %>% # clean up when there is no date for LeftDate
  ungroup() %>%
  mutate(
    JoinedDate = if_else(EventType == "Joined Practice", EventDate, NA)
  ) %>%
  group_by(PatientID, PracticeID) %>%
  mutate(
    JoinedDate = max(JoinedDate, na.rm = TRUE),
    JoinedDate = na_if(JoinedDate, as.Date(-Inf))
  ) %>% # clean up when there is no date for JoinedDate
  ungroup() |>
  filter(
    EventType != "Main Address Off Shetland",
    EventType != "Left Practice",
    EventType != "Joined Practice"
  )

write_parquet(
  cleaned_filtered,
  path(dir, "data", "working", "may_25_clean_data.parquet"), # will need to update every time a new extract
  compression = "zstd"
)

# Clean the environment
rm(list = ls())
