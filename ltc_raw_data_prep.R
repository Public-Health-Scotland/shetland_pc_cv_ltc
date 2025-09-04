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
data_file_name <- "2025-07 - LIST - CV LTC with ltc resolved events.zip"
# File path
data_file_path <- path(dir, "data", "raw", data_file_name)

raw_data <- unzip(zipfile = data_file_path)[1] |>
  read_xlsx(
    col_types = c(
      PatientID = "numeric",
      PracticeID = "numeric",
      DayOfBirth = "skip",
      MonthOfBirth = "skip",
      EventCode = "text",
      EventDate = "date",
      DateOfDeath = "date",
      EventType = "text"
    )
  )

cleaned_data <- raw_data |>
  # 1899 dates are in a different format so will now be NA
  drop_na(EventDate) |>
  mutate(across(c(EventDate, DateOfDeath), as.Date)) |>
  # One record is 1900-01-01 (obvious outlier) other records are in the future
  # We only need the latest ~4 years
  filter(between(year(EventDate), 1901, year(today()))) |>
  arrange(desc(EventDate)) |>
  # Use EventCode to highlight some specific other event types
  mutate(
    EventType = case_match(
      EventCode,
      # Using this format means we can find first or all still
      "9O41." ~ "LTC Admin - (first) LTC Invite",
      "66Z.." ~ "LTC Admin - (first) LTC Attendance",
      .default = EventType
    )
  ) |>
  select(-EventCode)

# Adding a columns where EventType = Main Address Off Shetland,Left Practice and Joined Practice with a date
left_shetland_dates <- cleaned_data |>
  filter(EventType == "Main Address Off Shetland") |>
  select(PatientID, LeftShetlandDate = EventDate) |>
  distinct(PatientID, .keep_all = TRUE)

left_practice_dates <- cleaned_data |>
  filter(EventType == "Left Practice") |>
  select(PatientID, PracticeID, LeftDate = EventDate) |>
  distinct(PatientID, PracticeID, .keep_all = TRUE)

joined_practice_dates <- cleaned_data |>
  filter(EventType == "Joined Practice") |>
  select(PatientID, PracticeID, JoinedDate = EventDate) |>
  distinct(PatientID, PracticeID, .keep_all = TRUE)

# Join new columns into main data
cleaned_filtered_data <- cleaned_data |>
  left_join(
    left_shetland_dates,
    by = "PatientID",
    relationship = "many-to-one"
  ) |>
  left_join(
    left_practice_dates,
    by = c("PatientID", "PracticeID"),
    relationship = "many-to-one"
  ) |>
  left_join(
    joined_practice_dates,
    by = c("PatientID", "PracticeID"),
    relationship = "many-to-one"
  ) |>
  filter(
    EventType != "Main Address Off Shetland",
    EventType != "Left Practice",
    EventType != "Joined Practice",
    EventType != "Date of Death"
  )

write_parquet(
  cleaned_filtered_data,
  path(dir, "data", "working", "june_25_clean_data_w_resolved.parquet"), # will need to update every time a new extract
  compression = "zstd"
)

# Clean the environment
rm(list = ls())
