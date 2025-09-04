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

# File names - UPDATE THESE
data_file_name <- "2025-07 - LIST - Pharmacotherapy - With Lookup Tables.zip"
lookup_file_name <- "2025-07 - LIST - Pharmacotherapy - Lookup Tables.xlsx"

# File paths
data_file_path <- path(dir, "data", "raw", data_file_name)
lookup_file_path <- path(dir, "data", "raw", lookup_file_name)

derived_event_type_lookup <- read_excel(
  path = lookup_file_path,
  range = "A19:B25",
  col_types = c("text", "numeric")
)

derived_staff_type_lookup <- read_excel(
  path = lookup_file_path,
  range = "A63:B66",
  col_types = c("text", "numeric")
)

raw_data <- unzip(zipfile = data_file_path, exdir = tempdir())[1] |>
  read_csv(
    col_types = cols(
      PatientID = col_integer(),
      PracticeID = col_integer(),
      DayOfBirth = col_integer(),
      MonthOfBirth = col_integer(),
      DateOfDeath = col_date(format = "%d/%m/%Y"),
      EventDate = col_date(format = "%d/%m/%Y"),
      EventCode = col_skip(),
      EventDescriptionID = col_skip(),
      DerivedEventTypeID = col_integer(),
      StaffTypeID = col_skip(),
      DerivedStaffTypeID = col_integer(),
      DateLeftShetland = col_date(format = "%d/%m/%Y")
    ),
    lazy = TRUE
  ) |>
  left_join(derived_event_type_lookup, by = join_by(DerivedEventTypeID)) |>
  left_join(derived_staff_type_lookup, by = join_by(DerivedStaffTypeID)) |>
  select(-DerivedEventTypeID, -DerivedStaffTypeID)


# Write out cleaned data
write_parquet(
  raw_data,
  path(dir, "data", "working", "june_25_pharmacotherapy.parquet"),
  compression = "zstd"
)

rm(list = ls())
