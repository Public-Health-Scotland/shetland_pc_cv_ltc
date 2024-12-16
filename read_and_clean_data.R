library(dplyr)
library(tidyr)
library(lubridate)
library(readr)
library(nanoparquet)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Print the files in the folder to check
dir_ls(path(dir, "data", "raw")) |> 
  path_file()

data_file_name <- "2024-11-18 - LIST CV LTC - extract 2.zip"

raw_data <- read_csv(
  file = path(dir, "data", "raw", data_file_name), 
  col_types = cols(
      PatientID = col_integer(),
      PracticeID = col_integer(),
      EventCode = col_character(),
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

write_parquet(
  cleaned_filtered,
  path(dir, "data", "working", "nov_24_clean_data.parquet"),
  compression = "zstd"
)

# Clean the environment
rm(list = ls())
