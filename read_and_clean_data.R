library(dplyr)
library(tidyr)
library(lubridate)
library(readr)
library(arrow)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

raw_data <- read_csv(
  file = path(dir, "data", "raw", "2024-11-04 - LIST CV LTC pseudo-anon extract.zip"), 
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

write_parquet(cleaned_filtered, path(dir, "data", "working", "nov_24_clean_data.parquet"), compression = "zstd")

# Clean the environment
rm(list = ls())

