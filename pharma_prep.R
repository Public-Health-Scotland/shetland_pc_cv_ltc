library(dplyr)
library(lubridate)
library(readxl)
library(nanoparquet)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Print the files in the folder to check
dir_ls(path(dir, "data", "raw")) |>
  path_file()

# File name - UPDATE THIS
data_file_name <- "2025-05 - LIST - Med Reviews.xlsx"
# File path
data_file_path <- path(dir, "data", "raw", data_file_name)

# Load file
# read_xlsx gives a warning for 1860-01-01 in PreviousReviewDate
med_reviews <- read_xlsx(
  path = data_file_path,
  col_types = c(
    PatientID = "numeric",
    PracticeID = "numeric",
    DayOfBirth = "numeric",
    MonthOfBirth = "numeric",
    DateOfDeath = "date",
    EventDate = "date",
    EventCode = "skip",
    EventDescription = "skip",
    DerivedEventType = "text",
    StaffType = "skip",
    DerivedStaffType = "text",
    DateLeftShetland = "date"
  )
) |>
  filter(!is.na(PracticeID)) |>
  select(
    PracticeID,
    EventDate,
    DerivedEventType,
    DerivedStaffType
  ) |>
  mutate(
    MonthYear = format(EventDate, "%Y-%m"),
    QuarterYear = paste0("Q", quarter(EventDate), "-", year(EventDate))
  )

# Write out cleaned data

write_parquet(
  med_reviews,
  path(dir, "data", "working", "med_reviews_clean.parquet"),
  compression = "zstd"
)

rm(list = ls())
