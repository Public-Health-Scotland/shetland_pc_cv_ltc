library(dplyr)
library(lubridate)
library(readxl)
library(nanoparquet)

# Load file
# read_xlsx gives a warning for 1860-01-01 in PreviousReviewDate
med_reviews <- read_xlsx("/conf/LIST_analytics/Shetland/Primary Care/LTC/data/raw/2025-05 - LIST - Med Reviews.xlsx") |>
  filter(!is.na(PracticeID)) |>
  select(PracticeID,
    EventDate,
    EventCode,
    EventDescription,
    DerivedEventType,
    DerivedStaffType
  ) |>
  mutate(
    MonthYear = format(EventDate, "%Y-%m"),
    QuarterYear = paste0("Q", quarter(EventDate), "-", year(EventDate))
      )

# Write out cleaned data

write_parquet(med_reviews, "/conf/LIST_analytics/Shetland/Primary Care/LTC/data/working/med_reviews_clean.parquet")

  




