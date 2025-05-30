library(dplyr)
library(lubridate)
library(readr)
library(writexl)
library(tidyr)
library(readxl)

# Load file
# read_xlsx gives a warning for 1860-01-01 in PreviousReviewDate
med_reviews <- read_xlsx("/conf/LIST_analytics/Shetland/Primary Care/LTC/data/raw/2025-04-23 - LIST - Med Reviews.xlsx") |>
  filter(!is.na(PracticeID)) |>
  select(PracticeID,
    EventDate = ReviewDate,
    EventCode,
    EventDescription,
    DerivedEventType,
    DerivedStaffType
  ) |>
  mutate(
    MonthYear = format(EventDate, "%Y-%m"),
    QuarterYear = paste0("Q", quarter(EventDate), "-", year(EventDate))
  )

# Summarise events by staff type and month, filter for Pharmacist

pharmacist_reviews <- med_reviews |>
  mutate(census_date = floor_date(EventDate, unit = "month")) |>
  count(PracticeID, census_date, DerivedStaffType, name = "NumberOfEvents") |>
  group_by(PracticeID, census_date) |>
  mutate(total_events = sum(NumberOfEvents)) |>
  ungroup() |>
  filter(DerivedStaffType == "Pharmacist") |>
  mutate(pharmacist_proportion = NumberOfEvents / total_events)

# Summarise event type by Practice

monthly_event_type <- med_reviews %>%
  mutate(census_date = floor_date(EventDate, unit = "month")) %>%
  count(PracticeID, census_date, DerivedEventType, name = "NumberOfEvents") %>%
  group_by(PracticeID, census_date) %>%
  mutate(
    TotalEvents = sum(NumberOfEvents),
    event_type_proportion = NumberOfEvents / TotalEvents
  ) %>%
  ungroup() %>%
  arrange(PracticeID, census_date, DerivedEventType)

# Summarise event type and add quarter column

quarterly_event_type <- monthly_event_type %>%
  mutate(quarter_start = floor_date(census_date, "quarter")) %>%
  group_by(PracticeID, quarter_start, DerivedEventType) %>%
  summarise(
    NumberOfEvents = sum(NumberOfEvents),
    TotalEvents = sum(TotalEvents),
    .groups = "drop"
  ) %>%
  mutate(event_type_proportion = NumberOfEvents / TotalEvents)

# Create a named list of data frames for export
output_list <- list(
  "Pharmacist Percentages" = pharmacist_reviews,
  "Monthly Percentages" = monthly_event_type,
  "Quarterly Percentages" = quarterly_event_type
)

# Write the list to the specified Excel file path

write_xlsx(output_list, path = "/conf/LIST_analytics/Shetland/Primary Care/PCPIP Indicators/Peter/pharmareviewsmay28.xlsx")




