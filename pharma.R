library(dplyr)
library(lubridate)
library(nanoparquet)
library(writexl)

# Read in pre-cleaned data

med_reviews <- read_parquet(
  "/conf/LIST_analytics/Shetland/Primary Care/LTC/data/working/med_reviews_clean.parquet"
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
  filter(
    DerivedEventType %in%
      c(
        "Medication Review with Person",
        "Notes based Medication Review",
        "Polypharmacy Medication Review"
      )
  ) |>
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
  ) %>%
  ungroup() %>%
  mutate(event_type_proportion = NumberOfEvents / TotalEvents)

# Create a named list of data frames for export
output_list <- list(
  "Pharmacist Percentages" = pharmacist_reviews,
  "Monthly Percentages" = monthly_event_type,
  "Quarterly Percentages" = quarterly_event_type
)

# Generate the file path with the current date in YYYYMMDD format

date_str <- format(Sys.Date(), "%Y%m%d")
file_path <- paste0(
  "/conf/LIST_analytics/Shetland/Primary Care/LTC/data/outputs/Shetland-PCPIP-indicators-",
  date_str,
  ".xlsx"
)

# Write the output

write_xlsx(output_list, path = file_path)
