library(dplyr)
library(lubridate)
library(nanoparquet)
library(writexl)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Read in pre-cleaned data
med_reviews <- read_parquet(
  path(dir, "data", "working", "med_reviews_clean.parquet")
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


#Shetland averages for pharmacist_reviews,monthly_event_type,quarterly_event_type

monthly_shetland_pharmacist_review_avg <- pharmacist_reviews |>
  group_by(census_date) |>
  summarise(
    NumberOfEvents_avg = mean(NumberOfEvents),
    total_events_avg = mean(total_events),
    pharmacist_proportion_avg = sum(NumberOfEvents)/sum(total_events)

  ) |>
  ungroup() 


monthly_shetland_event_type_avg <- monthly_event_type |>
  group_by(census_date) |>
  summarise(
    NumberOfEvents_avg = mean(NumberOfEvents),
    TotalEvents_avg = mean(TotalEvents),
    event_type_proportion_avg = sum(NumberOfEvents)/sum(TotalEvents)

  ) |>
  ungroup() 


quarterley_shetland_event_type_avg <- quarterly_event_type |>
  group_by(quarter_start) |>
  summarise(
    NumberOfEvents_avg = mean(NumberOfEvents),
    TotalEvents_avg = mean(TotalEvents),
    event_type_proportion_avg = sum(NumberOfEvents)/sum(TotalEvents)
    
  ) |>
  ungroup() 



# Create a named list of data frames for export
output_list <- list(
  "Pharmacist Percentages" = pharmacist_reviews,
  "Monthly Percentages" = monthly_event_type,
  "Quarterly Percentages" = quarterly_event_type
)

# Generate the file path with the current date in YYYYMMDD format

date_str <- format(Sys.Date(), "%Y%m%d")

output_path <- path(
  dir, "data", "outputs",
  paste0("Shetland-PCPIP-indicators-", date_str, ".xlsx")
)

# Write the output
write_xlsx(output_list, path = output_path)

rm(med_reviews, output_list, date_str, output_path)
