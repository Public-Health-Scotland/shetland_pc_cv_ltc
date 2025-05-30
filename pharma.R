library(dplyr)
library(lubridate)
library(readr)
library(writexl)
library(tidyr)
library(readxl)
library(openxlsx)

# Load file
# Gives warning for 1860-01-01 in PreviousReviewDate

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





### Prior Code below ... delete

# Monthly summary and percentage pivot
monthly_summary <- med_reviews %>%
  count(PracticeID, DerivedEventType, MonthYear, name = "NumberOfEvents") %>%
  group_by(PracticeID, MonthYear) %>%
  mutate(TotalEvents = sum(NumberOfEvents)) %>%
  ungroup() %>%
  mutate(Percentage = round(NumberOfEvents / TotalEvents * 100, 1))

monthly_summary_pivot <- monthly_summary %>%
  select(PracticeID, DerivedEventType, MonthYear, NumberOfEvents) %>%
  pivot_wider(names_from = MonthYear, values_from = NumberOfEvents, values_fill = 0)

monthly_percentage_pivot <- monthly_summary %>%
  select(PracticeID, DerivedEventType, MonthYear, Percentage) %>%
  pivot_wider(names_from = MonthYear, values_from = Percentage, values_fill = 0)

# Quarterly summary and percentage pivot
quarterly_summary <- med_reviews %>%
  count(PracticeID, DerivedEventType, QuarterYear, name = "NumberOfEvents") %>%
  group_by(PracticeID, QuarterYear) %>%
  mutate(TotalEvents = sum(NumberOfEvents)) %>%
  ungroup() %>%
  mutate(Percentage = round(NumberOfEvents / TotalEvents * 100, 1))

quarterly_percentage_pivot <- quarterly_summary %>%
  select(PracticeID, DerivedEventType, QuarterYear, Percentage) %>%
  pivot_wider(names_from = QuarterYear, values_from = Percentage, values_fill = 0)

# Chronologically order quarters
quarterly_percentage_long <- quarterly_percentage_pivot %>%
  pivot_longer(-c(PracticeID, DerivedEventType), names_to = "QuarterYear", values_to = "Percentage") %>%
  pivot_wider(names_from = DerivedEventType, values_from = Percentage, values_fill = 0) %>%
  arrange(
    PracticeID,
    as.Date(paste0(
      substr(QuarterYear, 4, 7), "-",
      (as.numeric(substr(QuarterYear, 2, 2)) - 1) * 3 + 1,
      "-01"
    ))
  )

# Monthly layout: months vertical, event types horizontal
monthly_percentage_long <- monthly_percentage_pivot %>%
  pivot_longer(-c(PracticeID, DerivedEventType), names_to = "MonthYear", values_to = "Percentage") %>%
  pivot_wider(names_from = DerivedEventType, values_from = Percentage, values_fill = 0) %>%
  arrange(PracticeID, MonthYear)

# Output previews
print(monthly_percentage_long)
print(quarterly_percentage_long)

#don't  use this code, it's just to show what the final structure should look like but you  can get there much more cleanly than this.
quarterly_percentage_long |> 
  filter(str_sub(QuarterYear, 4) >= 2023) |> 
  pivot_longer(contains(" "))

# Create a named list of data frames for export
output_list <- list(
  "Pharmacist Percentages" = final_results,
  "Monthly Percentages" = monthly_percentage_long,
  "Quarterly Percentages" = quarterly_percentage_long
)



# Write the list to the specified Excel file path
write_xlsx(output_list, path = "/conf/LIST_analytics/Shetland/Primary Care/PCPIP Indicators/Peter/pharmareviewsmay25.xlsx")



