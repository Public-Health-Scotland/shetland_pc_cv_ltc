library(dplyr)
library(lubridate)
library(readr)
library(writexl)
library(tidyr)
library(readxl)
library(openxlsx)

# Load file

med_reviews <- read.xlsx("/conf/LIST_analytics/Shetland/Primary Care/LTC/data/raw/2025-04-23 - LIST - Med Reviews.xlsx") |> 
  filter(!is.na(PracticeID)) |> 
  select(PracticeID, ReviewDate, EventCode, EventDescription, DerivedEventType, DerivedStaffType) |> 
  mutate(
    EventDate = as.Date(ReviewDate, origin = "1899-12-30"),
    MonthYear = format(EventDate, "%Y-%m"),
    QuarterYear = paste0("Q", quarter(EventDate), "-", year(EventDate))
  )


# Summarize events by staff type and month
event_summary <- med_reviews %>%
  count(PracticeID, DerivedStaffType, MonthYear, name = "NumberOfEvents") %>%
  pivot_wider(names_from = MonthYear, values_from = NumberOfEvents, values_fill = 0)

# Calculate pharmacist percentages per practice
summed_practice_datasets <- event_summary %>%
  group_split(PracticeID) %>%
  setNames(unique(event_summary$PracticeID)) %>%
  lapply(function(df) {
    df %>%
      group_by(DerivedStaffType) %>%
      summarize(across(starts_with("20"), sum, .names = "Total_{col}"), .groups = "drop")
  })

pharmacist_percentages <- lapply(summed_practice_datasets, function(df) {
  total <- df %>% summarize(across(starts_with("Total_"), sum))
  pharmacist <- df %>% filter(DerivedStaffType == "Pharmacist") %>%
    summarize(across(starts_with("Total_"), sum))
  round(pharmacist / total * 100, 1)
})

final_results <- bind_rows(pharmacist_percentages, .id = "PracticeID")

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

# Create a named list of data frames for export
output_list <- list(
  "Pharmacist Percentages" = final_results,
  "Monthly Percentages" = monthly_percentage_long,
  "Quarterly Percentages" = quarterly_percentage_long
)



# Write the list to the specified Excel file path
write_xlsx(output_list, path = "/conf/LIST_analytics/Shetland/Primary Care/PCPIP Indicators/Peter/pharmareviewsmay25.xlsx")



