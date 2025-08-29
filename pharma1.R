library(dplyr)
library(lubridate)
library(stringr)
library(readxl)
library(writexl)
library(fs)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

# Read in pre-cleaned data
med_reviews <- read_parquet(
  path(dir, "data", "working", "med_reviews_clean.parquet")
)

# Create monthly census dates
months <- tibble(
  census_date = seq.Date(
    from = as.Date("2000-01-01"),
    to = as.Date("2025-08-01"),
    by = "month"
  )
)

# Filter repeat prescriptions
repeat_prescription <- med_reviews |>
  filter(str_starts(DerivedEventType, "Repeat Medication Issued")) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(repeat_presc_issue_date = EventDate)

# Join with census months
denominator <- left_join(
  repeat_prescription,
  months |>
    mutate(
      census_date_minus12 = census_date - months(12),
      end_census_month = ceiling_date(census_date, "month") - days(1)
    ),
  by = join_by(within(repeat_presc_issue_date, repeat_presc_issue_date, census_date_minus12, end_census_month))
) |>
  select(PatientID, PracticeID, census_date, repeat_presc_issue_date) |>
  distinct(PatientID, PracticeID, census_date, .keep_all = TRUE)

# Medication reviews: calculate DaysFromBirth based on birthday logic
med_review_bday <- med_reviews |>
  filter(str_starts(DerivedEventType, "Medication Review")) |>
  select(PatientID, PracticeID, EventDate, DayOfBirth, MonthOfBirth) |>
  mutate(
    # Birthday in same year as review
    birthday_this_year = make_date(year = year(EventDate), month = MonthOfBirth, day = DayOfBirth),
    # Birthday in previous year
    birthday_last_year = make_date(year = year(EventDate) - 1, month = MonthOfBirth, day = DayOfBirth),
    # Use most recent birthday before or on review date
    BirthDate = if_else(EventDate >= birthday_this_year, birthday_this_year, birthday_last_year),
    # Days since birth date
    DaysFromBirth = as.integer(difftime(EventDate, BirthDate, units = "days")),
    # Flags for reviews within 90 and 180 days *after* birth date
    within_90 = between(DaysFromBirth,0, 90),
    within_180 = between(DaysFromBirth, 0, 180)
  )

# Join with denominator
final_data <- left_join(
  denominator,
  med_review_bday,
  by = join_by(
    PatientID == PatientID,
    PracticeID == PracticeID,
    closest(census_date > BirthDate)
  )
)

# Summarise for both 90 and 180 day splits
pharma_1_monthly <- final_data |>
  group_by(PracticeID, census_date) |>
  summarise(
    patients_on_repeat_presc = n_distinct(PatientID),
    med_reviews_180 = sum(within_180, na.rm = TRUE),
    med_reviews_90 = sum(within_90, na.rm = TRUE),
    prop_review_180 = med_reviews_180 / patients_on_repeat_presc,
    prop_review_90 = med_reviews_90 / patients_on_repeat_presc
  ) |>
  ungroup()

# Export to Excel
write_xlsx(
  pharma_1_monthly,
  path = path(dir, "data", "outputs", "Pharma_med_reviews_90_180.xlsx")
)
