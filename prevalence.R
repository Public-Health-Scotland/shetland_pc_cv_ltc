library(dplyr) 
library(stringr) 
library(lubridate) 
library(nanoparquet) 
library(fs) 

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

clean_data <- read_parquet(path(dir, "data", "working", "apr_25_clean_data.parquet"))

#clean_data <- filter(clean_data, PracticeID != 3)



# Find the first diagnosis date (any condition) for each patient
first_diag <- clean_data |>
  filter(str_starts(EventType, "Record of Diagnosis")) |>
  group_by(PatientID, PracticeID) |>   #do we need the PracticeID as we dont have Practice details
  summarise(
    FirstDiag = min(EventDate),
    DateOfDeath = first(DateOfDeath),    # do we need to add the Left Practice and Main Address Off Shetland?
    LeftDate = first(LeftDate),
    LeftShetlandDate = first(LeftShetlandDate),
  )


# LTC Invite
ltc_invite <- clean_data |>
  filter(str_ends(EventType, "LTC Invite")) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(ltc_invite_date = EventDate)

ltc_attend <- clean_data |>
  filter(str_ends(EventType, "LTC Attendance")) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(ltc_attend_date = EventDate)

# Read in population estimates for Shetland
shetland_pops <- read_parquet(path(dir, "data", "lookups", "shetland_pops.parquet"))

# Read in GP List sizes (aggregated to Shetland cluster level)
shetland_list_sizes <- read_parquet(path(dir, "data", "lookups", "shetland_list_sizes.parquet"))

# Create a tibble of monthly dates
months <- tibble(
  census_date = seq.Date(
    from = as.Date("2000-01-01"),
    to = as.Date("2025-04-22"), #latest date available
    by = "month"
  )
)

ltc_invite_census <- left_join(
  ltc_invite,
  months |>
    mutate(census_date_minus15 = census_date - months(15)),
  by = join_by(within(ltc_invite_date, ltc_invite_date, census_date_minus15, census_date))
) |>
  # We only need one record per census date per patient?
  select(PatientID, PracticeID, census_date, ltc_invite_date) |>
  distinct(.keep_all = TRUE)

ltc_invite_attend_census <- left_join(
  ltc_invite_census |>
    mutate(ltc_invite_date_plus60 = ltc_invite_date + days(60)),
  ltc_attend,
  by = join_by(
    PatientID == PatientID,
    PracticeID == PracticeID,
    within(ltc_invite_date, ltc_invite_date_plus60, ltc_attend_date, ltc_attend_date)
  )
) |>
  select(PatientID, PracticeID, census_date, ltc_invite_date, ltc_attend_date)

ltc_attend_census <- left_join(
  ltc_attend,
  months |>
    mutate(census_date_minus15 = census_date - months(15)),
  by = join_by(within(ltc_attend_date, ltc_attend_date, census_date_minus15, census_date))
) |>
  # We only need one record per census date per patient
  select(PatientID, PracticeID, census_date, ltc_attend_date) |>
  distinct(.keep_all = TRUE)

first_diag_census <- left_join(
  first_diag,
  months,
  # Join on condition that FirstDiag is before or on census_date
  by = join_by(FirstDiag <= census_date)
) |>
  # Exclude records after death
  filter(is.na(DateOfDeath) | census_date <= floor_date(DateOfDeath, unit = "month"))|> 
  filter(is.na(LeftDate) | census_date <= floor_date(LeftDate, unit = "month")) |>
  filter(is.na(LeftShetlandDate) | census_date <= floor_date(LeftShetlandDate, unit = "month"))

census_data <- first_diag_census |>
  left_join(
    ltc_invite_census,
    by = join_by(PatientID == PatientID, PracticeID == PracticeID, census_date == census_date),
    multiple = "first"
  ) |>
  left_join(
    ltc_attend_census,
    by = join_by(PatientID == PatientID, PracticeID == PracticeID, census_date == census_date),
    multiple = "first"
  )

monthly_summary <- census_data |>
  group_by(census_date, PracticeID) |>
  summarise(
    count = n_distinct(PatientID),
    ltc_invite_count = sum(!is.na(ltc_invite_date)),
    ltc_attend_count = sum(!is.na(ltc_attend_date)),
    .groups = "drop"
  ) |>
  ungroup() |> 
  mutate(census_year = year(census_date)) |>
  left_join(shetland_pops, by = join_by(closest(census_year >= pop_year))) |>
  left_join(shetland_list_sizes, by = join_by(closest(census_date >= Date)))
  
  ###Can be used once we can identify Practices#
  #left_join(shetland_list_sizes, by = join_by(closest(census_date >= Date))) |>
  #mutate(
    #pop_prev = count / pop,
    #list_prev = count / list_pop,
   # ltc_invite_prop = ltc_invite_count / count,
    #ltc_attend_prop = ltc_attend_count / count
  #)
