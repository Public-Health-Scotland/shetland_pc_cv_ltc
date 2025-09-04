library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(nanoparquet)
library(fs)
library(readr)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

clean_data <- read_parquet(
  path(dir, "data", "working", "may_25_clean_data.parquet")
)

# Find the first diagnosis date (any condition) for each patient
first_diag <- clean_data |>
  filter(str_starts(EventType, "Record of Diagnosis")) |>
  group_by(PatientID) |>
  summarise(
    FirstDiag = min(EventDate),
    DateOfDeath = first(DateOfDeath),
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
shetland_pops <- read_parquet(path(
  dir,
  "data",
  "lookups",
  "shetland_pops.parquet"
))

# Read in GP List sizes (Shetland Practice List Sizes)
shetland_list_sizes <- read_parquet(
  path(dir, "data", "lookups", "shetland_list_sizes.parquet")
)

# Create a tibble of monthly dates
months <- tibble(
  census_date = seq.Date(
    from = as.Date("2020-01-01"),
    to = as.Date("2025-06-01"), # latest date available
    by = "month"
  ),
  census_date_minus15 = census_date - months(15)
)

ltc_invite_census <- left_join(
  ltc_invite,
  months,
  by = join_by(within(
    ltc_invite_date,
    ltc_invite_date,
    census_date_minus15,
    census_date
  ))
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
    within(
      ltc_invite_date,
      ltc_invite_date_plus60,
      ltc_attend_date,
      ltc_attend_date
    )
  )
) |>
  select(PatientID, PracticeID, census_date, ltc_invite_date, ltc_attend_date)

ltc_attend_census <- left_join(
  ltc_attend,
  months,
  by = join_by(within(
    ltc_attend_date,
    ltc_attend_date,
    census_date_minus15,
    census_date
  ))
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
  # This assigns each patient to a practice for each month.
  left_join(
    clean_data |>
      select(PatientID, PracticeID, EventDate, JoinedDate) |>
      arrange(PatientID, EventDate) |>
      distinct(PatientID, PracticeID, .keep_all = TRUE),
    by = join_by(PatientID == PatientID, closest(census_date >= EventDate)),
    multiple = "last", # Use the latest practice joined if 2 events on the same day
    relationship = "many-to-one"
  ) |>
  arrange(PatientID, EventDate) |>
  fill(PracticeID, .direction = "downup") |>
  select(-EventDate, -JoinedDate) |>
  # Exclude records after death
  filter(
    is.na(DateOfDeath) | census_date <= floor_date(DateOfDeath, unit = "month")
  ) |>
  # filter(is.na(LeftDate) | census_date <= floor_date(LeftDate, unit = "month")) |>
  filter(
    is.na(LeftShetlandDate) |
      census_date <= floor_date(LeftShetlandDate, unit = "month")
  )

census_data <- first_diag_census |>
  left_join(
    ltc_invite_census,
    by = join_by(
      PatientID == PatientID,
      PracticeID == PracticeID,
      census_date == census_date
    ),
    multiple = "first"
  ) |>
  left_join(
    ltc_attend_census,
    by = join_by(
      PatientID == PatientID,
      PracticeID == PracticeID,
      census_date == census_date
    ),
    multiple = "first"
  )

monthly_summary <- census_data |>
  group_by(census_date, PracticeID) |>
  summarise(
    ltc_prev_count = n_distinct(PatientID),
    ltc_invite_count = sum(!is.na(ltc_invite_date)),
    ltc_attend_count = sum(!is.na(ltc_attend_date)),
    ltc_invite_prop = ltc_invite_count / ltc_prev_count,
    ltc_attend_prop = ltc_attend_count / ltc_prev_count
  ) |>
  ungroup() |>
  left_join(
    shetland_list_sizes,
    by = join_by(
      PracticeID == PracticeID,
      closest(census_date >= Date)
    )
  ) |>
  mutate(list_prev = ltc_prev_count / list_pop) |>
  select(
    PracticeID,
    census_date,
    ltc_prev_count,
    list_prev,
    list_pop,
    ltc_invite_prop,
    ltc_attend_prop
  )


#Shetland averages for first diagnosis, invites and attends
monthly_shetland_summary_avg <- monthly_summary |>
  group_by(census_date) |>
  summarise(
    ltc_prev_avg = mean(ltc_prev_count),
    ltc_invite_prop_avg = mean(ltc_invite_prop),                    
    ltc_attend_prop_avg = mean(ltc_attend_prop)
  ) |>
  ungroup() |>
  
  select(
    census_date,
    ltc_prev_avg,
    ltc_invite_prop_avg,                    
    ltc_attend_prop_avg
    
  )


rm(
  "census_data",
  "clean_data",
  "dir",
  "first_diag",
  "first_diag_census",
  "ltc_attend",
  "ltc_attend_census",
  "ltc_invite",
  "ltc_invite_attend_census",
  "ltc_invite_census",
  "months",
  "shetland_list_sizes",
  "shetland_pops"
)
