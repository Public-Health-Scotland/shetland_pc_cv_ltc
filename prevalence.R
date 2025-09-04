library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(nanoparquet)
library(fs)
library(readr)
library(dtplyr)

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

clean_data <- read_parquet(
  path(dir, "data", "working", "june_25_clean_data_w_resolved.parquet")
)

# Find the first diagnosis date (any condition) for each patient
first_diag <- clean_data |>
  filter(str_ends(EventType, "Code")) |>
  separate_wider_delim(
    cols = EventType,
    delim = regex("\\s+-\\s+"),
    names = c("ltc", "type")
  ) |>
  lazy_dt() |>
  group_by(PatientID, ltc) |>
  mutate(
    FirstDiag = na_if(
      min(EventDate[type == "Diagnosis Code"], na.rm = TRUE),
      as.Date(Inf)
    ),
    Resolution = na_if(
      max(EventDate[type == "Resolved Code"], na.rm = TRUE),
      as.Date(-Inf)
    )
  ) |>
  drop_na(FirstDiag) |>
  group_by(PatientID) |>
  summarise(
    FirstDiag = min(FirstDiag),
    Resolution = max(Resolution),
    DateOfDeath = first(DateOfDeath),
    LeftShetlandDate = first(LeftShetlandDate)
  ) |>
  ungroup() |>
  collect() |>
  # Clear resolved codes that happen before diagnosis
  mutate(Resolution = if_else(Resolution < FirstDiag, NA_Date_, Resolution))

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
    to = as.Date("2025-07-01"), # latest date available
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
      select(PatientID, PracticeID, EventDate, JoinedDate, LeftDate) |>
      arrange(PatientID, EventDate),
    by = join_by(PatientID == PatientID, closest(census_date >= EventDate)),
    multiple = "last", # Use the latest practice joined if 2 events on the same day
    relationship = "many-to-one"
  ) |>
  arrange(PatientID, EventDate) |>
  fill(PracticeID, .direction = "downup") |>
  select(-EventDate) |>
  # Exclude resolved conditions
  filter(
    is.na(Resolution) | census_date <= floor_date(Resolution, unit = "month")
  ) |>
  # Exclude records after death
  filter(
    is.na(DateOfDeath) | census_date <= floor_date(DateOfDeath, unit = "month")
  ) |>
  filter(
    is.na(LeftDate) | census_date <= floor_date(LeftDate, unit = "month")
  ) |>
  filter(
    is.na(LeftShetlandDate) |
      census_date <= floor_date(LeftShetlandDate, unit = "month")
  ) |>
  mutate(first_diag_plus15 = FirstDiag + months(15))


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
  ) |>
  mutate(
    PatientID_countable = if_else(
      first_diag_plus15 < census_date,
      PatientID,
      NA
    )
  )

monthly_summary <- census_data |>
  group_by(census_date, PracticeID) |>
  summarise(
    ltc_prev_count = n_distinct(PatientID),
    ltc_countable_prev_count = n_distinct(PatientID_countable) - 1,
    ltc_invite_count = sum(!is.na(ltc_invite_date)),
    ltc_attend_count = sum(!is.na(ltc_attend_date)),
    ltc_invite_prop = ltc_invite_count / ltc_countable_prev_count,
    ltc_attend_prop = ltc_attend_count / ltc_countable_prev_count
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
    ltc_countable_prev_count,
    list_prev,
    list_pop,
    ltc_invite_prop,
    ltc_attend_prop
  )


#Shetland averages for first diagnosis, invites and attends
monthly_shetland_summary_avg <- monthly_summary |>
  group_by(census_date) |>
  summarise(
    ltc_prev_total = sum(ltc_prev_count),
    ltc_countable_prev_total =sum(ltc_countable_prev_count),
    ltc_invite_prop_avg = sum(ltc_invite_count)/ltc_countable_prev_total,                    
    ltc_attend_prop_avg = sum(ltc_attend_count)/ltc_countable_prev_total
  ) |>
  ungroup() |>
  
  select(
    census_date,
    ltc_prev_total,
    ltc_countable_prev_total,
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
