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
  path(dir, "data", "working", "july_25_clean_data_w_resolved.parquet")
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

ltc_first_invite <- clean_data |>
  filter(str_ends(EventType, fixed("(first) LTC Invite"))) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(ltc_first_invite_date = EventDate)

ltc_attend <- clean_data |>
  filter(str_ends(EventType, "LTC Attendance")) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(ltc_attend_date = EventDate)

ltc_first_invite_attend <- left_join(
  ltc_first_invite,
  ltc_attend,
  by = join_by(
    PatientID,
    PracticeID,
    closest(ltc_first_invite_date < ltc_attend_date)
  )
) |>
  mutate(
    ltc_first_invite_month = floor_date(ltc_first_invite_date, "month"),
    # NA means no attendance. If they have an attendance, was it within 60 days?
    # NA gives FALSE, also > 60 days gives FALSE.
    attend_within_60 = !is.na(ltc_attend_date) &
      (ltc_attend_date <= (ltc_first_invite_date + days(60)))
  ) |>
  # Keep only one invite per patient per month
  group_by(PatientID, PracticeID, ltc_first_invite_month) |>
  summarise(
    attend_within_60 = any(attend_within_60),
    .groups = "drop"
  )

ltc_first_attend <- clean_data |>
  filter(str_ends(EventType, fixed("(first) LTC Attendance"))) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(ltc_first_attend_date = EventDate) |>
  mutate(ltc_first_attend_month = floor_date(ltc_first_attend_date, "month"))

ltc_hoc_sent <- clean_data |>
  filter(str_ends(EventType, fixed("HoC Letter"))) |>
  select(PatientID, PracticeID, EventDate) |>
  rename(hoc_sent_date = EventDate)

ltc_first_attend_hoc_sent <- left_join(
  ltc_first_attend,
  ltc_hoc_sent,
  by = join_by(
    PatientID,
    PracticeID,
    closest(ltc_first_attend_date <= hoc_sent_date)
  )
) |>
  mutate(
    hoc_sent_within_30 = !is.na(hoc_sent_date) &
      ((hoc_sent_date - ltc_first_attend_date) < 30)
  ) |>
  # Keep only one invite per patient per month
  group_by(PatientID, PracticeID, ltc_first_attend_month) |>
  summarise(
    hoc_sent_within_30 = any(hoc_sent_within_30),
    .groups = "drop"
  )

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
  census_date_minus15 = census_date - months(15),
  census_date_month_end = ceiling_date(census_date, unit = "month") - days(1)
)

ltc_invite_census <- inner_join(
  ltc_invite,
  months,
  by = join_by(between(
    ltc_invite_date,
    census_date_minus15,
    census_date
  ))
) |>
  # We only need one record per census date per patient?
  select(PatientID, PracticeID, census_date, ltc_invite_date) |>
  distinct(.keep_all = TRUE)

ltc_first_invite_attend_census <- inner_join(
  ltc_first_invite_attend,
  months,
  by = join_by(ltc_first_invite_month == census_date)
) |>
  # We only need one record per census date per patient?
  select(
    PatientID,
    PracticeID,
    census_date = ltc_first_invite_month,
    attend_within_60
  ) |>
  drop_na(census_date) |>
  distinct(.keep_all = TRUE)

ltc_attend_census <- inner_join(
  ltc_attend,
  months,
  by = join_by(between(
    ltc_attend_date,
    census_date_minus15,
    census_date
  ))
) |>
  # We only need one record per census date per patient
  select(PatientID, PracticeID, census_date, ltc_attend_date) |>
  distinct(.keep_all = TRUE)

ltc_first_attend_hoc_sent_census <- inner_join(
  ltc_first_attend_hoc_sent,
  months,
  by = join_by(
    # Looking at the past 15 months of first attendances
    between(ltc_first_attend_month, census_date_minus15, census_date)
  )
) |>
  # We only need one record per census date per patient
  select(
    PatientID,
    PracticeID,
    census_date = ltc_first_attend_month,
    hoc_sent_within_30
  ) |>
  distinct(.keep_all = TRUE)

first_diag_census <- inner_join(
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
  left_join(
    ltc_first_invite_attend_census,
    by = join_by(
      PatientID == PatientID,
      PracticeID == PracticeID,
      census_date == census_date
    ),
    multiple = "first"
  ) |>
  left_join(
    ltc_first_attend_hoc_sent_census,
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
    ltc_attend_prop = ltc_attend_count / ltc_countable_prev_count,
    # NA values mean no invite, so count only those with T/F (i.e. had an invite)
    ltc_first_invite_count = sum(!is.na(attend_within_60)),
    ltc_first_invite_attend_count = sum(attend_within_60, na.rm = TRUE),
    ltc_first_invite_attend_prop = if_else(
      ltc_first_invite_count > 0,
      ltc_first_invite_attend_count / ltc_first_invite_count,
      NA_real_
    ),
    ltc_first_attend_count = sum(!is.na(hoc_sent_within_30)),
    ltc_first_attend_hoc_sent_count = sum(hoc_sent_within_30, na.rm = TRUE),
    ltc_first_attend_hoc_sent_prop = if_else(
      ltc_first_attend_count > 0,
      ltc_first_attend_hoc_sent_count / ltc_first_attend_count,
      NA_real_
    )
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
    ltc_first_invite_count,
    ltc_first_attend_count,
    list_prev,
    list_pop,
    ltc_invite_prop,
    ltc_attend_prop,
    ltc_first_invite_attend_prop,
    ltc_first_attend_hoc_sent_prop
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
  "ltc_first_attend",
  "ltc_first_attend_census",
  "ltc_hoc_sent",
  "ltc_first_attend_hoc_sent",
  "ltc_first_attend_hoc_sent_census",
  "ltc_invite",
  "ltc_invite_census",
  "ltc_invite_attend_census",
  "ltc_first_invite",
  "ltc_first_invite_attend",
  "ltc_first_invite_attend_census",
  "months",
  "shetland_list_sizes",
  "shetland_pops"
)
