library(dplyr) # A Grammar of Data Manipulation
library(stringr) # Simple, Consistent Wrappers for Common String Operations
library(lubridate) # Make Dealing with Dates a Little Easier
library(nanoparquet) # Read and Write 'Parquet' Files
library(fs) # Cross-Platform File System Operations Based on 'libuv'

dir <- path("/conf/LIST_analytics/Shetland/Primary Care/LTC")

clean_data <- read_parquet(path(dir, "data", "working", "nov_24_clean_data.parquet"))

# Find the first diagnosis date (any condition) for each patient
first_diag <- clean_data |>
  filter(str_starts(EventType, "Record of Diagnosis")) |>
  group_by(PatientID) |>
  summarise(
    FirstDiag = min(EventDate),
    DateOfDeath = first(DateOfDeath)
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
    to = as.Date("2024-11-01"),
    by = "month"
  )
)

ltc_invite_census <- left_join(
  ltc_invite,
  months |>
    mutate(census_date_minus15 = census_date - months(15)),
  by = join_by(within(ltc_invite_date, ltc_invite_date, census_date_minus15, census_date))
) |> 
  # We only need one record per census date per patient
  select(PatientID, census_date, ltc_invite_date) |> 
  distinct(.keep_all = TRUE)

ltc_invite_attend_census <- left_join(
  ltc_invite_census |> 
    mutate(ltc_invite_date_plus60 = ltc_invite_date + days(60)),
  ltc_attend,
  by = join_by(PatientID == PatientID,
               within(ltc_invite_date, ltc_invite_date_plus60, ltc_attend_date, ltc_attend_date))
) |> 
  select(PatientID, census_date, ltc_invite_date, ltc_attend_date)

ltc_attend_census <- left_join(
  ltc_attend,
  months |>
    mutate(census_date_minus15 = census_date - months(15)),
  by = join_by(within(ltc_attend_date, ltc_attend_date, census_date_minus15, census_date))
) |> 
  # We only need one record per census date per patient
  select(PatientID, census_date, ltc_attend_date) |> 
  distinct(.keep_all = TRUE)

first_diag_census <- left_join(
  first_diag,
  months,
  # Join on condition that FirstDiag is before or on census_date
  by = join_by(FirstDiag <= census_date)
) |>
  # Exclude records after death
  filter(is.na(DateOfDeath) | census_date <= floor_date(DateOfDeath, unit = "month"))

census_data <- first_diag_census |>
  left_join(
    ltc_invite_census,
    by = join_by(PatientID == PatientID, census_date == census_date),
    multiple = "first"
  ) |>
  left_join(
    ltc_attend_census,
    by = join_by(PatientID == PatientID, census_date == census_date),
    multiple = "first"
  )

monthly_summary <- census_data |>
  group_by(census_date) |>
  summarise(
    count = n_distinct(PatientID),
    ltc_invite_count = sum(!is.na(ltc_invite_date)),
    ltc_attend_count = sum(!is.na(ltc_attend_date)),
    .groups = "drop"
  ) |>
  mutate(census_year = year(census_date)) |> 
  left_join(shetland_pops, by = join_by(closest(census_year >= pop_year))) |>
  left_join(shetland_list_sizes, by = join_by(closest(census_date >= Date))) |>
  mutate(
    pop_prev = count / pop,
    list_prev = count / list_pop,
    ltc_invite_prop = ltc_invite_count / count,
    ltc_attend_prop = ltc_attend_count / count
  )
