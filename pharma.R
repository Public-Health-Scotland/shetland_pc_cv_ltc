library(dplyr) # %>% select group_by summarize n mutate summarise across everything bind_rows
library(lubridate) # dmy
#library(zoo) # as.Date
library(readr) # No used functions found
library(writexl) # No used functions found
library(tidyr) # %>% pivot_wider everything



# Specify the path to the zip file
zip_file <- "/conf/LIST_analytics/Shetland/Primary Care/LTC/data/raw/2025-02-04 - LIST - Med Reviews.zip"

# Unzip the folder to a temporary directory
unzip_dir <- tempdir()
unzip(zip_file, exdir = unzip_dir)

# List the files in the unzipped directory
unzipped_files <- list.files(unzip_dir, full.names = TRUE)

# Read the first file with specified column names and types
col_names <- c("PracticeID", "EventDate", "EventCode", "EventDescription", "DerivedEventType", "DerivedStaffType")  # Replace with actual column names
col_types <- c("numeric", "Date", "factor", "character", "character", "character")  # Replace with actual column types

med_reviews_raw <- read_csv(unzipped_files)

# Remove rows with NA in PracticeID column
med_reviews <- subset(med_reviews_raw, !is.na(PracticeID))

med_reviews <- med_reviews %>% 
  select(PracticeID, EventDate, EventCode, EventDescription, DerivedEventType, DerivedStaffType)

# Convert EventDate to Date format
med_reviews$EventDate <- dmy(med_reviews$EventDate)

# Add a month/year column based on EventDate
med_reviews$MonthYear <- format(med_reviews$EventDate, "%b-%Y")

# Ensure EventDate is in Date format and create MonthYear column
med_reviews$EventDate <- as.Date(med_reviews$EventDate, format = "%d/%m/%Y")
med_reviews$MonthYear <- format(med_reviews$EventDate, "%Y-%m")

# Tabulate the number of events by staff type and month/year
event_summary <- med_reviews %>%
  group_by(DerivedStaffType, MonthYear) %>%
  summarize(NumberOfEvents = n(), .groups = 'drop') %>%
  pivot_wider(names_from = MonthYear, values_from = NumberOfEvents, values_fill = list(NumberOfEvents = 0))

# Calculate the total number of events for each staff type
# Same as mutate(event_summary, TotalEvents = rowSums(select(event_summary, -DerivedStaffType)))
event_summary <- event_summary %>%
  mutate(TotalEvents = rowSums(select(., -DerivedStaffType)))


# Calculate the total number of events for each month
total_by_month <- event_summary %>%
  select(-DerivedStaffType) %>%
  summarise(across(everything(), sum))

# Add a row for the total number of events in each month
total_by_month <- total_by_month %>%
  mutate(DerivedStaffType = "Total")

# Combine the event summary with the total row
combined_summary <- bind_rows(event_summary, total_by_month)

###

# Filter the rows where 'DerivedStaffType' is 'Pharmacist' or 'Total'
filtered_df <- combined_summary[combined_summary$DerivedStaffType %in% c('Pharmacist', 'Total'), ]

# Calculate the percentage of Pharmacist share of the total for each column
percentage_share <- (filtered_df[filtered_df$DerivedStaffType == 'Pharmacist', -1] / 
                       filtered_df[filtered_df$DerivedStaffType == 'Total', -1]) * 100

# Round the percentage share to one decimal place
percentage_share <- round(percentage_share, 1)

# Convert the percentage share to a data frame and add the DerivedStaffType column
percentage_row <- data.frame(DerivedStaffType = 'Percentage completed by Pharmacist', percentage_share)

# Ensure the column names match exactly
colnames(percentage_row) <- colnames(filtered_df)

# Remove leading 'x' from column names in percentage_row
colnames(percentage_row) <- gsub("^x", "", colnames(percentage_row))

# Add one decimal place to whole numbers in the 'Percentage completed by Pharmacist' row
percentage_row[1, -1] <- format(percentage_row[1, -1], nsmall = 1)

# Write percentage row table to excel
write_xlsx(percentage_row, "/conf/LIST_analytics/Shetland/Primary Care/ReviewsbyPharmacist.xlsx")



