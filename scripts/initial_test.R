# Load the packages
library(sf)
library(ggplot2)
library(here)
library(janitor)
library(tidyverse)

#########
# CREATE TYPE_PESTICIDE
type_pesticide <- read_csv(here("data/PUR_product_database/productdb/type_pesticide.dat"))
# get into v1 format
colnames(type_pesticide)[1] <- "v1"
type_pesticide <- type_pesticide %>%
  bind_rows(tibble(v1 = "A0ADJUVANT")) %>%
  arrange(desc(row_number()))
# Split v1 into typepesticide_cd
type_pesticide <- type_pesticide %>%
  mutate(
    typepesticide_cd = substr(v1, 1, 2),  # First 2 characters
    typepesticide_dsc = substr(v1, 3, nchar(v1))  # Rest of the string
  )
# Sort the data by 'typepesticide_cd'
type_pesticide <- type_pesticide %>%
  arrange(typepesticide_cd)
type_pesticide <- select(type_pesticide, -v1)
# write.table(prod_type_pesticide, here("data/PUR_product_database/updated_productdb/type_pesticide.csv"), row.names = FALSE, sep = ",")

#########
# CREATE PROD_TYPE_PESTICIDE
prod_type_pesticide <- read_csv(here("data/PUR_product_database/productdb/prod_type_pesticide.dat"))
# get into v1 format
colnames(prod_type_pesticide)[1] <- "v1"
prod_type_pesticide <- prod_type_pesticide %>%
  bind_rows(tibble(v1 = "20532K0")) %>%
  arrange(desc(row_number()))
# Extract 'prodno' (first 6 digits from the start of the string)
prod_type_pesticide <- prod_type_pesticide %>%
  mutate(
    # Calculate the substring for 'prodno', removing the last 2 characters
    prodno_raw = substr(v1, 1, nchar(v1) - 2),
    # Convert to numeric, padding with leading zeros to ensure consistency
    prodno = sprintf("%06d", as.numeric(prodno_raw))
  )

# Extract 'typepesticide_cd' (last 2 characters)
prod_type_pesticide <- prod_type_pesticide %>%
  mutate(
    typepesticide_cd = substr(v1, nchar(v1) - 1, nchar(v1))
  )

# Drop the original column 'v1'
prod_type_pesticide <- prod_type_pesticide %>%
  select(-v1, -prodno_raw)
# write.table(prod_type_pesticide, here("data/PUR_product_database/updated_productdb/prod_type_pesticide.csv"), row.names = FALSE, sep = ",")

# MERGE
pesticide_use_summary <- prod_type_pesticide %>%
  left_join(type_pesticide, by = "typepesticide_cd")

# Step 1: Sort by 'prodno'
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno)

# Step 2: Create the 'dups' column to tag duplicates in 'prodno'
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(dups = ifelse(duplicated(prodno), 1, 0))

# Step 3: Tabulate the duplicates
tabulate(pesticide_use_summary$dups)

#Create the binary columns based on typepesticide_cd
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    Insecticide = ifelse(typepesticide_cd %in% c("N0", "O0", "W0", "R0"), 1, 0),
    Herbicide = ifelse(typepesticide_cd == "M0", 1, 0),
    Fungicide = ifelse(typepesticide_cd == "K0", 1, 0),
    VertControl = ifelse(typepesticide_cd == "U0", 1, 0)
  )

# View the updated dataframe
head(pesticide_use_summary)

# Step 1: Sort by 'prodno' and 'Insecticide' (descending order)
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(Insecticide))

# Step 2: Create 'InsecticideFill' column based on 'Insecticide' values
pesticide_use_summary$InsecticideFill <- ifelse(pesticide_use_summary$Insecticide == 1, 1, NA)

# Step 3: Fill missing values in 'InsecticideFill' by propagating the last non-NA value within each 'prodno'
library(zoo)
pesticide_use_summary <- pesticide_use_summary %>%
  group_by(prodno) %>%
  mutate(InsecticideFill = zoo::na.locf(InsecticideFill, na.rm = FALSE)) %>%
  ungroup()

# Create the InsectAll, Adjuvant, and Fertilizer columns
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    InsectAll = ifelse(InsecticideFill == 1, 1, 0),  # InsectAll is 1 if InsecticideFill is 1
    Adjuvant = ifelse(typepesticide_cd == "A0", 1, 0),  # Adjuvant is 1 if typepesticide_cd is "A0"
    Fertilizer = ifelse(typepesticide_cd == "J0", 1, 0)  # Fertilizer is 1 if typepesticide_cd is "J0"
  )

# Step 1: Create 'OtherIcide' column (Insecticide == 0, Adjuvant == 0, Fertilizer == 0)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    OtherIcide = ifelse(Insecticide == 0 & Adjuvant == 0 & Fertilizer == 0, 1, 0)
  )

# Step 2: Sort by prodno (ascending) and 'OtherIcide' (descending)
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(OtherIcide))

# Step 3: Create 'OtherIcideFill' column by filling down the 'OtherIcide' values
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(OtherIcideFill = ifelse(OtherIcide == 1, 1, NA)) %>%
  fill(OtherIcideFill, .direction = "down")

# Step 4: Create 'InsectOnly' column (InsecticideFill == 1 and OtherIcideFill != 1)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(InsectOnly = ifelse(InsecticideFill == 1 & OtherIcideFill != 1, 1, 0))

# Step 1: Create 'Herbicide' column (if typepesticide_cd == "M0")
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    Herbicide = ifelse(typepesticide_cd == "M0", 1, 0)
  )

# Step 2: Fill down 'Herbicide' values for each 'prodno'
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(Herbicide)) %>%
  mutate(
    HerbicideFill = ifelse(Herbicide == 1, 1, NA)
  ) %>%
  fill(HerbicideFill, .direction = "down")

# Step 3: Create 'OtherIcideHerb' column (Herbicide == 0, Adjuvant == 0, Fertilizer == 0)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    OtherIcideHerb = ifelse(Herbicide == 0 & Adjuvant == 0 & Fertilizer == 0, 1, 0)
  )

# Step 4: Sort by prodno and 'OtherIcideHerb' (descending)
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(OtherIcideHerb))

# Step 5: Fill down 'OtherIcideHerb' values
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    OtherIcideHerbFill = ifelse(OtherIcideHerb == 1, 1, NA)
  ) %>%
  fill(OtherIcideHerbFill, .direction = "down")

# Step 6: Create 'HerbOnly' column (HerbicideFill == 1 and OtherIcideHerbFill != 1)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    HerbOnly = ifelse(HerbicideFill == 1 & OtherIcideHerbFill != 1, 1, 0)
  )

# Step 1: Create 'Fungicide' column (if typepesticide_cd == "K0")
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    Fungicide = ifelse(typepesticide_cd == "K0", 1, 0)
  )

# Step 2: Fill down 'Fungicide' values for each 'prodno'
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(Fungicide)) %>%
  mutate(
    FungicideFill = ifelse(Fungicide == 1, 1, NA)
  ) %>%
  fill(FungicideFill, .direction = "down")

# Step 3: Create 'OtherIcideFung' column (Fungicide == 0, Adjuvant == 0, Fertilizer == 0)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    OtherIcideFung = ifelse(Fungicide == 0 & Adjuvant == 0 & Fertilizer == 0, 1, 0)
  )

# Step 4: Sort by prodno and 'OtherIcideFung' (descending)
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno, desc(OtherIcideFung))

# Step 5: Fill down 'OtherIcideFung' values
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    OtherIcideFungFill = ifelse(OtherIcideFung == 1, 1, NA)
  ) %>%
  fill(OtherIcideFungFill, .direction = "down")

# Step 6: Create 'FungOnly' column (FungicideFill == 1 and OtherIcideFungFill != 1)
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    FungOnly = ifelse(FungicideFill == 1 & OtherIcideFungFill != 1, 1, 0)
  )

# Creating the InsectFung column: InsecticideFill == 1 and Fungicide == 1
pesticide_use_summary <- pesticide_use_summary %>%
  mutate(
    InsectFung = ifelse(FungicideFill == 1 & Fungicide == 1, 1, 0)
  )

# Sorting by prodno
pesticide_use_summary <- pesticide_use_summary %>%
  arrange(prodno)

# Collapse data by prodno, keeping the first occurrence for specific columns and max for others
pesticide_use_summary_collapsed <- pesticide_use_summary %>%
  group_by(prodno) %>%
  summarise(
    InsectAll = first(InsectAll, order_by = prodno),
    InsectOnly = first(InsectOnly, order_by = prodno),
    HerbOnly = first(HerbOnly, order_by = prodno),
    FungOnly = first(FungOnly, order_by = prodno),
    Herbicide = max(Herbicide, na.rm = TRUE),
    Fungicide = max(Fungicide, na.rm = TRUE),
    InsectFung = max(InsectFung, na.rm = TRUE)
  )

# Dropping rows where prodno is missing or equal to -1
pesticide_use_summary_collapsed <- pesticide_use_summary_collapsed %>%
  filter(!is.na(prodno) & prodno != -1)
write.table(pesticide_use_summary_collapsed, here("data/PUR_product_database/KernPURPesticideType24/KernPURPesticideType24.csv"), row.names = FALSE, sep = ",")

products <- read_csv(here("data/PUR_reports/ftp_files_2022/product.txt"))
wei_organic <- read_csv(here("data/PUR_product_database/wei_data/wei_organic.csv"))

##################################
# Getting kern county field boundaries
kern_county <- st_read(here("data/CalAgPermits/FieldBoundaries/field_boundaries_15_20240507/field_boundaries_15.shp")) %>%
  clean_names

kern_county_clean <- as.data.frame(kern_county)
kern_county_clean$permit_yr <- as.numeric(kern_county_clean$permit_yr)

# Filtering by 2022 and issued permits
kern_2022 <- kern_county_clean %>% 
  filter(status == "Issued", permit_yr == 2022)

# Filtering (multi-year permits)
kern_2022_filtered <- kern_2022 %>% 
  filter(p_eff_date <= as.Date("2022-12-31") & 
           p_exp_date >= as.Date("2022-01-01") & 
           status == "Issued")

# Resolve overlaps
kern_2022_filtered <- kern_2022_filtered %>%
  group_by(site_id) %>%
  slice_max(calc_acres) %>%  # Retain the polygon with the largest calculated acreage
  ungroup()

summary(kern_2022_filtered)
plot(kern_2022_filtered["calc_acres"])  # Plot polygons by acreage

# Kern county is county code 15
udc22_15 <- read_csv(here("data/PUR_reports/ftp_files_2022/udc22_15.txt"))




