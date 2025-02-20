library(dplyr)
library(here)
library(tidyverse)
library(readr)
library(janitor)

rm(list = ls())

# Define data paths
orgPrd_file <- "data/PUR_product_database/wei_data/OrgPrd_Wei_EcoEcon_2024.csv"
product_file_template <- "data/PUR_reports/pur{year}/product.txt"
kernPUR_file <- "data/PUR_product_database/KernPURPesticideType24/KernPURPesticideType24.csv"
orgChem_file <- "data/PUR_product_database/wei_data/active_ing_allowed_organic_wei.csv"


# Set year range
PURYr <- data.frame(year = 2011:2014, Yr = 11:14)
prodType_list <- list()
udc_combined <- list()



# For testing only
# year <- PURYr$year
# Yr <- PURYr$Yr

# Loop over years
for (year in PURYr$year) {
  # Import Wei data
  orgPrd <- read_csv(orgPrd_file) %>%
    clean_names() %>%
    filter(!is.na(prodno)) %>%
    mutate(orgPrd = 1)
  
  # Import product.txt for each year 
  product_file <- glue::glue(product_file_template, year = year)
  product_data <- read_csv(product_file) %>% clean_names()
  
  # Merge orgPrd with prodno and indicate 'Y' for organics if match
  merged_data_PUR_org <- product_data %>%
    full_join(orgPrd, by = "prodno") %>%
    mutate(
      mergeOrg = case_when(
        is.na(product_name.x) ~ "2", # Exists only in orgPrd
        is.na(product_name.y) ~ "1", # Exists only in product_data
        TRUE ~ "3"                  # Exists in both
      ),
      orgPrd = if_else(mergeOrg == "1", 0, orgPrd) # Replace orgPrd with 0 if mergeOrg == "1"
    )
  
  # Merge with KernPUR pesticide data
  kernPUR_data <- read_csv(kernPUR_file) %>% clean_names()
  kernPUR_data$prodno <- as.integer(kernPUR_data$prodno)
  merged_data <- merged_data_PUR_org %>%
    left_join(kernPUR_data, by = "prodno") %>%
    mutate(
      mergePrd = case_when(
        is.na(herb_only) & is.na(insect_only) & is.na(insect_all) & is.na(fung_only) & is.na(herbicide) & is.na(fungicide) & is.na(insect_fung) ~ "2", # Exists only in kernPUR_data
        is.na(herb_only) | is.na(insect_only) | is.na(insect_all) | is.na(fung_only) | is.na(herbicide) | is.na(fungicide) | is.na(insect_fung) ~ "1", # Exists only in merged_data
        TRUE ~ "3"  # Exists in both
      )
    )
  
  # Save processed data to a list
  prodType_list[[paste0("prodType_")]] <- merged_data
  
  # Make sure as.character
  prodType_list$prodType_$full_exp_dt <- as.character(prodType_list$prodType_$full_exp_dt) # Was not merging bc wrong type. Only for these two years
  
  combined_prodType <- bind_rows(prodType_list, .id = "year")
  
  #### Export merged data after the first merge
  write_csv(merged_data, glue::glue("prodType_{year}.csv"))
  
  
  # Process and clean the OrgChem data
  orgChem <- read_csv(orgChem_file) %>% clean_names() %>%
    filter(!is.na(chem_code)) %>% # Drop rows with missing chem_code
    mutate(orgChem = 1)
  
  orgChem <- orgChem %>%
    group_by(chem_code) %>%
    slice(1) %>%
    ungroup()
  
  #### Export merged data 
  write_csv(orgChem, glue::glue("orgChem_{year}.csv"))
  
  
  f <- list()
  
  # Loop over years from 11 to 22
  # for (year in PURYr) {
  root_dir <- "data/PUR_reports"
  
  # Generate the directory path for the current year
  year_dir <- file.path(root_dir, paste0("pur", year))
  
  # Generate the file pattern for the current year
  file_pattern <- paste0("udc", substr(year, 3, 4), "_.*\\.txt$") # _.*\\.txt$ for all; _15.txt for one 
  
  # Generate the full file path
  filenames <- list.files(year_dir, pattern = file_pattern, full.names = TRUE)
  
  # Store the filenames in the list
  f[[paste0("file")]] <- filenames
  
  
  f <- as.data.frame(f)
  building_list <- list()
  
  # for testing
  # f <- f$file[1]
  
  for (file in f$file) { 
    year_collapsed_data <- data.frame()
    
    udc_data <- read_csv(file) %>% 
      clean_names() %>%
      mutate(source = basename(file)) %>%
      filter(record_id %in% c("1", "4", "A", "B", "E", "F")) %>%
      mutate(permit = substr(grower_id, 5, nchar(grower_id)),
             permitsite = paste(permit, site_loc_id, sep = "")) 
    
    udc_data <- udc_data[!udc_data$permitsite == "", ] 
    
    udc_date <- lubridate::mdy(udc_data$applic_dt) # Date conversion
    
    udc_data <- udc_data %>%
      mutate(
        applic_dt = format(udc_date, "%m-%d-%Y"), 
        year = substr(applic_dt, 7, nchar(applic_dt)), # adding year column
        comtrs_original = comtrs, # comtrs exists, creating comtrs_original column
        comtrs = if_else(is.na(comtrs) | comtrs == "", "", as.character(comtrs)) # check if comtrs is NA or empty, replace with ""
      )
    
    # Destringing
    numeric_columns <- c("use_no", "prodno", "chem_code", "lbs_chm_used", "lbs_prd_used", "acre_treated", "site_code", "year")
    udc_data[numeric_columns] <- lapply(udc_data[numeric_columns], as.numeric)
    
    # Sort the dataset by `prodno`
    udc_data <- udc_data %>% arrange(prodno)
    
    # Add 3 empty observations (rows)
    empty_rows <- data.frame(matrix(NA, nrow = 3, ncol = ncol(udc_data)))
    names(empty_rows) <- names(udc_data)
    empty_rows$year[is.na(empty_rows$year)] <- 1
    # Set specific columns for rows where `year` == 1
    empty_rows$use_no[empty_rows$year == 1] <- 1
    empty_rows$permitsite[empty_rows$year == 1] <- "1"
    empty_rows$site_code[empty_rows$year == 1] <- 1
    
    udc_data <- bind_rows(udc_data, empty_rows)
    
    # Assuming orgChem is the dataframe you want to merge with the current dataframe
    merged_udc_data <- full_join(udc_data, orgChem, by = "chem_code") %>% 
      mutate(
        mergeOrgChem = case_when(
          !is.na(active_ingredient_name) & is.na(year) ~ 2,  # From orgChem only
          is.na(active_ingredient_name) & !is.na(year) ~ 1,  # From udc_data only
          !is.na(active_ingredient_name) & !is.na(year) ~ 3, # From both datasets
        )
      )
    
    merged_udc_data$orgChem[merged_udc_data$mergeOrgChem == 1] <- 0
    
    collapsed_data <- merged_udc_data %>%
      group_by(use_no, year) %>%
      summarise(
        lbs_prd_used = first(lbs_prd_used),
        lbs_chm_used = sum(lbs_chm_used, na.rm = TRUE),
        acre_treated = first(acre_treated),
        unit_treated = first(unit_treated),
        base_ln_mer = first(base_ln_mer),
        county_cd = first(county_cd),
        township = first(township),
        range = first(range),
        section = first(section),
        tship_dir = first(tship_dir),
        range_dir = first(range_dir),
        permit = first(permit),
        site_loc_id = first(site_loc_id),
        permitsite = first(permitsite),
        applic_dt = first(applic_dt),
        prodno = first(prodno),
        site_code = first(site_code),
        comtrs = min(comtrs, na.rm = TRUE),
        orgChem = min(orgChem)
      )
    # Warning message:
    # There were 2 warnings in `summarise()`.
    # The first warning was:
    #   ℹ In argument: `comtrs = min(comtrs, na.rm = TRUE)`.
    # ℹ In group 1: `use_no = 1` and `year = 1`.
    # Caused by warning in `min()`:
    #   ! no non-missing arguments, returning NA
    
    #### Export merged data 
    g <- str_extract(file, "\\d+(?=\\.txt$)")
    write_csv(product_data, glue::glue("first_collapse_{year}_{g}.csv"))
    
    collapsed_data$acre_treated[collapsed_data$unit_treated == "U"] <- NA
    
    # Formatting section, township, and range as two-digit strings
    collapsed_data$section <- sprintf("%02d", as.numeric(collapsed_data$section))
    collapsed_data$township <- sprintf("%02d", as.numeric(collapsed_data$township))
    collapsed_data$range <- sprintf("%02d", as.numeric(collapsed_data$range))
    
    # Creating comtrs by concatenating several variables
    collapsed_data$comtrs <- ifelse(collapsed_data$comtrs == "", 
                                    paste(collapsed_data$county_cd, collapsed_data$base_ln_mer, 
                                          collapsed_data$township, collapsed_data$tship_dir, 
                                          collapsed_data$range, collapsed_data$range_dir, 
                                          collapsed_data$section, sep = ""), 
                                    collapsed_data$comtrs)
    
    # Creating CDPR_TR by concatenating selected variables
    collapsed_data$CDPR_TR <- if_else(
      is.na(collapsed_data$township) | is.na(collapsed_data$tship_dir) | 
        is.na(collapsed_data$range) | is.na(collapsed_data$range_dir),
      "", 
      paste(collapsed_data$township, collapsed_data$tship_dir, 
            collapsed_data$range, collapsed_data$range_dir, sep = "")
    )
    
    # Remove rows where comtrs or CDPR_TR contain NA (missing data)
    collapsed_data <- collapsed_data[!is.na(collapsed_data$comtrs) & !is.na(collapsed_data$CDPR_TR), ]
    
    # Remove leading zeros from 'site_loc_id' for 2013
    collapsed_data$site_loc_id0 <- collapsed_data$site_loc_id
    collapsed_data$site_loc_id0 <- ifelse(collapsed_data$year == 2013, sub("^0+", "", collapsed_data$site_loc_id), collapsed_data$site_loc_id0)
    
    # Concatenate 'permit' and 'site_loc_id0' to create 'permitsite' for rows where Yr == 2013
    collapsed_data$permitsite <- ifelse(collapsed_data$year == 2013, paste(collapsed_data$permit, collapsed_data$site_loc_id0, sep = ""), collapsed_data$permitsite)
    
    # 1. Encode 'base_ln_mer' as a factor (categorical) and create 'base_ln_mer2'
    collapsed_data$base_ln_mer2 <- as.numeric(factor(collapsed_data$base_ln_mer))
    
    # 2. Drop 'base_ln_mer', 'tship_dir', and 'range_dir' (remove columns)
    collapsed_data <- collapsed_data[, !(names(collapsed_data) %in% c("base_ln_mer", "tship_dir", "range_dir"))]
    
    # 3. Convert 'county_cd', 'section', 'range', and 'township' to numeric
    collapsed_data$county_cd <- as.numeric(collapsed_data$county_cd)
    collapsed_data$section <- as.numeric(collapsed_data$section)
    collapsed_data$range <- as.numeric(collapsed_data$range) # NAs introduced by coercion
    collapsed_data$township <- as.numeric(collapsed_data$township) # NAs introduced by coercion
    
    # Step 4: Sort the dataset by 'prodno'
    collapsed_data <- collapsed_data %>% arrange(prodno)
    
    merged_collapsed <- merge(collapsed_data, combined_prodType[, c("prodno", "signlwrd_ind", "fumigant_sw", "show_regno", 
                                                                    "product_name.x", "orgPrd", "fung_only", "fungicide", 
                                                                    "herb_only", "herbicide", "insect_all", "insect_only", 
                                                                    "insect_fung","mergePrd", "mergeOrg" )],
                              by = "prodno", all.x = TRUE)
    
    #### Export merged data 
    write_csv(merged_collapsed, glue::glue("second_collapse_{year}_{g}.csv"))
    
    # Create the _merge classification based on the conditions
    merged_collapsed$merge <- case_when(
      # Case 2: Only in combined_prodType (orgPrd is not NA)
      is.na(merged_collapsed$orgPrd) ~ "2", 
      
      # Case 1: Only in collapsed_data (any of these columns are NA)
      is.na(merged_collapsed$fung_only) | 
        is.na(merged_collapsed$fungicide) | 
        is.na(merged_collapsed$herb_only) | 
        is.na(merged_collapsed$herbicide) | 
        is.na(merged_collapsed$insect_all) | 
        is.na(merged_collapsed$insect_only) | 
        is.na(merged_collapsed$insect_fung) ~ "1", 
      
      # Case 3: Exists in both collapsed_data and combined_prodType (no NA in the relevant columns)
      TRUE ~ "3"
    )
    
    # Count the rows where _merge == 1
    count_merge_1 <- sum(merged_collapsed$merge == "1")
    print(paste("Number of rows with _merge == 1: ", count_merge_1))
    
    # Drop rows where _merge != 3
    merged_collapsed <- merged_collapsed[merged_collapsed$merge == "3", ]
    
    # Drop the _merge column
    merged_collapsed$merge <- NULL
    
    new_rows <- data.frame(matrix(NA, nrow = 3, ncol = ncol(merged_collapsed)))
    names(new_rows) <- names(merged_collapsed)
    
    merged_collapsed <- rbind(merged_collapsed, new_rows)
    
    # Replace year with 1 where it is missing
    merged_collapsed$year[is.na(merged_collapsed$year)] <- 1
    
    # Replace use_no, permitsite, site_code, show_regno with specific values when year == 1
    merged_collapsed$use_no[merged_collapsed$year == 1] <- 1
    merged_collapsed$permitsite[merged_collapsed$year == 1] <- "1"
    merged_collapsed$site_code[merged_collapsed$year == 1] <- 1
    merged_collapsed$show_regno[merged_collapsed$year == 1] <- "1-1"
    
    # Split 'show_regno' by "-" into two parts 'reg1' and 'reg2'
    merged_collapsed <- merged_collapsed %>%
      tidyr::separate(show_regno, into = c("reg1", "reg2"), sep = "-") # Warning message: Expected 2 pieces. Additional pieces discarded in 46944 rows [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, ...].
    
    # Create 'eparegno' by concatenating 'reg1' and 'reg2'
    merged_collapsed$eparegno <- paste(merged_collapsed$reg1, merged_collapsed$reg2, sep = "-")
    
    # Remove any spaces from 'eparegno'
    merged_collapsed$eparegno <- gsub(" ", "", merged_collapsed$eparegno)
    
    # Drop the 'reg1' and 'reg2' columns
    merged_collapsed <- merged_collapsed %>%
      dplyr::select(-reg1, -reg2)
    
    # Create 'Fumig' based on the 'fumigant_sw' column
    merged_collapsed$Fumig <- merged_collapsed$fumigant_sw == "X"
    
    # 1. Loop through columns that start with Insect*, Herb*, Fung*, and Fumig to create Prd and AI columns
    # Get the list of column names that match the desired prefixes
    varlist <- grep("^(insect|herb|fung|fumig|Fumig)", names(merged_collapsed), value = TRUE)
    
    # Loop over the varlist and generate the new columns
    for (x in varlist) {
      # Create Prd and AI columns based on the condition
      merged_collapsed[[paste0("Prd", x)]] <- ifelse(merged_collapsed[[x]] == 1, merged_collapsed$lbs_prd_used, NA)
      merged_collapsed[[paste0("AI", x)]] <- ifelse(merged_collapsed[[x]] == 1, merged_collapsed$lbs_chm_used, NA)
    }
    
    # 2. Create PrdOrg and AIOrg based on orgPrd or orgChem
    # Create 'PrdOrg' where 'orgPrd == 1' or 'orgChem == 1'
    merged_collapsed <- merged_collapsed %>%
      mutate(PrdOrg = ifelse(orgPrd == 1 | orgChem == 1, lbs_prd_used, NA),
             AIOrg = ifelse(orgPrd == 1 | orgChem == 1, lbs_chm_used, NA))
    
    for (col_name in colnames(merged_collapsed)) {
      if (grepl("^Prd|^lbs_prd_used|^lbs_chm_used|^AI", col_name)) {
        # Replace missing values with 0 and convert to kilograms
        merged_collapsed <- merged_collapsed %>%
          mutate(!!col_name := ifelse(is.na(get(col_name)), 0, get(col_name)), 
                 !!paste0("Kg", col_name) := get(col_name) * 0.453592)
      }
    }
    
    # 4. Generate treatedha and treatedhaOrg columns
    merged_collapsed <- merged_collapsed %>%
      mutate(treatedha = acre_treated * 0.404686,
             treatedhaOrg = ifelse(orgPrd == 1 | orgChem == 1, treatedha, NA))
    
    # Sort the dataframe
    merged_collapsed <- merged_collapsed %>%
      arrange(permitsite, year, site_code)
    
    # Summarize the dataset (equivalent to collapse in Stata)
    collapsed_summary <- merged_collapsed %>%
      group_by(permitsite, year, site_code) %>%
      summarise(
        KgPrdPest = sum(Kglbs_prd_used, na.rm = TRUE),
        KgPrdFungicide = sum(KgPrdfungicide, na.rm = TRUE),
        KgPrdHerbicide = sum(KgPrdherbicide, na.rm = TRUE),
        KgPrdInsecticide = sum(KgPrdinsect_all, na.rm = TRUE),
        KgPrdInsectOnly = sum(KgPrdinsect_only, na.rm = TRUE),
        KgPrdHerbOnly = sum(KgPrdherb_only, na.rm = TRUE),
        KgPrdFungOnly = sum(KgPrdfung_only, na.rm = TRUE),
        KgPrdInsectFung = sum(KgPrdinsect_fung, na.rm = TRUE),
        KgAIPest = sum(Kglbs_chm_used, na.rm = TRUE),
        KgAIFungicide = sum(KgAIfungicide, na.rm = TRUE),
        KgAIHerbicide = sum(KgAIherbicide, na.rm = TRUE),
        KgAIInsecticide = sum(KgAIinsect_all, na.rm = TRUE),
        KgAIInsectOnly = sum(KgAIinsect_only, na.rm = TRUE),
        KgAIHerbOnly = sum(KgAIherb_only, na.rm = TRUE),
        KgAIInsectFung = sum(KgAIinsect_fung, na.rm = TRUE),
        KgAIFungOnly = sum(KgAIfung_only, na.rm = TRUE),
        permit = first(permit),
        site_loc_id = first(site_loc_id),
        comtrs = first(comtrs),
        CDPR_TR = first(CDPR_TR),
        treatedha = sum(treatedha, na.rm = TRUE),
        PURFieldSize = max(acre_treated, na.rm = TRUE),
        orgSpray = sum(!is.na(treatedhaOrg)),  # Count non-missing values
        allSpray = sum(!is.na(acre_treated)),  # Count non-missing values
        KgPrdOrg = sum(KgPrdOrg, na.rm = TRUE),
        KgAIOrg = sum(KgAIOrg, na.rm = TRUE),
        KgPrdFumig = sum(KgPrdFumig, na.rm = TRUE),
        KgAIFumig = sum(KgAIFumig, na.rm = TRUE),
        county_cd = first(county_cd)  # Keep the first county code
      ) %>%
      ungroup()
    
    #### Export merged data 
    write_csv(collapsed_summary, glue::glue("third_collapse_{year}_{g}.csv"))
    
    # code for how to drop values from year column equal to 1
    collapsed_summary <- collapsed_summary %>%
      filter(year != 1)
    
    standardize_types <- function(df) {
      df <- df %>%
        mutate(
          permitsite = as.character(permitsite),
          year = as.integer(year),
          site_code = as.integer(site_code),
          KgPrdPest = as.numeric(KgPrdPest),
          KgPrdFungicide = as.numeric(KgPrdFungicide),
          KgPrdHerbicide = as.numeric(KgPrdHerbicide),
          KgPrdInsecticide = as.numeric(KgPrdInsecticide),
          KgAIPest = as.numeric(KgAIPest),
          permit = as.character(permit),
          site_loc_id = as.character(site_loc_id),
          comtrs = as.character(comtrs),
          CDPR_TR = as.character(CDPR_TR),
          treatedha = as.numeric(treatedha),
          PURFieldSize = as.numeric(PURFieldSize),
          orgSpray = as.integer(orgSpray),
          allSpray = as.integer(allSpray),
          county_cd = as.integer(county_cd)  # Assuming county codes are integers
        )
      return(df)
    }
    
    collapsed_summary <- standardize_types(collapsed_summary)
    
    # Add the collapsed data for this file to the year's data
    building_list <- bind_rows(building_list, collapsed_summary)
    
    # Get all objects starting with "StatePUR"
    
    
  }
  assign(paste0("StatePUR_", year), building_list) 
  
  #### Export merged data 
  write_csv(building_list, glue::glue("StatePUR_{year}.csv"))
  
} 

# Combine all the dataframes into one
appendYears <- bind_rows(mget(ls(pattern = "^StatePUR_")), .id = "year")
# Save merged data as csv
write_csv(appendYears, "appendYears.csv")

###################################################

appendYears <- read_csv("appendYears.csv")

# Ensure orgField is correctly calculated
orgLag <- appendYears %>%
  mutate(orgField = (orgSpray == allSpray) & ((KgAIPest - KgAIOrg) < 1))

# Collapse to minimum orgField by permitsite and year
orgLag <- orgLag %>%
  group_by(permitsite, year) %>%
  summarise(orgField = min(orgField, na.rm = TRUE), .groups = "drop")

# Generate unique site-year identifier
orgLag <- orgLag %>%
  mutate(rpermitsite = as.numeric(as.factor(permitsite)))

# Drop missing years
orgLag <- orgLag %>% filter(!is.na(year))

# Time series filling (assuming filling missing years within each site)
orgLag <- orgLag %>%
  complete(rpermitsite, year, fill = list(orgField = NA))

# Creating lags for 1 and 2 years
orgLag <- orgLag %>%
  arrange(rpermitsite, year) %>%
  group_by(rpermitsite) %>%
  mutate(
    orgField_L1 = lag(orgField, 1),
    orgField_L2 = lag(orgField, 2)
  )

# Counting number of observed years per site
orgLag <- orgLag %>%
  group_by(rpermitsite) %>%
  mutate(NumCult = sum(!is.na(permitsite))) 

# Replacing missing values for lagged organic fields where the site was observed in at least 2 years
orgLag <- orgLag %>%
  mutate(
    orgField_L1 = ifelse(NumCult > 1, orgField, orgField_L1),
    orgField_L2 = ifelse(NumCult > 1, orgField, orgField_L2)
  )

# Sum of organic fields across lags
orgLag <- orgLag %>%
  mutate(orgFieldLags = rowSums(across(starts_with("orgField")), na.rm = TRUE),
         orgField3y = as.integer(orgFieldLags == 3))

# Collapse by permitsite and year
orgLag <- orgLag %>%
  group_by(permitsite, year) %>%
  summarise(
    orgField = min(orgField, na.rm = TRUE),
    orgField3y = min(orgField3y, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(permitsite != "")

write.csv(orgLag, "orgLag.csv", row.names = FALSE)

StatePUR1121 <- appendYears %>%
  full_join(orgLag, by = c("permitsite", "year"))

write.csv(StatePUR1121, "StatePUR1121.csv", row.names = FALSE)

test <- read.csv("StatePUR1121.csv")

oop <- test %>%
  filter(permitsite == "27014530400010")

