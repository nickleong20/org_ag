library(sf)
library(ggplot2)
library(tidyverse)


StatePUR1121_county15 <- read_csv("StatePUR1121.csv") %>%
  filter(county_cd == 15) 
county_15 <- st_read("data/CalAgPermits/FieldBoundaries/field_boundaries_15_20240507/field_boundaries_15.shp")

test <- StatePUR1121_county15 %>%
  filter(grepl("M25S27E24$", comtrs))
test2 <- county_15 %>%
  filter(mtrs == "M25S27E24")
