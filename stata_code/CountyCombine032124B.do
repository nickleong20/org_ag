/*
3/24/24
Combining counties.
Ran years mostly individually, from 2012-2021. Should work fine in earlier and later years.
The key is to add some observations to counties that have no ag observations so they don't hit a snag being empty datasets


*/

capture log close
set more off
log using /Users/ashleylarsen/GoogleDrive/Stata/CountyCombine_0324.log,text replace

/*
****************
*Process & append years
*takes ~2h, adjust screensaver.
****************
clear all

local PURYr "2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021" 
foreach Yr in `PURYr' {
	
	***first generate product tables Table A.2 and product type
	import delimited /Users/ashleylarsen/GoogleDrive/PURdata/OrgPrd_Wei_EcoEcon_2024.csv, case(preserve) clear
	drop if prodno == .
	tempfile orgPrd
	save `orgPrd', replace
	
	import delimited /Users/ashleylarsen/GoogleDrive/PURdata/pur`Yr'/product.txt, clear
	merge 1:1 prodno using `orgPrd', keepus(orgPrd) gen(mergeOrg)
	replace orgPrd = 0 if mergeOrg==1
	
	*from KernPURPesticideType in spatialorganics.
	merge 1:1 prodno using /Users/ashleylarsen/GoogleDrive/Stata/KernPURPesticideType, keepus() gen(mergePrd)
	tempfile prodType_`Yr'
	save `prodType_`Yr'', replace
	***
	*Seperately bring in chemicals that are organic, Table A.1
	import delimited /Users/ashleylarsen/GoogleDrive/PURdata/OrgChem_Wei_EcoEcon_2024.csv, case(preserve) clear
	drop if chem_code == .
	collapse (first) orgChem, by(chem_code)
	tempfile orgChem
	save `orgChem', replace
	
	
clear 
cd /Users/ashleylarsen/GoogleDrive/PURdata/pur`Yr'/udc


tempfile building_`Yr'
save `building_`Yr'', emptyok
*set trace on
* List all the ".txt" files in folder "${route}"
local filenames : dir "$udc" files "*.txt"
* loop over all files, while appending them 
foreach f of local filenames {
    import delimited using `"`f'"' ,varnames(1) case(preserve) stringcols(_all) clear
    *it identifies the procedence of the rows (i.e., the .xlsx)
    gen source = `"`f'"'
    listsome county_cd, max(2)
	*keeping daily, monthly production ag (almost all are A and B)
	keep if record_id =="1" | record_id=="4"| record_id=="A"| record_id =="B" | record_id=="E"| record_id=="F"
	
	*permitsite
	gen permit = substr(grower_id,5,.)
	gen permitsite = permit+site_loc_id
	drop if permitsite ==""

	*date
	gen applicationdate = date(applic_dt, "MD20Y")
	gen year = `Yr'

	
	*creating comtrs
	capture gen comtrs =""
	
	*destring key variables
	destring use_no prodno chem_code lbs_chm_used lbs_prd_used acre_treated site_code year, replace
	sort prodno
	

	*dealing with udc that have no ag observations. Can't collapse, append empty datasets. Adding 3 observation with year = 1 at end of dataset
	insobs 3
	replace year = 1 if year == .
	replace use_no = 1 if year==1
	replace permitsite = "1" if year==1
	replace site_code = 1 if year==1
	
	merge m:1 chem_code using `orgChem', keepus(orgChem) gen(mergeOrgChem)
	replace orgChem = 0 if mergeOrgChem==1
	
	*Note, acres treated is a misnomer. If it is misc units (U), it isn't converted. sqft are converted.
	*In collapse, take mimimum of orgChem so only products with all org get a 1. 
	collapse (first) lbs_prd_used (sum) lbs_chm_used (first) acre_treated (first) unit_treated (first) base_ln_mer (first) county_cd (first) township (first) range (first) section (first) tship_dir (first) range_dir (first) permit (first) site_loc_id (first) permitsite (first) applicationdate (first) prodno (first) site_code (first) comtrs (min) orgChem, by(use_no year)

	*don't know what unit treated = U means. Removing it from acres treated. 
	count if unit_treated == "U"
	replace acre_treated = . if unit_treated == "U"
	
	replace section = string(real(section),"%02.0f")
	replace township = string(real(township),"%02.0f")
	replace range = string(real(range),"%02.0f")
	replace comtrs = county_cd+ base_ln_mer +township+ tship_dir + range + range_dir+section if comtrs==""
	gen CDPR_TR= township+ tship_dir + range + range_dir
	replace comtrs = "" if strpos(comtrs, "?")
	replace comtrs = "" if strpos(comtrs, ".")
	replace CDPR_TR = "" if strpos(CDPR_TR, "?")
	replace CDPR_TR = "" if strpos(CDPR_TR, ".")
	
	*2013 has leading 0s
	gen site_loc_id0 = usubstr(site_loc_id, indexnot(site_loc_id, "0"), .)
	replace permitsite = permit +site_loc_id0 if `Yr' ==2013
	
	encode base_ln_mer, generate(base_ln_mer2)
	drop base_ln_mer tship_dir range_dir 
	destring county_cd section range township, replace

	sort prodno
	*merging here to avoid a big file later, but inefficient either way. 
	merge m:1 prodno using `prodType_`Yr'',keepus(signlwrd_ind fumigant_sw show_regno product_name orgPrd FungOnly Fungicide HerbOnly Herbicide InsectAll InsectOnly  InsectFung mergePrd mergeOrg)
	count if _merge==1
	drop if _merge!=3
	drop _merge

	*dealing with udc that have no ag observations. Can't split, collapse, append empty datasets. Adding 3 observation with year = 1 at end of dataset
	insobs 3
	replace year = 1 if year == .
	replace use_no = 1 if year==1
	replace permitsite = "1" if year==1
	replace site_code = 1 if year==1
	replace show_regno = "1-1" if year==1
	
	
	*creating eparegno
	split show_regno, gen(reg) parse("-")
	gen eparegno = reg1+"-"+reg2
	replace eparegno = subinstr(eparegno, " ", "",.)	
	drop reg*
	
	*create organic = lbs_Ai if org==1, and insecticides etc etc 
	gen Fumig = (fumigant_sw=="X")
	
	
	
	foreach x of varlist Insect* Herb* Fung* Fumig {		
	  gen Prd`x'= lbs_prd_used if `x'==1
	  gen AI`x' = lbs_chm_used if `x' ==1
	}
	
	gen PrdOrg = lbs_prd_used if orgPrd==1 | orgChem==1
	gen AIOrg = lbs_chm_used if orgPrd==1 | orgChem==1
	
	*Convert to Kg
	foreach x of varlist Prd* lbs_prd_used lbs_chm_used AI*  {	
	  replace `x' = 0 if missing(`x') 
	  gen Kg`x' = `x' * 0.453592
	}
	
	gen treatedha = (acre_treated * 0.404686)
	gen treatedhaOrg = treatedha if orgPrd==1 | orgChem==1
	
	summ Kg* year county_cd
	count if KgPrdOrg >0
	unique permitsite year site_code
	unique permitsite year
	unique permit year
	
	*describe
	*Collapsing for permitsite year site_code. 
	sort permitsite year site_code
	collapse (sum) KgPrdPest = Kglbs_prd_used (sum) KgPrdFungicide  (sum) KgPrdHerbicide (sum) KgPrdInsecticide = KgPrdInsectAll (sum) KgPrdInsectOnly (sum) KgPrdHerbOnl (sum) KgPrdFungOnly  (sum) KgPrdInsectFung (sum) KgAIPest=Kglbs_chm_used (sum) KgAIFungicide  (sum) KgAIHerbicide (sum) KgAIInsecticide = KgAIInsectAll  (sum) KgAIInsectOnly (sum) KgAIHerbOnly (sum) KgAIInsectFung (sum) KgAIFungOnly (first) permit (first) site_loc_id (first) comtrs (first) CDPR_TR (sum) treatedha (max) PURFieldSize = acre_treated (count) orgSpray = treatedhaOrg (count) allSpray = acre_treated (sum) KgPrdOrg (sum) KgAIOrg (sum) KgPrdFumig (sum) KgAIFumig (first) county_cd, by(permitsite year site_code)


	*describe
	* append new rows 
	append using `building_`Yr''
    save `"`building_`Yr''"', replace
	}
*saving each complete year
drop if year==1
tempfile StatePUR`Yr'
save `StatePUR`Yr''
	
}

*******************
**Organic pmtsite
*******************
*append years together, saving tempfile
*calculate organic for 1 year. Watch rounding. 
*tsset and lag, 3y

use `StatePUR2011', clear
append using `StatePUR2012'

tempfile appendYears
save `appendYears', replace

local PURYr "2013 2014 2015 2016 2017 2018 2019 2020 2021" 
foreach Yr in `PURYr' {
	use `StatePUR`Yr'', clear
	append using `appendYears'
	save `appendYears', replace
}
* will merge back with this later. 
*saving now, update below.
save /Users/ashleylarsen/GoogleDrive/Stata/StatePUR1121, replace
*/
******************

*don't care what crop it is, want to know if it was organic. 
gen orgField = ((orgSpray == allSpray) & ((KgAIPest-KgAIOrg)<1))
collapse (min) orgField, by(permitsite year)
egen rpermitsite = group(permitsite)
*drop if year==. (handfull)
count if year ==.
drop if year==.
tsset rpermitsite year
tsfill, full

*creating lag of organic
forvalues i = 1/2 {
    gen orgField_L`i' = L`i'.orgField
}

*basically assuming that it was fallow in 2y prior if the permitsite DNE, but the permitsite has been observed in at least 2y. 
*so if its conv in year = t, it is in year L1-2 if those are fallowed per above. If organic, organic. 
sort rpermitsite year
by rpermitsite : egen NumCult=sum(permitsite!="")
replace orgField_L1 = orgField if NumCult >1
replace orgField_L2 = orgField if NumCult >1

egen orgFieldLags = rowtotal(orgField*)
gen orgField3y = (orgFieldLags==3)

collapse (min) orgField (min) orgField3y, by(permitsite year)
drop if permitsite == ""
tempfile OrgLag
save `OrgLag', replace

/*******************
*Nicol's Tox Data
*******************
*code originally from KernPURProduct0019xlsxC_CDPR.do in fallowing folder.
*not ready to run. 
*need to figure out how to get back to LD50, then use Nicol's pesticide.csv to hopefully merge with the rest of the PUR data. mass/TI = ld50

import delimited /Users/ashleylarsen/GoogleDrive/Projects/Fallowing_abandonment/KernRetirePesticides_STOTEN/Kern_PesticideActiveIngredients_2005_2021/Kern_PesticideActiveIngredients_2005_2021_PMPM_Output_Annual_Summary.csv, clear

bysort year: summ pesticide_kg ti_net 
gen permitsite = grower_id + site_loc_id if year>=2020 

gen permit  = substr(grower_id,5,.) if year<2020 
replace permit = grower_id if year>=2020 & year!=. 
replace permitsite = permit+site_loc_id if year<2020 
*structural pest control, landscape etc 
count if permitsite=="" 

*2013 has leading 0s in site 
gen site_loc_id0 = substr(site_loc_id, indexnot(site_loc_id, "0"), .) 
replace permitsite = permit+site_loc_id0 if year ==2013 

drop if grower_id=="" | site_loc_id =="" 
* 2 permits have non-numerics ("R"), both landscape--this was in old data from Nicol 
*charlist permit 
*gen StrPermit = strpos(permit, "R") 
*list permit pesticide_kg ti_net site_type if StrPermit!=0 
*drop if StrPermit!=0 
destring permit, replace 

*site_type is caps crop eg ALFALFA
collapse (sum) ti_net (sum) ti_mammal (sum) ti_bird (sum) ti_honeybee (sum) ti_earthworm (sum) pesticide_kg (first) permit , by(permitsite site_type year) 
rename site_type site_name

tempfile NicolTox
save `NicolTox', replace

import delimited /Users/ashleylarsen/GoogleDrive/PURdata/pur2021/site.txt, clear
*there's a dup on bermudagrass, the the state PUR only uses code 22017 so sticking with that. 
drop if site_code== 33017
merge 1:m site_name using `NicolTox', keepus()

tempfile NicolToxSite
save `NicolToxSite', replace

*******************/


use /Users/ashleylarsen/GoogleDrive/Stata/StatePUR1121, clear

*site_code is crop code. 
merge m:1 permitsite year using `OrgLag', keepus(orgField orgField3y) gen(mergeOrgLags)
drop mergeOrgLags

*merge 1:1 permitsite year using `NicolToxSite', keepus(ti* Tox* site*) gen(mergeTox)

*updating save here.
*can get back to the above by dropping orgField and orgField3y.Not really, b/c of collapses
save /Users/ashleylarsen/GoogleDrive/Stata/StatePUR1121, replace

log close



