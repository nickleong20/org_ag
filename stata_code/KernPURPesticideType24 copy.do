

/*
10/26/23 update to just pesticide type
*/


capture log close
set more off
log using KernPURPesticideType24.log,text replace

cd /Users/ashleylarsen/GoogleDrive/PURdata/productdb010324/


********************/
*type_pesticide.dat
********************
insheet using type_pesticide.dat, clear
gen typepesticide_cd=substr(v1,1,2 )
gen typepesticide_dsc = substr(v1,3,. )
sort typepesticide_cd
drop v1
tempfile type_pesticide
save `type_pesticide', replace


*******************
*prod type pesticide
********************
insheet using prod_type_pesticide.dat, clear
*prodno is the first '6' numbers, type pest_cd is the last 2 charactacters.
*to actualy get consistent 6 numbers need to split, then buffer with leading zeros.
*b/c the prodno part is different lengths, working from the back.
*prodno
gen v1a=substr(v1, 1, length(v1) - 2)
destring v1a, gen(prodno)
*typepest_cd
gen typepesticide_cd=substr(v1, length(v1) - 1, length(v1))
sort typepesticide_cd
drop v1*
tempfile prod_type_pest
save `prod_type_pest', replace

merge m:1 typepesticide_cd using `type_pesticide', keepus()
drop _merge
**NEW***
sort prodno 
duplicates tag prodno, gen(dups)
tabulate dups

*N0 = insecticide, O0= miticide, W0 = insect growth regular, R0 = repellent
gen Insecticide = (typepesticide_cd =="N0" | typepesticide_cd =="O0" | typepesticide_cd =="W0"| typepesticide_cd =="R0")
gen Herbicide = (typepesticide_cd == "M0")
gen Fungicide = (typepesticide_cd == "K0")
gen VertControl = (typepesticide_cd == "U0")

*Filling down so any product that has an insecticide component gets a 1
gsort prodno -Insecticide
gen InsecticideFill = Insecticide if Insecticide ==1
bysort prodno (InsecticideFill) : replace InsecticideFill = InsecticideFill[_n-1] if missing(InsecticideFill) & _n > 1

gen InsectAll = (InsecticideFill==1)
gen Adjuvant = (typepesticide_cd == "A0")
gen Fertilizer =  (typepesticide_cd == "J0")

*creating non-acceptable insecticide categories and filling down
gen OtherIcide = (Insecticide ==0 & Adjuvant ==0 & Fertilizer ==0)
gsort prodno -OtherIcide
gen OtherIcideFill = OtherIcide if OtherIcide ==1
bysort prodno (OtherIcideFill) : replace OtherIcideFill = OtherIcideFill[_n-1] if missing(OtherIcideFill) & _n > 1
gen InsectOnly = (InsecticideFill ==1 & OtherIcideFill!=1)

*creating Herbicide Only
*filling down herbicides
gsort prodno Herbicide
gen HerbicideFill = Herbicide if Herbicide ==1
bysort prodno (HerbicideFill) : replace HerbicideFill = HerbicideFill[_n-1] if missing(HerbicideFill) & _n > 1
*creating the "other" category
gen OtherIcideHerb= (Herbicide ==0 & Adjuvant ==0 & Fertilizer ==0)
gsort prodno -OtherIcideHerb
gen OtherIcideHerbFill = OtherIcideHerb if OtherIcideHerb ==1
bysort prodno (OtherIcideHerbFill) : replace OtherIcideHerbFill = OtherIcideHerbFill[_n-1] if missing(OtherIcideHerbFill) & _n > 1
gen HerbOnly = (HerbicideFill ==1 & OtherIcideHerbFill!=1)

*creating Fungicide Only
*filling down fungicide
gsort prodno Fungicide
gen FungicideFill = Fungicide if Fungicide ==1
bysort prodno (FungicideFill) : replace FungicideFill = FungicideFill[_n-1] if missing(FungicideFill) & _n > 1
*creating the "other" category
gen OtherIcideFung= (Fungicide ==0 & Adjuvant ==0 & Fertilizer ==0)
gsort prodno -OtherIcideFung
gen OtherIcideFungFill = OtherIcideFung if OtherIcideFung ==1
bysort prodno (OtherIcideFungFill) : replace OtherIcideFungFill = OtherIcideFungFill[_n-1] if missing(OtherIcideFungFill) & _n > 1
gen FungOnly = (FungicideFill ==1 & OtherIcideFungFill!=1)


*creating Insecticide/Fungicide (insec-all & fungicide)
gen InsectFung = (InsecticideFill ==1 & Fungicide ==1 )


sort prodno
collapse (first) InsectAll (first) InsectOnly (first) HerbOnly (first) FungOnly (max) Herbicide (max) Fungicide (max) InsectFung, by(prodno)


sort prodno

drop if prodno==.
drop if prodno==-1

save /Users/ashleylarsen/GoogleDrive/Stata/KernPURPesticideType24, replace


log close
