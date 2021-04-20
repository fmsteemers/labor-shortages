
####################
##### CONTENT PROGRAM
# Labor shortages solutions
# Untapped demographic groups
#    Employment shares by:
#        Women, minority racial groups, disabled, below HS educated, older workers
#        Population shares by the same groups, excluding occupation
# Infographic: Population and LFPRs by age for 2000, 2007, and 2019
# Enrollment status (Navy request)
# Living at home
# COVID-19 research --- demographics by occupations impacted
# QBE request for share of women in leadership positions by occupation/industry

# DATASET UPDATED ON AUGUST 13, 2019 WITH NEW EMP BASE BREAKDOWN TO RESTART CHARTING AND HAVE A FEW MORE OPTIONS (NILFACT)

##### To do:


setwd("I:/Steemers/CPS/IPUMS")





#install.packages('ipumsr')
#install.packages('zoo')
require(data.table)
require(tidyr)
require(dplyr)
require(stringr)
require(readr)
require(purrr)
require(lubridate)
require(ipumsr)
require(zoo)
require(readxl)
#install.packages('xml2')
require(xml2)

### Read DDI file
ddi_vector <- c("cps_00101.xml","cps_00104.xml","cps_00109.xml")
#ddi_vector <- c("cps_00108.xml")
ddi_list <- lapply(ddi_vector,read_ipums_ddi)
names(ddi_list) <- ddi_vector
# Read in IPUMS data with ddis
data_list <- lapply(ddi_list,read_ipums_micro_chunked,IpumsDataFrameCallback$new(function(x, pos) {
  x
}),
chunk_size = 10000,vars = NULL,var_attrs = c("val_labels", "var_label","var_desc"))

# Bind list into one dataframe
data_frame <- bind_rows(data_list)
names(data_frame) <- tolower(names(data_frame))

# Reference labels
reference_labels <- ddi_list$cps_00109.xml$var_info$val_labels
names(reference_labels) <- ddi_list$cps_00109.xml$var_info$var_name

names(data_frame)

# ACS CPS SOC2010 crosswalk
# For cps occ labels
cw_cps_soc2010 <- read_excel('I:/Steemers/CPS/nem-occcode-cps-crosswalk.xlsx',sheet = 'r_cw_cps_soc2010') %>%
     mutate(soc2010_2 = str_replace(soc2010,'-',''))
print(cw_cps_soc2010)

# For COVID-19 demographics analysis
# Crosswalk from ACS to CPS occupations. Needed because 'impacted occupations' file uses ACS occupations
### I MADE A CHANGE HERE NOT YET CHECKED
### DISTINCT OCC CODES BECAUSE THERE IS 1 ACS OVERLAP
cw_acs_lbl <- read_csv('cw_acs_cps_soc2010_occsoc.csv') %>%
distinct(occ = cpsocc,cpsocc_lbl,.keep_all = T) %>%
select(occ,occsoc,acsocc,cpsocc_lbl,acsocc_lbl)
print(cw_acs_lbl,n = 6)

write_csv(cw_acs_lbl,'cw_acs_lbl.csv')

# Import Gad's COVID-19 impacted industries
# Use 'cw_acs_lbl' crosswalk to go from ACS to CPS coding
impacted_occ <- read_excel('I:/Steemers/ACS/Input/impacted occupations_GL.xlsx',sheet = 'R_impacted_occ') %>%
    mutate(impacted_lbl = case_when(impacted == 2 ~ 'most_impacted',
                                   impacted==1 ~ 'impacted'))
print(impacted_occ)

# OCC2010 2 digit coding
occ2010_2digit <- data_frame %>%
  select(occ2010) %>%
  distinct(occ2010) %>%
  mutate(occ2010_2digit = case_when(occ2010 %in% c(10:430) ~ 'Management',
                                    occ2010 %in% c(500:950) ~ 'Business and financial operations',
                                    occ2010 %in% c(1000:1240) ~ 'Computer and mathematical',
                                    occ2010 %in% c(1300:1650) ~ 'Architecture and engineering',
                                    occ2010 %in% c(1600:1980) ~ 'Life, physical and social science',
                                    occ2010 %in% c(2000:2060) ~ 'Community and social services',
                                    occ2010 %in% c(2100:2150) ~ 'Legal',
                                    occ2010 %in% c(2200:2550) ~ 'Education, training and library',
                                    occ2010 %in% c(2600:2920) ~ 'Arts, design, entertainment, sports and media',
                                    occ2010 %in% c(3000:3540) ~ 'Healthcare practitioner and technical',
                                    occ2010 %in% c(3600:3650) ~ 'Healthcare support',
                                    occ2010 %in% c(3700:3950) ~ 'Protective services',
                                    occ2010 %in% c(4000:4150) ~ 'Food preparation and serving related',
                                    occ2010 %in% c(4200:4250) ~ 'Building and grounds cleaning and maintenance',
                                    occ2010 %in% c(4300:4650) ~ 'Personal care and services',
                                    occ2010 %in% c(4700:4965) ~ 'Sales and related',
                                    occ2010 %in% c(5000:5940) ~ 'Office and administrative support',
                                    occ2010 %in% c(6005:6130) ~ 'Farming, fishing and forestry',
                                    occ2010 %in% c(6200:6940) ~ 'Construction and extraction',
                                    occ2010 %in% c(7000:7630) ~ 'Installation, maintenance and repair',
                                    occ2010 %in% c(7700:8965) ~ 'Production',
                                    occ2010 %in% c(9000:9750) ~ 'Transportation and material moving',
                                    occ2010 %in% c(9800:9830) ~ 'Military',
                                    occ2010 %in% c(9920) ~ 'Unemployed or never worked'))

### occ2010 is not yet available for 2018 due to a significant occ classification change---https://usa.ipums.org/usa-action/revisions#revision_12_13_2019
### Instead, use a 2-digit breakdown based on occ instead of occ2010. Seems correct. 
occ_2digit <- data_frame %>%
    select(occ) %>%
    distinct(occ) %>%
    mutate(occ_2digit = case_when(occ %in% c(10:499) ~ 'Management',
              occ %in% c(500:999) ~ 'Business and financial operations',
              occ %in% c(1000:1299) ~ 'Computer and mathematical',
              occ %in% c(1300:1599) ~ 'Architecture and engineering',
              occ %in% c(1600:1999) ~ 'Life, physical and social science',
              occ %in% c(2000:2099) ~ 'Community and social services',
              occ %in% c(2100:2199) ~ 'Legal',
              occ %in% c(2200:2599) ~ 'Education, training and library',
              occ %in% c(2600:2999) ~ 'Arts, design, entertainment, sports and media',
              occ %in% c(3000:3599) ~ 'Healthcare practitioner and technical',
              occ %in% c(3600:3699) ~ 'Healthcare support',
              occ %in% c(3700:3999) ~ 'Protective services',
              occ %in% c(4000:4199) ~ 'Food preparation and serving related',
              occ %in% c(4200:4299) ~ 'Building and grounds cleaning and maintenance',
              occ %in% c(4300:4699) ~ 'Personal care and services',
              occ %in% c(4700:4999) ~ 'Sales and related',
              occ %in% c(5000:5999) ~ 'Office and administrative support',
              occ %in% c(6005:6199) ~ 'Farming, fishing and forestry',
              occ %in% c(6200:6999) ~ 'Construction and extraction',
              occ %in% c(7000:7699) ~ 'Installation, maintenance and repair',
              occ %in% c(7700:8999) ~ 'Production',
              occ %in% c(9000:9799) ~ 'Transportation and material moving',
              occ %in% c(9800:9899) ~ 'Military',
              occ %in% c(9920) ~ 'Unemployed or never worked'))

print(occ_2digit)

### For QBE and COVID-19 demographics analysis
### Industry in1990 2 digit
industry <- data_frame %>%
    select(ind1990) %>%
    distinct(ind1990) %>%
    mutate(ind1990_2digit = case_when(ind1990 %in% c(10:32) ~ 'Agriculture, forestry and fisheries',
                                     ind1990 %in% c(40:60) ~ 'Mining and construction',
                                     ind1990 %in% c(100:392) ~ 'Manufacturing',
                                     ind1990 %in% c(400:472) ~ 'Transportation, communications, and other public utilities',
                                     ind1990 %in% c(500:571) ~ 'Wholesale trade',
                                     ind1990 %in% c(580:691) ~ 'Retail trade',
                                     ind1990 %in% c(700:712) ~ 'Finance, insurance, real estate',
                                     ind1990 %in% c(721:760) ~ 'Business and repair services',
                                     ind1990 %in% c(761:791) ~ 'Personal services',
                                     ind1990 %in% c(800:810) ~ 'Entertainment and recreation services',
                                     ind1990 %in% c(812:893) ~ 'Professional and related services',
                                     ind1990 %in% c(900:932) ~ 'Public administration',
                                     ind1990 %in% c(940:960) ~ 'Active military'))
print(industry)
# occ labels
cw_cps <- read_excel('I:/Steemers/CPS/nem-occcode-cps-crosswalk.xlsx',sheet = 'r_cw_cps_soc2010') %>%
     mutate(soc2010_2 = str_replace(soc2010,'-','')) %>%
     distinct(occ = cpsocc,cpsocc_lbl)
print(cw_cps_soc2010)

############################
### QBE analysis
qbe_analysis <- data_frame %>%
  filter(year>=2012,empstat %in% c(10,12),uhrswork1>=35) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(cw_cps,by = 'occ') %>%
  left_join(industry,by = 'ind1990') %>%
  group_by(year,month,occ,cpsocc_lbl,occ2010_2digit,sex,ind1990,ind1990_2digit) %>%
  summarise(pop = sum(wtfinl)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$IND1990 %>% rename(ind1990 = val,ind1990_lbl = lbl),by = c('ind1990'))

print(qbe_analysis)
write_csv(qbe_analysis,'qbe_analysis.csv')
###############################

### It appears that the sums are exactly double what it should be... Mistake at IPUMS in wtfinl?
test <- data_frame %>%
  filter(year>=2017,age>=16) %>%
  group_by(year,month) %>%
  summarise(pop = sum(wtfinl))
print(test,n = 20)
table(data_frame$year,data_frame$month)


### For Devin --- Unemployment duration
unemp_duration <- data_frame %>%
  filter(year>=2018,age>=16) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  #left_join(reference_labels$DURUNEMP %>% rename(durunemp = val,durunemp_lbl = lbl),by = c('durunemp')) %>%
  #mutate(durunemp_b = cut(durunemp,2)) %>%
  mutate(durunemp_b = as.character(cut(as.double(durunemp), breaks = c(seq(0,209,4), 1000), include.lowest = T, right = F))) %>%
  group_by(durunemp_b) %>%
  mutate(number_max = max(durunemp)) %>%
  ungroup() %>%
  mutate(empstat_lbl_adj = ifelse(empstat %in% c(20:22),durunemp_b,empstat_lbl)) %>%
  group_by(year,month,empstat_lbl_adj) %>%
  summarise(pop = sum(wtfinl),number_max_s = max(number_max)) %>%
  unite(col = year_month,year,month,sep = '_',remove = TRUE) %>%
  spread(key = year_month,value = pop, fill = 0) %>%
  arrange(number_max_s)

print(unemp_duration)

print(data_frame$month %>% class())

print(unemp_duration$durunemp_b %>% unique())

write_csv(unemp_duration,'unemp_duration.csv')



#### Navy UR by young age group
navy_ur <- data_frame %>%
  filter(year>=1994) %>%
  mutate(age_group = case_when(age %in% c(16:17) ~ '16-17',
                               age %in% c(18:24) ~ '18-24',
                               age %in% c(25) ~ '25',
                               age %in% c(26:32) ~ '26-32',
                               age %in% c(33:34) ~ '33-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  select(year,month,empstat,age_group,wtfinl) %>%
  group_by(year,month,age_group,empstat) %>%
  summarise(pop = sum(wtfinl)) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  select(-empstat) %>%
  spread(key = empstat_lbl,value = pop)

print(navy_ur)

write_csv(navy_ur,'navy_ur.csv')

###################################################
### New emp base for charting expanding LF charts
### First, build temp dataset, later groupby 2 or 6 digit occupations
emp_base_temp <- data_frame %>%
  filter(year>=1994) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(occ_2digit,by = 'occ') %>%
  mutate(occ2010_2digit = ifelse(year>=2020,occ_2digit,occ2010_2digit))

### Base dataset with 2 digit occupations
emp_base_new <- emp_base_temp %>%
  select(year,month,wtfinl,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat) %>%
  group_by(year,month,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat) %>%
  summarise(pop = sum(wtfinl)) %>%
  #summarise(emp = n()) %>%
  ungroup() %>%
  mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany')) %>%
  left_join(reference_labels$STATEFIP %>% rename(statefip = val,statefip_lbl = lbl),by = c('statefip')) %>%
  left_join(reference_labels$WKSTAT %>% rename(wkstat = val,wkstat_lbl = lbl),by = c('wkstat')) %>%
  mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-'))
print(emp_base_new)


##################################################
### FOR MIKE SAMPLE CODE
temp <- emp_base_new %>%
  select(statefip) %>%
  mutate(statefip_lbl2 = as_factor(statefip))
##################################################


### Now including occ2010 6digit occupation (only used for gender dataset)
emp_base_new_6d <- emp_base_temp %>%
  select(year,month,wtfinl,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat,occ2010) %>%
  group_by(year,month,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat,occ2010) %>%
  summarise(pop = sum(wtfinl)) %>%
  #summarise(emp = n()) %>%
  ungroup() %>%
  mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany')) %>%
  left_join(reference_labels$STATEFIP %>% rename(statefip = val,statefip_lbl = lbl),by = c('statefip')) %>%
  left_join(reference_labels$WKSTAT %>% rename(wkstat = val,wkstat_lbl = lbl),by = c('wkstat')) %>%
  left_join(reference_labels$OCC2010 %>% rename(occ2010 = val,occ2010_lbl = lbl),by = c('occ2010')) %>%
  mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-'))

write_csv(emp_base_new,'emp_base_new.csv')


write_csv(emp_base_new_6d %>% select(year,month,occ2010,occ2010_lbl,occ2010_2digit,educ_b,age_group,race_group,empstat_adj_lbl,sex_lbl,pop,labforce_lbl,wkstat_lbl,statefip_lbl),'emp_base_new_6d.csv')

###################################################
### OLD ---- But still input in 2 digit/6digit occupation analyses
emp_base <- data_frame %>%
  filter(empstat %in% c(10,12)) %>%
  filter(year>=1994) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:54) ~ '30-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(occ_2digit,by = 'occ') %>%
  mutate(occ2010_2digit = ifelse(year>=2020,occ_2digit,occ2010_2digit)) %>%
  select(year,month,wtfinl,occ2010,sex,diffany,educ_b,age_group,race_group,occ2010_2digit) %>%
  group_by(year,month,occ2010,sex,diffany,educ_b,age_group,race_group,occ2010_2digit) %>%
  summarise(emp = sum(wtfinl)) %>%
  #summarise(emp = n()) %>%
  left_join(reference_labels$OCC2010 %>% rename(occ2010 = val,occ2010_lbl = lbl),by = c('occ2010')) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany')) %>%
    ##### NEEDS TO BE UPDATED!!!!
  mutate(period = case_when(year %in% c(2014,2015) | (year==2013 & month >=5) | (year==2016 & month <=4) ~ 'period1',
                           year %in% c(2019,2018,2017) | (year==2016 & month >=5) ~ 'period2',
                           TRUE ~ 'period0'))
print(emp_base)

tail(emp_base %>% distinct(year,month,occ2010_2digit))

### OLD DO NOT USE
write_csv(emp_base,'emp_base.csv')

### OLD PROGRAMMING FOR DIFFERENT BREAKDOWNS IS AVAILABLE IN A PREVIOUS SCRIPT VERSION

### Employment breakdowns
covid19_cps_demographics <- data_frame %>%
  filter(year>=2012,empstat %in% c(10,12)) %>%
  mutate(pt = ifelse(uhrswork1<35,'pt','ft')) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(cw_acs_lbl,by = 'occ') %>%
  left_join(industry,by = 'ind1990') %>%
  left_join(impacted_occ,by = 'occsoc') %>%
  group_by(year,month,occ,cpsocc_lbl,occ2010_2digit,ind1990,ind1990_2digit,sex_lbl,educ_b,age_group,race_group,pt,impacted,impacted_lbl) %>%
  summarise(emp = sum(wtfinl)) %>%
  left_join(reference_labels$IND1990 %>% rename(ind1990 = val,ind1990_lbl = lbl),by = c('ind1990'))


#### TEST CHECK CPS AND ACS OCCUPATIONS CROSSWALK IMPACTED OCCUPATIONS
### Employment breakdowns
covid19_cps_demographics_test_cw <- data_frame %>%
  filter(year>=2012,empstat %in% c(10,12)) %>%
  mutate(pt = ifelse(uhrswork1<35,'pt','ft')) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(cw_acs_lbl,by = 'occ') %>%
  left_join(industry,by = 'ind1990') %>%
  left_join(impacted_occ,by = 'occsoc') 

#### TEST CHECK CPS AND ACS OCCUPATIONS CROSSWALK IMPACTED OCCUPATIONSprint(covid19_cps_demographics_test_cw)
temp_check <- covid19_cps_demographics_test_cw %>%
    filter(impacted %in% c(1,2)) %>%
    head(10000)
print(temp_check)
write_csv(temp_check,'temp_check.csv')

### Wage breakdowns
### To do

### Variables to use uhrswork1''ahrsworkt''wkstat''qocc''educ''schlcoll''earnwt''hourwage''paidhour''union''earnweek''uhrsworkorg
covid19_cps_wages <- data_frame %>%
  filter(year>=2012,empstat %in% c(10,12),hourwage<=99.99) %>%
  mutate(pt = ifelse(uhrswork1<35,'pt','ft')) %>%
  left_join(occ2010_2digit,by = 'occ2010') %>%
  left_join(cw_acs_lbl,by = 'occ') %>%
  left_join(industry,by = 'ind1990') %>%
  left_join(impacted_occ,by = 'occsoc') %>%
  group_by(year,pt,impacted,impacted_lbl) %>%
  summarise(wage_mean = mean(hourwage),wage_weightedmean = weighted.mean(hourwage,w = earnwt,na.rm = T),wage_median = median(hourwage,na.rm = T))


write_csv(covid19_cps_demographics,'covid19_cps_demographics.csv')

temp_wage <- data_frame %>%
select(hourwage) %>%
distinct(hourwage) %>%
arrange(-hourwage)

print(temp_wage)

print(covid19_cps_wages)
write_csv(covid19_cps_wages,'covid19_cps_wages.csv')

############ Different reshaping
### Datasets per breakdown, with all other as cross-tabs
### Calculate each breakdown and share individually
### Function for each breakdown seperately
    groupByVars_base <- c('sex_lbl','educ_b','age_group','race_group','diffany_lbl')
    groupByVars_monthly_6d <- c('year', 'month', 'occ2010_2digit', 'occ2010','occ2010_lbl')
    #groupByVars_monthly_6d <- c('year', 'month', 'period', 'occ2010_2digit', 'occ2010','occ2010_lbl')
    groupByVars_monthly_2d <- c('year', 'month', 'occ2010_2digit')
    groupByVars_period_6d <- c('period', 'occ2010_2digit', 'occ2010','occ2010_lbl')
    groupByVars_period_2d <- c('period', 'occ2010_2digit')

share_reshaper2 <- function(breakdown,empbase = emp_base_new,groupByVariables = groupByVars_monthly_2d) {
    
    df <- empbase %>%
    filter(empstat %in% c(10,12)) %>%
    filter(year>=2003) %>%
    ungroup() %>%
    select(groupByVars_base,groupByVariables,pop) %>%
    group_by_at(vars(one_of(groupByVars_base),one_of(groupByVariables))) %>%
    summarise(pop = sum(pop)) %>%
    spread(key = breakdown, value = pop, fill = 0,sep = '') %>%
    ungroup() %>%
    mutate(total = rowSums(.[,contains(breakdown,vars = names(.))],na.rm = T))
    #mutate(total = rowSums(.[,8:ncol(.)],na.rm = T))
    
    names(df) <- names(df) %>% str_replace_all(breakdown,'')
    
    return(df)
}

vector_breakdown <- c('sex_lbl','educ_b','age_group','race_group','diffany_lbl')
vector_breakdown_gender <- c('sex_lbl')
### breakdowns with 6 digit occupations (only useful for Women)
list_df_shares_monthly_6d_2 <- lapply(vector_breakdown_gender,share_reshaper2,empbase = emp_base_new_6d,groupByVariables = groupByVars_monthly_6d)
names(list_df_shares_monthly_6d_2) <- vector_breakdown_gender
### breakdown with 2 digit occupations (best to limit size datasets))
list_df_shares_monthly_2d_2 <- lapply(vector_breakdown,share_reshaper2,empbase = emp_base_new,groupByVariables = groupByVars_monthly_2d)
names(list_df_shares_monthly_2d_2) <- vector_breakdown

names(emp_base_new_6d)

#print(list_df_shares_monthly_6d_2$educ_b)

### Exporting to CSV
# 6 digit occupations
write_csv(list_df_shares_monthly_6d_2$sex_lbl,'cps_emp_shares_sex_lbl_6d.csv')
#write_csv(list_df_shares_monthly_6d_2$educ_b,'cps_emp_shares_educ.csv')
#write_csv(list_df_shares_monthly_6d_2$age_group,'cps_emp_shares_age_group.csv')
#write_csv(list_df_shares_monthly_6d_2$race_group,'cps_emp_shares_race_group.csv')
#write_csv(list_df_shares_monthly_6d_2$diffany_lbl,'cps_emp_shares_diffany_lbl.csv')

list_df_shares_monthly_6d_2$sex_lbl %>% names()


# 2 digit occupations
write_csv(list_df_shares_monthly_2d_2$sex_lbl,'cps_emp_shares_sex_lbl_2d.csv')
write_csv(list_df_shares_monthly_2d_2$educ_b,'cps_emp_shares_educ_2d.csv')
write_csv(list_df_shares_monthly_2d_2$age_group,'cps_emp_shares_age_group_2d.csv')
write_csv(list_df_shares_monthly_2d_2$race_group,'cps_emp_shares_race_group_2d.csv')
write_csv(list_df_shares_monthly_2d_2$diffany_lbl,'cps_emp_shares_diffany_lbl_2d.csv')

tail(list_df_shares_monthly_2d_2$race_group %>% distinct(year,month,occ2010_2digit) %>% arrange(year,month))

######### Population breakdowns ---- to see if employment share increases is cyclical or demographic trend
pop_base <- data_frame %>%
  filter(year>=1994) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:110) ~ 'some college',
                                    educ == 111 ~ 'ba',
                                    educ >= 123 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:24) ~ '16-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:54) ~ '30-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  select(year,month,wtfinl,sex,diffany,educ_b,age_group,race_group) %>%
  group_by(year,month,sex,diffany,educ_b,age_group,race_group) %>%
  summarise(pop = sum(wtfinl)) %>%
  #summarise(emp = n()) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany'))
print(pop_base)
write_csv(pop_base,'pop_base.csv')

names(data_frame)

###################################################
### Infographic - population and LFPRs
### MAKE SURE TO UPDATE PERIOD VARAIBLE WITH THE LAST 12 MONTHS
base_infographic <- data_frame %>%
  filter(empstat != 1) %>% # Exclude armed-forces
  filter(year>=1994) %>%
  filter(age>=16) %>%
  #filter(year %in% c(2000,2007),year==2018 & month %in% c(8:12),year==2019 & month %in% c(1:7)) %>%
  mutate(educ_b = case_when(educ %in% c(1:109) ~ 'below ba',
                                    educ >= 110 ~ 'ba and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:39) ~ '35-39',
                               age %in% c(40:44) ~ '40-44',
                               age %in% c(45:49) ~ '45-49',
                               age %in% c(50:54) ~ '50-54',
                               age %in% c(55:59) ~ '55-59',
                               age %in% c(60:64) ~ '60-64',
                               age %in% c(65:69) ~ '65-69',
                               age %in% c(70:74) ~ '70-74',
                               age %in% c(75:79) ~ '75-79',
                               age %in% c(80:84) ~ '80-84',
                               age >= 85 ~ '85<')) %>%
  mutate(age_group2 = case_when(age %in% c(16:24) ~ '16-24',
                               age %in% c(25:34) ~ '25-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age %in% c(65:74) ~ '65-74',
                               age %in% c(75:84) ~ '75-84',
                               age >= 85 ~ '85<')) %>%
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0 ~ 'other')) %>%
  mutate(period = case_when(year==2000 ~ '2000',
                           year==2007 ~ '2007',
                            year==2016 ~ '2016',
                            year==2017 ~ '2017',
                           ((year==2018 & month %in% c(11:12)) | (year==2019 & month %in% c(1:10))) ~ 'last_12months')) %>%
  ### UPDATE PERIOD VARIABLE WITH LAST 12 MONTHS
  filter(period %in% c('2000','2007','2016','2017','last_12months'),!(is.na(age_group))) %>%
  select(period,wtfinl,sex,educ_b,age_group,age_group2,race_group,empstat,labforce,nilfact) %>%
  group_by(period,sex,educ_b,age_group,age_group2,race_group,empstat,labforce,nilfact) %>%
  summarise(pop = sum(wtfinl)) %>%
  ungroup() %>%
  mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-'))
print(base_infographic)

base_infographic$period %>% unique()

######### Infographic calculation
### Calculation 1: impact age distribution on Labor Force size
### Total population in last 12 months
base_infographic_agedistr_total <- base_infographic %>%
 group_by(period) %>%
 summarise(total = sum(pop) / 12) %>% # Adjust for monthly CPS, so totals divided by 12 months for annual
 filter(period == 'last_12months') %>%
 mutate(merge = 'merge') %>%
 select(merge,total)
print(base_infographic_agedistr_total)

### LFPRs by age in last 12 months
base_infographic_agedistr_lfpr <- base_infographic %>%
select(period,age_group,labforce_lbl,pop) %>%
group_by(period,age_group,labforce_lbl) %>%
summarise(pop = sum(pop)) %>%
 spread(key = labforce_lbl,value = pop) %>%
mutate(lfpr = `Yes, in the labor force`/ sum(`Yes, in the labor force`,`No, not in the labor force`,na.rm = T)) %>%
filter(period == 'last_12months') %>%
ungroup() %>%
select(age_group,lfpr)
print(base_infographic_agedistr_lfpr,n= 20)

### Age distributions by year
base_infographic_agedistr_agedistr <- base_infographic %>%
select(period,age_group,pop) %>%
group_by(period,age_group) %>%
summarise(pop = sum(pop)) %>%
mutate(share = pop / sum(pop,na.rm = T))
print(base_infographic_agedistr_agedistr,n= 100)

### Base infographic actual
base_infographic_actual <- base_infographic %>%
 group_by(period) %>%
 filter(labforce_lbl == 'Yes, in the labor force') %>%
 summarise(lf_actual = sum(pop) / 12)
print(base_infographic_actual)

### Merge
base_infographic_agedistr_final <- base_infographic_agedistr_agedistr %>%
left_join(base_infographic_agedistr_lfpr,by = 'age_group') %>%
mutate(merge = 'merge') %>%
left_join(base_infographic_agedistr_total,by = 'merge') %>%
#print(base_infographic_agedistr_final) %>%
mutate(outcome = share * lfpr * total) %>%
group_by(period) %>%
summarise(lf_imputed = sum(outcome)) %>%
left_join(base_infographic_actual,by = 'period')
print(base_infographic_agedistr_final)

### Output
write_csv(base_infographic_agedistr_final,'base_infographic_agedistr_final.csv')


######################################################
### Calculation 2: impact labor force participation on labor force size
lfpr_total <- base_infographic %>%
 #filter(age_group2 == '16-24') %>%
 group_by(period,sex_lbl,age_group2,educ_b) %>%
 summarise(total = sum(pop) / 12) %>% # Adjust for monthly CPS, so totals divided by 12 months for annual
 filter(period == 'last_12months') %>%
 mutate(merge = 'merge') %>%
 ungroup() %>%
 select(merge,sex_lbl,age_group2,educ_b,total)
print(lfpr_total)

### LFPRs by age in last 12 months, 2007, 2000
lfpr_lfpr <- base_infographic %>%
 select(period,sex_lbl,age_group2,labforce_lbl,educ_b,pop) %>%
 group_by(period,sex_lbl,age_group2,labforce_lbl,educ_b) %>%
 summarise(pop = sum(pop) / 12) %>%
 spread(key = labforce_lbl,value = pop) %>%
 ungroup() %>%
 group_by(period,sex_lbl,age_group2,educ_b) %>%
 #mutate(totalpop = sum(`Yes, in the labor force`,`No, not in the labor force`,na.rm = T)) %>%
 mutate(lfpr = `Yes, in the labor force` / sum(`Yes, in the labor force`,`No, not in the labor force`,na.rm = T)) %>%
 ungroup() #%>%
 #select(period,age_group2,educ_b,lfpr) %>%
 #filter(age_group2 == '16-24')
 print(lfpr_lfpr,n= 100)

### Merge
lfpr_lfpr_final <- lfpr_lfpr %>%
mutate(merge = 'merge') %>%
left_join(lfpr_total,by = c('merge','sex_lbl','age_group2','educ_b')) %>%
mutate(outcome = lfpr * total) %>%
#group_by(period) %>%
group_by(period,sex_lbl,age_group2,educ_b) %>%
summarise(lf_imputed = sum(outcome))
print(lfpr_lfpr_final,n = 100)


#####
### Output
write_csv(lfpr_lfpr_final,'lfpr_lfpr_final.csv')



######################################################
### Calculation 3: impact disability on labor force size
disability_total <- base_infographic %>%
 #filter(age_group2 == '16-24') %>%
 group_by(period,sex_lbl,age_group2,educ_b) %>%
 summarise(total = sum(pop) / 12) %>% # Adjust for monthly CPS, so totals divided by 12 months for annual
 filter(period == 'last_12months') %>%
 mutate(merge = 'merge') %>%
 ungroup() %>%
 select(merge,sex_lbl,age_group2,educ_b,total)
print(disability_total)

### Disability by age in last 12 months, 2007, 2000
disability_disability <- base_infographic %>%
 select(period,sex_lbl,age_group2,empstat_lbl,educ_b,pop) %>%
 group_by(period,sex_lbl,age_group2,empstat_lbl,educ_b) %>%
 summarise(pop = sum(pop) / 12) %>%
 spread(key = empstat_lbl,value = pop) %>%
 ungroup() %>%
 group_by(period,sex_lbl,age_group2,educ_b) %>%
 #mutate(totalpop = sum(`Yes, in the labor force`,`No, not in the labor force`,na.rm = T)) %>%
 mutate(sh_disability = `NILF, unable to work` / sum(`NILF, unable to work`,`At work`,`NILF, other`,`NILF, retired`,`Has job, not at work last week`,`Unemployed, experienced worker`,`Unemployed, new worker`,na.rm = T)) %>%
 ungroup() #%>%
 #select(period,age_group2,educ_b,lfpr) %>%
 #filter(age_group2 == '16-24')
print(disability_disability %>% select(sh_disability,everything()),n= 50)

### Merge
disability_final <- disability_disability %>%
mutate(merge = 'merge') %>%
left_join(disability_total,by = c('merge','sex_lbl','age_group2','educ_b')) %>%
mutate(outcome = sh_disability * total) %>%
#group_by(period) %>%
group_by(period,sex_lbl,age_group2,educ_b) %>%
summarise(disability_imputed = sum(outcome))
print(disability_final,n = 20)


#####
### Output
write_csv(disability_final,'disability_final.csv')

######################################################
### Calculation 4: impact education on labor force size
educ_total_lf <- base_infographic %>%
 filter(labforce_lbl == 'Yes, in the labor force') %>%
 #filter(age_group2 == '16-24') %>%
 #group_by(period,educ_b) %>%
 group_by(period) %>%
 summarise(total = sum(pop) / 12) %>% # Adjust for monthly CPS, so totals divided by 12 months for annual
 filter(period == 'last_12months') %>%
 mutate(merge = 'merge') %>%
 ungroup() %>%
 select(merge,total)
print(educ_total_lf)

print('good')
### Education share nonBA in last 12 months, 2007, 2000
educ_educ <- base_infographic %>%
 filter(labforce_lbl == 'Yes, in the labor force') %>%
 select(period,educ_b,pop) %>%
 group_by(period,educ_b) %>%
 summarise(pop = sum(pop) / 12) %>%
 spread(key = educ_b,value = pop) %>%
 ungroup() %>%
 group_by(period) %>%
 #mutate(totalpop = sum(`Yes, in the labor force`,`No, not in the labor force`,na.rm = T)) %>%
 mutate(sh_educ_b = `below ba` / sum(`below ba`,`ba and above`,na.rm = T)) %>%
 ungroup() #%>%
 #select(period,age_group2,educ_b,lfpr) %>%
 #filter(age_group2 == '16-24')
print(educ_educ %>% select(sh_educ_b,everything()),n= 50)

### Merge
educ_final <- educ_educ %>%
mutate(merge = 'merge') %>%
left_join(educ_total_lf,by = c('merge')) %>%
mutate(outcome_nonba = sh_educ_b * total) %>%
mutate(outcome_ba = (1-sh_educ_b) * total) %>%
#group_by(period) %>%
group_by(period) %>%
summarise(educ_imputed_noba = sum(outcome_nonba),educ_imputed_ba = sum(outcome_ba))
print(educ_final,n = 20)


#####
### Output
write_csv(educ_final,'educ_final.csv')

base_infographic$empstat_lbl %>% unique()



###################################################
### Enrollment in college
### Navy request
school_enrollment <- data_frame %>%
  filter(year>=1994) %>%
  filter(age>=16 & age<=24) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:39) ~ '35-39',
                               age %in% c(40:44) ~ '40-44',
                               age %in% c(45:49) ~ '45-49',
                               age %in% c(50:54) ~ '50-54',
                               age %in% c(55:59) ~ '55-59',
                               age %in% c(60:64) ~ '60-64',
                               age %in% c(65:69) ~ '65-69',
                               age %in% c(70:74) ~ '70-74',
                               age %in% c(75:79) ~ '75-79',
                               age %in% c(80:84) ~ '80-84',
                               age >= 85 ~ '85<')) %>%
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  select(year,month,wtfinl,sex,age,age_group,race_group,empstat,nilfact,labforce,schlcoll) %>%
  group_by(year,month,sex,age,age_group,race_group,empstat,nilfact,labforce,schlcoll) %>%
  summarise(pop = sum(wtfinl)) %>%
  ungroup() %>%
  mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  left_join(reference_labels$SCHLCOLL %>% rename(schlcoll = val,schlcoll_adj_lbl = lbl),by = c('schlcoll')) %>%
  mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-'))
print(school_enrollment)


write_csv(school_enrollment,'school_enrollment.csv')





###################################################
### Living at home
### LSS report (monthly instead of march supplement)
### There are a few breaks over time
###    Relate variable starts tracking grandchild in 1994
###    Marital status changes in 1989, maybe affecting living at home
###    Asians are tracked from 1989 onwards
###    Famrel is constant but only tracks children from 1983 onwards and is comparison to family head, which may be a child with spouse living with parents.
###    Relate captures relationship to household head (owner or renter house) and probably better for this exercise for people still at home. Family units, captured by famrel variable, can still capture young adults who are dependent but will in this variable not be a child.
living_athome <- data_frame %>%
  filter(year>=1983) %>%
  filter(empstat != 1) %>% # Exclude armed-forces
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  mutate(marital_status = case_when(marst == 1 ~ 'married, spouse present',
                               marst == 2 ~ 'married, spouse absent',
                               marst == 3 ~ 'separated',
                               marst == 4 ~ 'divorced',
                               marst == 6 ~ 'never married / single',                             
                               marst == 5 ~ 'widowed')) %>%
  #select(year,month,wtfinl,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,marital_status,relate,famrel) %>%
  #group_by(year,month,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,marital_status,relate,famrel) %>%
  select(year,month,wtfinl,sex,educ_b,age_group,race_group,labforce,marital_status,relate,famrel,statefip) %>%
  group_by(year,month,sex,educ_b,age_group,race_group,labforce,marital_status,relate,famrel,statefip) %>%
  #select(year,month,wtfinl,sex,educ_b,age_group,race_group,labforce,famrel_lbl_adj_onlychild,famrel_lbl_adj,famrel_lbl_adj_alt,statefip) %>%
  #group_by(year,month,sex,educ_b,age_group,race_group,labforce,famrel_lbl_adj_onlychild,famrel_lbl_adj,famrel_lbl_adj_alt,statefip) %>%
  summarise(pop = sum(wtfinl)) %>%
  #summarise(emp = n()) %>%
  ungroup() %>%
  #mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  #mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  #mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  #left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  #left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  #left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  #left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany')) %>%
  left_join(reference_labels$RELATE %>% rename(relate = val,relate_lbl = lbl),by = c('relate')) %>%
  left_join(reference_labels$STATEFIP %>% rename(statefip = val,statefip_lbl = lbl),by = c('statefip')) %>%
  left_join(reference_labels$FAMREL %>% rename(famrel = val,famrel_lbl = lbl),by = c('famrel')) %>%
  mutate(famrel_lbl_adj_onlychild = ifelse(famrel == 3 | (famrel == 1 & relate %in% c(301:303)) | relate %in% c(301:303),'Child',famrel_lbl)) %>%  
  mutate(famrel_lbl_adj = ifelse(famrel == 3 | (famrel == 1 & relate %in% c(301:303)) | relate %in% c(301:303) | relate==901,'Child',famrel_lbl)) %>%  
  mutate(famrel_lbl_adj_alt = ifelse(famrel == 3 | relate==301 | relate==901,'Child',famrel_lbl))
#mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-')) %>%
  





write_csv(living_athome,'living_athome.csv')



test_break1989 <- data_frame %>%
  filter(year>=1983) %>%
  filter(empstat != 1) %>%
mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                                    educ == 73 ~ 'hs',
                                    educ %in% c(80:109) ~ 'some college',
                                    educ %in% c(110,111) ~ 'ba',
                                    educ >= 120 ~ 'ma and above')) %>%
mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age >= 65 ~ '65<')) %>%
mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                               race == 200 & hispan==0 ~ 'black',
                               race %in% c(650,651,652) & hispan==0 ~ 'asian',
                               hispan!=0 ~ 'hispanic',
                               (race == 300 | race > 652) & hispan==0~ 'other')) %>%
mutate(marital_status = case_when(marst == 1 ~ 'married, spouse present',
                               marst == 2 ~ 'married, spouse absent',
                               marst == 3 ~ 'separated',
                               marst == 4 ~ 'divorced',
                               marst == 6 ~ 'never married / single',                             
                               marst == 5 ~ 'widowed')) %>%
  group_by(year,month,sex,educ_b,age_group,race_group,marital_status,relate) %>%
  summarise(pop = sum(wtfinl))



write_csv(test_break1989,'test_break1989.csv')
