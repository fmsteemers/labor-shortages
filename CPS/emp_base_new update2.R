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
#require(xml2)

### Read DDI file
ddi_vector <- c("cps_00124.xml","cps_00125.xml","cps_00126.xml","cps_00127.xml")
#ddi_vector <- c("cps_00108.xml")
ddi_list <- lapply(ddi_vector,read_ipums_ddi)
names(ddi_list) <- ddi_vector
# Read in IPUMS data with ddis
data_list <- lapply(ddi_list,read_ipums_micro_chunked,IpumsDataFrameCallback$new(function(x, pos) {
  x
}),
chunk_size = 1000000,vars = NULL,var_attrs = c("val_labels", "var_label","var_desc"))

# Bind list into one dataframe
data_frame <- bind_rows(data_list)
names(data_frame) <- tolower(names(data_frame))

# Reference labels
reference_labels <- ddi_list$cps_00127.xml$var_info$val_labels
names(reference_labels) <- ddi_list$cps_00127.xml$var_info$var_name

#names(data_frame)

# ACS CPS SOC2010 crosswalk
# For cps occ labels
cw_cps_soc2010 <- read_excel('I:/Steemers/CPS/nem-occcode-cps-crosswalk.xlsx',sheet = 'r_cw_cps_soc2010') %>%
  mutate(soc2010_2 = str_replace(soc2010,'-',''))
print(cw_cps_soc2010)

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

###################################################
emp_base_temp <- data_frame %>%
  filter(year>=1994) %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below hs',
                            educ == 73 ~ 'hs',
                            educ %in% c(80:109) ~ 'some college',
                            educ %in% c(110,111) ~ 'ba',
                            educ >= 120 ~ 'ma and above')) %>%
  mutate(age_group = case_when(age %in% c(16:17) ~ '16-17',
                               age %in% c(18:19) ~ '18-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:28) ~ '25-28',
                               age %in% c(29) ~ '29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:44) ~ '35-44',
                               age %in% c(45:54) ~ '45-54',
                               age %in% c(55:64) ~ '55-64',
                               age %in% c(55:64) ~ '65-69',
                               age %in% c(55:64) ~ '70-74',
                               age >= 65 ~ '75<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                                race == 200 & hispan==0 ~ 'black',
                                race %in% c(650,651,652) & hispan==0 ~ 'asian',
                                hispan!=0 ~ 'hispanic',
                                (race == 300 | race > 652) & hispan==0~ 'other')) %>%
  left_join(occ2010_2digit,by = 'occ2010')

### Base dataset with 2 digit occupations
emp_base_new <- emp_base_temp %>%
  # select(year,month,wtfinl,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat,covidtelew) %>%
  # group_by(year,month,occ2010_2digit,sex,diffany,educ_b,age_group,race_group,empstat,labforce,nilfact,statefip,wkstat,covidtelew) %>%
  #summarise(emp = n()) %>%
  ungroup() %>%
  mutate(empstat_adj = ifelse(empstat==34 & nilfact==1,32,empstat)) %>%
  mutate(empstat_adj = ifelse(empstat==36 & nilfact==4,34,empstat_adj)) %>%
  mutate(nilfact = ifelse(empstat_adj==32 & nilfact==1,99,nilfact)) %>%
  select(year,month,wtfinl,occ2010_2digit,sex,educ_b,age_group,race_group,empstat,empstat_adj,labforce,statefip,wkstat,covidtelew) %>%
  group_by(year,month,occ2010_2digit,sex,educ_b,age_group,race_group,empstat,empstat_adj,labforce,statefip,wkstat,covidtelew) %>%
  summarise(pop = sum(wtfinl)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  #left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat_adj = val,empstat_adj_lbl = lbl),by = c('empstat_adj')) %>%
  #left_join(reference_labels$DIFFANY %>% rename(diffany = val,diffany_lbl = lbl),by = c('diffany')) %>%
  left_join(reference_labels$STATEFIP %>% rename(statefip = val,statefip_lbl = lbl),by = c('statefip')) %>%
  left_join(reference_labels$WKSTAT %>% rename(wkstat = val,wkstat_lbl = lbl),by = c('wkstat')) %>%
  left_join(reference_labels$COVIDTELEW %>% rename(covidtelew = val,covidtelew_lbl = lbl),by = c('covidtelew')) #%>%
#mutate(empstat_nilfact_lb = str_c(empstat_adj_lbl,nilfact_lbl,sep = '-'))
print(emp_base_new)

fwrite(emp_base_new,"emp_base_new2.csv")

##View State Codes
state_codes <- emp_base_new %>%
  select(statefip,statefip_lbl)
print(state_codes)
