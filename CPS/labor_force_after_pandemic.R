setwd('I:/Steemers/CPS/IPUMS')
require(rebus)
require(broom)
require(ipumsr)
require(dplyr)
require(purrr)
require(stringr)
require(readr)
require(tidyr)
require(data.table)
require(zoo)
require(readxl)
require(ggplot2)
############################################################################################################
############################################################################################################
### Read DDI file
ddi_vector <- c("cps_00126.xml","cps_00125.xml","cps_00124.xml","cps_00123.xml")
ddi_list <- lapply(ddi_vector,read_ipums_ddi)
names(ddi_list) <- ddi_vector
# data_list <- lapply(ddi_list,read_ipums_micro,IpumsDataFrameCallback$new(function(x, pos) {
#   x
# }),var_attrs = c("val_labels", "var_label","var_desc"))
data_list <- lapply(ddi_list,read_ipums_micro_chunked,IpumsDataFrameCallback$new(function(x, pos) {
  x
}),chunk_size = 40000,vars = NULL,var_attrs = c("val_labels", "var_label","var_desc"))
data_frame <- bind_rows(data_list)
names(data_frame) <- tolower(names(data_frame))

#setwd('I:/Steemers/ACS/Output')
setwd('I:/Steemers/Team charts and data/Labor force after pandemic') ### for all models
#setwd('I:/Steemers/ACS/Output/Wage gaps - full 6digit model') ### exception for only running the 6digit occ ind model
### Create reference labels for IPUMS variables
reference_labels <- ddi_list$cps_00122$var_info$val_labels
names(reference_labels) <- ddi_list$cps_00122$var_info$var_name

df <- data_frame %>%
  mutate(educ_b = case_when(educ %in% c(1:72) ~ 'below_hs',
                            educ == 73 ~ 'hs',
                            educ %in% c(80:109) ~ 'some_college',
                            educ %in% c(110,111) ~ 'ba',
                            educ >= 120 ~ 'ma_above')) %>%
  mutate(educ_b_large = case_when(educ %in% c(1:109) ~ 'below BA',
                                  educ >= 110 ~ 'BA and above')) %>%
  mutate(age_group = case_when(age %in% c(16:19) ~ '16-19',
                               age %in% c(20:24) ~ '20-24',
                               age %in% c(25:29) ~ '25-29',
                               age %in% c(30:34) ~ '30-34',
                               age %in% c(35:40) ~ '35-39',
                               age %in% c(40:44) ~ '40-44',
                               age %in% c(45:49) ~ '45-49',
                               age %in% c(50:54) ~ '50-54',
                               age %in% c(55:59) ~ '55-59',
                               age %in% c(60:64) ~ '60-64',
                               age >= 65 ~ '65<')) %>%
  mutate(age_group_large = case_when(age %in% c(16:24) ~ '16-24',
                                     age %in% c(25:34) ~ '25-34',
                                     age %in% c(35:44) ~ '35-44',
                                     age %in% c(45:54) ~ '45-54',
                                     age >= 55 ~ '55<')) %>%
  # Other race includes those with more than 1 race---does this make sense?
  mutate(race_group = case_when(race == 100 & hispan==0 ~ 'white',
                                race == 200 & hispan==0 ~ 'black',
                                race %in% c(650,651,652) & hispan==0 ~ 'asian', #### Before 1989, there are only 4 groups possible---Asians cannot be tracked.
                                hispan!=0 ~ 'hispanic',
                                (race == 300 | race >= 700) & hispan==0~ 'other')) %>%
  mutate(child_age = case_when(nchild == 0 ~ 'no children',
                               yngch <= 5 ~ '1 or more aged 5 or younger',
                               yngch >= 6 & yngch <= 12 ~ '1 or more aged 6 to 12',
                               yngch >= 13 & yngch <= 17 ~ '1 or more aged 13 to 17',
                               yngch >= 18 & yngch <= 98 ~ '1 or more aged 18 or higher',
                               TRUE ~ 'Other')) %>%
  #mutate(citizen_group = ifelse(citizen == 5,'non-citizen','citizen')) %>%
  select(year,month,wtfinl,educ_b,age_group,age_group_large,race_group,sex,empstat,labforce,classwkr,wkstat,nilfact,wnlook,wrkoffer,child_age) %>%
  group_by(year,month,educ_b,age_group,age_group_large,race_group,sex,empstat,labforce,classwkr,wkstat,nilfact,wnlook,wrkoffer,child_age) %>%
  summarise(value = sum(wtfinl)) %>%
  left_join(reference_labels$SEX %>% rename(sex = val,sex_lbl = lbl),by = c('sex')) %>%
  left_join(reference_labels$EMPSTAT %>% rename(empstat = val,empstat_lbl = lbl),by = c('empstat')) %>%
  left_join(reference_labels$LABFORCE %>% rename(labforce = val,labforce_lbl = lbl),by = c('labforce')) %>%
  left_join(reference_labels$CLASSWKR %>% rename(classwkr = val,classwkr_lbl = lbl),by = c('classwkr')) %>%
  left_join(reference_labels$WKSTAT %>% rename(wkstat = val,wkstat_lbl = lbl),by = c('wkstat')) %>%
  left_join(reference_labels$NILFACT %>% rename(nilfact = val,nilfact_lbl = lbl),by = c('nilfact')) %>%
  left_join(reference_labels$WNLOOK %>% rename(wnlook = val,wnlook_lbl = lbl),by = c('wnlook')) %>%
  left_join(reference_labels$WRKOFFER %>% rename(wrkoffer = val,wrkoffer_lbl = lbl),by = c('wrkoffer'))

write_csv(df,'cps_pop_breakdown.csv')



