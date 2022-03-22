# install.packages("devtools")
# install.packages("hoopR")

# library(hoopR)
# library(writexl)

# Download all play by play data since 2006
# The files can be huge so I recommend saving out the dataframes occassionally to avoid memory limits
# # I set my working directory to the github data folder
mbb_pbp_2006_2007 <-  hoopR::load_mbb_pbp(2006:2007)
mbb_pbp_2007_2008 <-  hoopR::load_mbb_pbp(2007:2008)
mbb_pbp_2008_2009 <-  hoopR::load_mbb_pbp(2008:2009)
mbb_pbp_2009_2010 <-  hoopR::load_mbb_pbp(2009:2010)
mbb_pbp_2010_2011 <-  hoopR::load_mbb_pbp(2010:2011)
mbb_pbp_2011_2012 <-  hoopR::load_mbb_pbp(2011:2012)
mbb_pbp_2012_2013 <-  hoopR::load_mbb_pbp(2012:2013)
mbb_pbp_2013_2014 <-  hoopR::load_mbb_pbp(2013:2014)
mbb_pbp_2014_2015 <-  hoopR::load_mbb_pbp(2014:2015)
mbb_pbp_2015_2016 <-  hoopR::load_mbb_pbp(2015:2016)
mbb_pbp_2016_2017 <-  hoopR::load_mbb_pbp(2016:2017)
mbb_pbp_2017_2018 <-  hoopR::load_mbb_pbp(2017:2018)
mbb_pbp_2018_2019 <-  hoopR::load_mbb_pbp(2018:2019)
mbb_pbp_2019_2020 <-  hoopR::load_mbb_pbp(2019:2020)
mbb_pbp_2020_2021 <-  hoopR::load_mbb_pbp(2020:2021)
mbb_pbp_2021_2022 <-  hoopR::load_mbb_pbp(2021:2022)

# Write out each dataframe to a CSV (if you want to do it one by one)
# write.csv(mbb_pbp_2010_2011 , file = "mbb_pbp_2010_2011.csv")

# Write out each dataframe to a CSV (looping and doing batch)
nms <- ls(pattern = "^m................$") # we know that each dataframe is 17 characters long and starts with an 'm'
for(nm in nms) {
  X <- get(nm)
  if (is.data.frame(X)) {
    filename <- paste0(nm, ".csv")
    message("Writing out ", filename)
    write.csv(X, file = filename, row.names = FALSE)
  }
}