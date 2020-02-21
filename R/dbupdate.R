library(DBI)
library(odbc)
library(here)
library(tidyverse)


# Connect ------------------------------------------
credentials <- readRDS("~/Documents/dbconnect.rds")

tradedata <- DBI::dbConnect(odbc::odbc(),
                            driver = "PostgreSQL",
                            database = "tradedata",
                            UID    = credentials$UID[1],
                            PWD    = credentials$PWD[1],
                            host = "localhost",
                            port = 5432)

# Update -------------------------------------------
# Delete and replace a years worth of data
# Check that we have records first
sql = "select * from trademonthly where year = 2019"
x <- dbGetQuery(conn = tradedata, statement = sql)

# Then delete with care!
dbSendQuery(conn = tradedata, statement = "delete from trademonthly where year = 2018")

# Finally update with new year
newdata <- read.csv(here("data", "2019.csv"), header = TRUE)
colnames(newdata) <- c("year", "month", "type", "comcode", "codseq", "cooseq", "tradeind", "mot", "cbcode", "value", "netmass", "suppunit", "rectype", "sitc")
newdata$comcode <- newdata$comcode/10
dbWriteTable(conn = tradedata, name = "trademonthly", newdata, row.names = FALSE, append = TRUE)


#
# Make tradeannual table----------------------------------------------
#

ft <- c(year = "integer",
        type = "varchar(1)",
        comcode = "integer",
        codseq = "integer",
        cooseq = "integer", 
        tradeind = "integer",
        mot = "integer",
        cbcode = "integer",
        rectype = "integer",
        sitc = "integer",
        value = "bigint",
        netmass = "bigint",
        suppunit = "bigint")

sql <- "select year, type, comcode, codseq, cooseq, tradeind, mot, cbcode, rectype, sitc, sum(value) as value, sum(netmass) as netmass, sum(suppunit) as suppunit
        from trademonthly
        group by year, type, comcode, codseq, cooseq, tradeind, mot, cbcode, rectype, sitc"

x <- dbGetQuery(tradedata, sql)

dbWriteTable(tradedata, name = "tradeannual", x, field.types = ft, row.names = FALSE, overwrite = TRUE)


# Misc ------------------------------------------------------------------
# make names db safe: no '.' or other illegal characters,
# all lower case and unique
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}


cncodes <- here("data", "CN CODES MASTER TABLE.csv")
geocodes <- here("data", "GEO MASTER TABLE.csv")
iso2codes <- here("data", "iso.csv")

cn <- read.csv(cncodes)
colnames(cn) <- dbSafeNames(colnames(cn))
cn$com_code <- cn$com_code/10
cn <- cn %>% 
  mutate(com_description = iconv(com_description, to = "UTF-8"),
         sit_description = iconv(sit_description, to = "UTF-8"),
         div_description = iconv(div_description, to = "UTF-8"),
         hs6_description = iconv(hs6_description, to = "UTF-8"),
         hs4_description = iconv(hs4_description, to = "UTF-8"),
         hs2_description = iconv(hs2_description, to = "UTF-8"))
dbWriteTable(conn = tradedata, name = "cncodes", cn, row.names = FALSE, overwrite = TRUE)

geo <- read.csv(geocodes)
colnames(geo) <- dbSafeNames(colnames(geo))
dbWriteTable(conn = tradedata, name = "geo", geo, row.names = FALSE, overwrite = TRUE)

#from https://datahub.io/dataset/iso-3166-1-alpha-2-country-codes/resource/9c3b30dd-f5f3-4bbe-a3cb-d7b2c21d66ce
ISOcountries <- read.csv(iso2codes)
colnames(ISOcountries) <- dbSafeNames(colnames(ISOcountries))
dbWriteTable(conn = tradedata, name = "isocountries", ISOcountries, row.names = FALSE, overwrite = TRUE)
