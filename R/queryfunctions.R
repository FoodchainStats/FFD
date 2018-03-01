
# Functions for UK Overview section -----------------------------------------


####
# Get UK Balance data
#
# returns: tradeflow, tradevalue
####



get_uk_balance_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, commoditygroup = "ffd") {

  require(dplyr)
  require(RPostgreSQL)
  
  cg <- switch(commoditygroup, "ffd" = "ffd_desc",
               "ffdplus" = "ffd_plus",
               "hs4" = "hs4_description",
               "hs6" = "hs6_description",
               "cn8" = "com_description")
  
  
  if(cg == "ffd_desc") { cg <- "and cn.ffd_desc != 'Not entered'"}
  else {cg <- ""}
  
  
  
  sql <- paste("select case when t.type in ('E','D') then 'exports' 
                        when t.type in ('A', 'I') then 'imports' end as tradeflow, sum(t.value) as tradevalue 
                        from ", tradetable, " as t
                        join cncodes as cn on t.comcode = cn.com_code
                        where t.year = ", tradeyear, cg, 
                        "group by tradeflow
                         order by tradeflow")
  
  returndata <- dbGetQuery(db, sql)
  returndata
  
}



####
# Get UK Market data
#
# returns: country, year, tradevalue, % change, proportion
#          tradevolume
####

  get_uk_market_data <- function (db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                           "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup, "ffd" = "ffd_desc",
                                 "ffdplus" = "ffd_plus",
                                 "hs4" = "hs4_description",
                                 "hs6" = "hs6_description",
                                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cg <- "and cn.ffd_desc != 'Not entered'"}
    else {cg <- ""}
    
    sql <- paste("select g.master_country_name as country,
                           g.continent,  
                           year,
                           sum(t.value) as tradevalue,
                           sum(t.netmass)/1000 as tradevolume
                           from ", tradetable, " as t
                           join geo as g on t.codseq = g.country_id
                           join cncodes as cn on t.comcode = cn.com_code
                           where t.type ", ft, cg, 
                 " group by g.master_country_name, g.continent, year
                           order by g.master_country_name, year")
    
    d <- dbGetQuery(db, sql)
    
    returndata <- d %>%
      group_by(country) %>%
      mutate(pctchg = (tradevalue/lag(tradevalue))-1) %>%
      filter(year == tradeyear) %>%
      group_by(year) %>% 
      mutate(propn = tradevalue/sum(tradevalue)) %>% 
      arrange(-tradevalue) %>% 
      select(country:tradevalue, pctchg, propn, tradevolume)
    
    returndata
    
  }
  

  
  
####
# Get UK Product data
#
# returns: commodity, year, tradevalue, % change, proportion
#          tradevolume
####
  
  
  get_uk_product_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "ffd_desc",
                     "ffdplus" = "ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
    
    addffd <- switch(commoditygroup,
                 "ffd" = "",
                 "ffdplus" = "",
                 "hs4" = "",
                 "hs6" = "",
                 "cn8" = "cn.ffd_plus, ")
    
    
    
    sql <- paste("select ", cgtext," as commodity, ", addffd, 
                           "t.year,
                           sum(t.value) as tradevalue,
                           sum(t.netmass)/1000 as tradevolume
                   from ", tradetable," as t
                   join geo as g on t.codseq = g.country_id
                   join cncodes as cn on t.comcode = cn.com_code
                   where t.type ", ft, cgfilter,
                  " group by commodity, ", addffd, "t.year
                   order by commodity, ", addffd, "t.year", sep = "")
    
    d <- dbGetQuery(db, sql)
    
    returndata <- d %>%
      group_by(commodity) %>%
      mutate(pctchg = (tradevalue/lag(tradevalue))-1) %>%
      filter(year == tradeyear) %>%
      group_by(year) %>% 
      mutate(propn = tradevalue/sum(tradevalue)) %>% 
      arrange(-tradevalue) %>% 
      select(commodity:tradevalue, pctchg, propn, tradevolume)
    
    returndata
    
  }
  

  

# Functions for Country view section --------------------------------------

####
# Make country balance data
# 
# Returns:
#  
####
    
  get_country_balance_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, commoditygroup = "ffd", country = "France"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    
    
    sql <- paste("select case when t.type in ('E','D') then 'exports' 
                              when t.type in ('A', 'I') then 'imports' end as tradeflow,
                         sum(t.value) as tradevalue 
            from ", tradetable, " as t
            join geo as g on g.country_id = t.codseq
            join cncodes as cn on t.comcode = cn.com_code
            where g.master_country_name = '", country,
                 "' and year = ", tradeyear, cgfilter,
                 " group by tradeflow;", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata 
    
  }
  
  
####
# Make country top trade
# 
# Returns: Country, commodity, year, tradevalue, 
#          % change, proportion, tradevolume
#  
####
  
  
  
  get_country_top_trade <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", country = "France") {

    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    

    cgtext <- switch(commoditygroup,
                     "ffd" = "ffd_desc",
                     "ffdplus" = "ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
 
     addffd <- switch(commoditygroup,
                     "ffd" = "",
                     "ffdplus" = "",
                     "hs4" = "",
                     "hs6" = "",
                     "cn8" = "cn.ffd_plus, ")  
        
    sql <- paste("select g.master_country_name, ",
                           cgtext, " as commodity, ", addffd,
                           "t.year,
                           sum(t.value) as tradevalue,
                           sum(t.netmass)/1000 as tradevolume
            from ", tradetable, " as t
            join cncodes as cn on t.comcode = cn.com_code
            join geo as g on g.country_id = t.codseq
            where g.master_country_name = '", country,  
                 "' and t.type ", ft, cgfilter,
            "group by g.master_country_name, commodity, ", addffd, "year
            order by g.master_country_name, commodity, ", addffd, "year", sep = "")
    
    d <- dbGetQuery(db, sql)
    
    returndata <- d %>% 
      group_by(commodity) %>% 
      mutate(pctchg = (tradevalue/lag(tradevalue))-1) %>% 
      filter(year == tradeyear) %>% 
      group_by(year) %>% 
      mutate(propn = tradevalue/sum(tradevalue)) %>% 
      arrange(-tradevalue) %>%
      select(master_country_name:tradevalue, pctchg, propn, tradevolume)
    
    returndata
  }
  

  
####
# Make country total trade
# 
# Returns: Country, year, tradevalue, 
#  
####
  
  
  get_country_tot_trade <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", country = "France"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "ffd_desc",
                     "ffdplus" = "ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
    
    
    sql <- paste("select g.master_country_name, t.year, sum(t.value) as tradevalue
              from ", tradetable, " as t
              join cncodes as cn on t.comcode = cn.com_code
              join geo as g on g.country_id = t.codseq
              where g.master_country_name = '", country,  
                 "' and t.type ", ft,
                 "and year = ", tradeyear, cgfilter,
              "group by g.master_country_name, year
              order by g.master_country_name, year", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata
    
  }
  
 
####
# Commodity dropdown list
# 
# Returns: Commodity 
#  
#### 
   
  get_commodity_list <- function(db, tradetable = "trademonthly", flowtype = "exports", commoditygroup = "ffd", country = "France"){

    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "cn.ffd_desc",
                     "ffdplus" = "cn.ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
    
  sql <- paste("select cn.",cg ," as code, ", cgtext," as commodity 
              from ", tradetable, " as t
              join cncodes as cn on t.comcode = cn.com_code
              join geo as g on g.country_id = t.codseq
              where g.master_country_name = '", country,  
                               "' and t.type ", ft, cgfilter, 
              " group by code, commodity
              order by code, commodity", sep = "")
  
  returndata <- dbGetQuery(db, sql) 
  
  returndata
  
  }
  
  
####
# Make country commodity data 
# 
# Returns: Commodity 
#  
####   
  
  get_country_commodity_data <- function(db, tradetable = "trademonthly", flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese", country = "France"){

    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "ffd_desc",
                     "ffdplus" = "ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")

        
    sql <- paste("select t.year, t.month, ", cgtext," as commodity,
               sum(t.value) as tradevalue,
               sum(t.netmass)/1000 as tradevolume
               from ", tradetable, " as t
               join geo as g on g.country_id = t.codseq
               join cncodes as cn on t.comcode = cn.com_code
               where g.master_country_name = '", country,
                 "' and cn.", cg,"  = '", commodity,  
                 "' and type ", ft,
                 " group by t.year, t.month, commodity
               order by t.year, t.month, commodity", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata

    
  }
  
  
  
  
  
  
# Functions for Product View section --------------------------------------------------

  ####
  # Commodity dropdown list
  # 
  # Returns: Commodity 
  #  
  #### 
  
  get_global_commodity_list <- function(db, commoditygroup = "ffd"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    if(cg == "ffd_desc") { cgfilter <- " where cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "cn.ffd_desc",
                     "ffdplus" = "cn.ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
    
    sql <- paste("select cn.",cg ," as code, ", cgtext," as commodity 
                 from cncodes as cn", cgfilter," group by code, commodity order by commodity", sep = "")
    
    returndata <- dbGetQuery(db, sql) 
    
    returndata
    
  }
  
  
  
    
  
####
# Make country markets data
# 
# Returns: UN ISO2 country, commodity, tradevalue, 
#  
####
  
  get_country_markets_data <- function(db, tradetable = "trademonthly", tradeyear = "2016", flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    cgtext <- switch(commoditygroup,
                     "ffd" = "ffd_desc",
                     "ffdplus" = "ffd_plus",
                     "hs4" = "to_char(cn.hs4_code, '0000') || ' - ' || cn.hs4_description",
                     "hs6" = "to_char(cn.hs6_code, '000000') || ' - ' || cn.hs6_description",
                     "cn8" = "to_char(cn.com_code, '00000000') || ' - ' || cn.com_description")
    
    sql <- paste("select g.un_code, cn.", cg,", sum(t.value) as exports
            from ", tradetable, " as t
            join geo as g on t.codseq = g.country_id
            join cncodes as cn on t.comcode = cn.com_code
            where cn.", cg," = '", commodity, 
                 "' and t.year = ", tradeyear, "
            and t.type ", ft,
                 "group by g.un_code, cn.", cg, sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata
    
  }
  

####
# Make country total exports data
# 
# Returns: UN ISO2 country, commodity, tradevalue, 
#  
#### 
  
  
  get_country_totexports_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    sql <- paste("select g.un_code, cn.", cg,", sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 where cn.", cg," = '", commodity, 
                 "' and t.year = ", tradeyear, 
                 " and t.type ", ft,
                 " group by g.un_code, cn.", cg, sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata
  }

  
  
  
  
####
# Make product map data
# 
# Returns: UN ISO3 country, commodity, tradevalue, 
#          Plotly plotcolour, Plotly hovertext
#  
#### 

  
  get_product_map_data <- function (db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    sql <- paste("select i.iso_3166_1_3_letter_code as code, g.master_country_name as country, cn.", cg,", sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 join isocountries as i on g.un_code = i.iso_3166_1_2_letter_code
                 where cn.", cg," = '", commodity, 
                 "' and t.year = ", tradeyear, 
                 "and t.type ", ft,
                 "group by i.iso_3166_1_3_letter_code, g.master_country_name, cn.", cg, sep = "")
    
    chtdata <- dbGetQuery(db, sql)
    chtdata$plotcol <- log(chtdata$tradevalue)
    chtdata <- chtdata[order(-chtdata$tradevalue), ]
    chtdata$hover <- with(chtdata, paste(country, '<br>', money(tradevalue), '<br>', "Rank: ", which(chtdata$country == country)))  
    
    # There seems to be the odd zero value (esp for Malta)
    # which breaks the chart, so we filter them out
    returndata <- chtdata %>% 
      filter(tradevalue > 0)
    
    returndata
    
  }
  


####
# Make product map table
# 
# Returns: UN ISO2, country, commodity, year, tradevalue, 
#          % change, proportion, tradevolume
#  
####     
  
  get_product_map_table <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese"){
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    sql <- paste("select g.un_code,
                 g.master_country_name as country, 
                 g.continent,
                 cn.", cg," as commodity, 
                 year, 
                 sum(t.value) as tradevalue,
                 sum(t.netmass)/1000 as tradevolume
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 where cn.", cg," = '", commodity,
                 "' and t.type ", ft, cgfilter,
                 "group by g.un_code, g.master_country_name, g.continent, cn.", cg,", year
                 order by g.un_code, g.master_country_name, g.continent, cn.", cg,", year", sep = "")
    
    d <- dbGetQuery(db, sql)
    
    returndata <- d %>%
      group_by(country, commodity) %>%
      mutate(pctchg = (tradevalue/lag(tradevalue))-1) %>%
      filter(year == tradeyear) %>%
      group_by(year) %>% 
      mutate(propn = tradevalue/sum(tradevalue)) %>% 
      arrange(-tradevalue) %>%
      select(un_code:tradevalue, pctchg, propn, tradevolume)
    
    returndata
    
  }
  
  
  
####
# Make product top trade
# 
# Returns: UN ISO3, country, commodity, tradevalue
#  
####    
  
  get_product_top_trade <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    sql <- paste("select i.iso_3166_1_3_letter_code as code, g.master_country_name as country, cn.", cg," as commodity, sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 join isocountries as i on g.un_code = i.iso_3166_1_2_letter_code
                 where cn.", cg," = '", commodity, "' and t.year = ", tradeyear,
                 "and t.type ", ft,
                 "group by i.iso_3166_1_3_letter_code, g.master_country_name, commodity
                 order by tradevalue desc limit 10", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata
    
  }
  
  
  
####
# Make product treemap data
# 
# Returns: continent, country, commodity, year, tradevalue
#  
####   
  
  get_product_treemap_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese") {
  
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    sql <- paste("select g.continent, g.master_country_name as country, cn.", cg," as commodity, year, sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 where cn.", cg," = '", commodity,
                 "' and t.type ", ft, cgfilter,
                 "and year = ", tradeyear,
                 "group by g.continent, g.master_country_name, commodity, year
                 order by g.continent, g.master_country_name, commodity, year", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    returndata
    
  }
  
  
  
####
# Make product eu/non eu data
# 
# Returns: eunoneu, tottrade
#  
####    
  
  get_product_eunoneu_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd", commodity = "Cheese") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = "ffd_desc",
                 "ffdplus" = "ffd_plus",
                 "hs4" = "hs4_description",
                 "hs6" = "hs6_description",
                 "cn8" = "com_description")
    
    
    if(cg == "ffd_desc") { cgfilter <- " and cn.ffd_desc != 'Not entered'"}
    else {cgfilter <- ""}
    
    
    eu <- c("AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA", "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR")
    
    sql <- paste("select g.eu_non_eu as eunoneu, g.master_country_name, cn.", cg," as commodity, sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code
                 where cn.", cg," = '", commodity, 
                 "' and t.year = ", tradeyear, 
                 " and t.type ", ft,
                 "group by g.eu_non_eu, g.master_country_name, commodity", sep = "")
    
    d <- dbGetQuery(db, sql)
    
    returndata <- d %>% 
      select(c(eunoneu, tradevalue)) %>% 
      group_by(eunoneu) %>% 
      summarise(tottrade = sum(tradevalue))
    
    returndata
  }
  
  
  ####
  # Make all eu/non eu data
  # 
  # Returns: eunoneu, tottrade
  #  
  ####    
  
  get_all_eunoneu_data <- function(db, tradetable = "trademonthly", tradeyear = 2016, flowtype = "exports", commoditygroup = "ffd") {
    
    require(dplyr)
    require(RPostgreSQL)
    
    ft <- switch(flowtype, "exports" = "in ('E', 'D')",
                 "imports" = "in ('A', 'I')")
    
    cg <- switch(commoditygroup,
                 "ffd" = " and cn.ffd_desc != 'Not entered'",
                 "ffdplus" = "",
                 "hs4" = "",
                 "hs6" = "",
                 "cn8" = "")
    
    

    eu <- c("AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA", "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR")
    
    sql <- paste("select g.eu_non_eu as eunoneu, sum(t.value) as tradevalue
                 from ", tradetable, " as t
                 join geo as g on t.codseq = g.country_id
                 join cncodes as cn on t.comcode = cn.com_code",
                 " where t.year = ", tradeyear, 
                 " and t.type ", ft,
                 cg, 
                 "group by g.eu_non_eu", sep = "")
    
    returndata <- dbGetQuery(db, sql)
    
    
    returndata
  }
  
  