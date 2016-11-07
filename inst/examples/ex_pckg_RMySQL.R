# source("C:/Alabern/Data/ConnData/init_conndata.R")
digorig::do.init()
library(RMySQL)

ch = RMySQL::dbConnect(
  RMySQL::MySQL(), 
  user=connData$db_mysql_user, 
  password=connData$db_mysql_pass, 
  host=connData$db_mysql_ip,
  default.file=connData$db_mysql_ssl_ca_params)

res = DBI::dbSendQuery(ch, "SHOW DATABASES")
df <- DBI::dbFetch(res)
DBI::dbClearResult(res)
df

DBI::dbSendQuery(ch, paste0("use ", connData$db_mysql_schema))

DBI::dbListTables(ch)

DBI::dbListFields(ch, 'table1')
