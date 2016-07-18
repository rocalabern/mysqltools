# source("C:/Alabern/Data/ConnData/init_conndata.R")
digorig::do.init()
library(RMySQL)

mydb = RMySQL::dbConnect(
  RMySQL::MySQL(), 
  user=connData$db_mysql_user, 
  password=connData$db_mysql_pass, 
  host=connData$db_mysql_ip,
  default.file=connData$db_mysql_ssl_ca_params)

res = DBI::dbSendQuery(ch, "SHOW DATABASES")
df <- DBI::dbFetch(res)
DBI::dbClearResult(res)
df

DBI::dbSendQuery(mydb, paste0("use ", connData$db_mysql_schema))

DBI::dbListTables(mydb)

DBI::dbListFields(mydb, 'table1')
