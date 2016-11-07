digorig::do.init()
library(RJDBC)

drv <- JDBC(
  "com.mysql.jdbc.Driver",
  "inst/java/mysql-connector-java-5.1.40-bin.jar"
  # "inst/java/mysql-connector-java-5.1.40-bin.jar"
  # ,identifier.quote="`"
  )

ch <- dbConnect(
  drv, 
  url = paste0(
    "jdbc:mysql://",connData$db_mysql_pmt_ip,":",connData$db_mysql_pmt_port,"/",connData$db_mysql_pmt_schema,
    "?verifyServerCertificate=false&useSSL=true&requireSSL=true"
    ), 
  user = connData$db_mysql_pmt_user,
  pass = connData$db_mysql_pmt_pass)

res = DBI::dbSendQuery(ch, "SHOW DATABASES")
df <- DBI::dbFetch(res)
DBI::dbClearResult(res)
df

DBI::dbSendQuery(ch, paste0("use ", connData$db_mysql_schema))

DBI::dbListTables(ch)

DBI::dbListFields(ch, 'table1')