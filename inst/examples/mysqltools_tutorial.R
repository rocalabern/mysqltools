# source("C:/Alabern/Data/ConnData/init_conndata.R")
digorig::do.init()
library(mysqltools)

ch = ms.connect(host=connData$db_mysql_ip)
ch = ms.connect(host=connData$db_mysql_ip, schema=connData$db_mysql_schema)

ms.showSchemas(ch)
ms.printSchemas(ch)

ms.schema(ch, "schema_name_1")
ms.schema(ch, "schema_name_2")

ms.showTables(ch)
ms.printTables(ch)
ms.printTables(ch, "filt")

df = ms.Query(ch, "select * from schema_name_1.table1 limit 100")
df

dfTableInfo = db.Table.Info(ch, "schema_name_1.table1")

ms.colnames(ch, "table1")

ms.colnames(ch, "schema_name_1.table1")
ms.colnames(ch, "schema_name_2.table2")

ms.close(ch)