digorig::do.init()
ch_da = ms.connect(host=connData$db_mysql_da_ip)

ms.showSchemas(ch_da)
ms.showTables(ch_da, "risk")
ms.showTables(ch_da, "pmt_da_mycollector")

ms.printSchemas(ch_da)
ms.showTables(ch_da, "risk")
ms.printTables(ch_da, "identity")
ms.listTables(ch_da, "identity")

ms.Table.Exists(ch_da, "risk.alerts_blocked_users")
ms.Table.Exists(ch_da, "risk.asdasd")

df_r = data.frame(col_A=letters[1:10], col_B=1:10)
ms.Table.Drop(ch_da, "risk.prova_r")
ms.Table.Create(ch_da, "risk.prova_r", df_r)
df_db_select = ms.Query(ch_da, "select * from risk.prova_r")
df_db = ms.Table.Read(ch_da, "risk.prova_r")
ms.Table.Truncate(ch_da, "risk.prova_r")
df_db_empty = ms.Table.Read(ch_da, "risk.prova_r")
ms.Table.Drop(ch_da, "risk.prova_r")
ms.Table.Drop(ch_da, "risk.prova_r")
df_r
df_db_select
df_db
df_db_empty

ms.Table.Create(ch_da, "risk.prova_r", df_r)
ms.colnames(ch_da, "risk.prova_r")
ms.Table.Info(ch_da, "risk.prova_r")
ms.Table.Drop(ch_da, "risk.prova_r")

df_r = data.frame(col_A=letters[1:10], col_B=1:10)
ms.Table.Drop(ch_da, "risk.prova_r")
ms.Table.Create(ch_da, "risk.prova_r", df_r)

df = ms.Table.Read(ch_da, "risk.prova_r")
df

ms.Table.Delete(ch_da, "risk.prova_r", "col_A='c'")

df = ms.Table.Read(ch_da, "risk.prova_r")
df

ms.Table.Drop(ch_da, "risk.prova_r")

ms.close(ch_da)