digorig::do.init()
ch = ms.connect(host=connData$db_mysql_da_ip)

ms.showSchemas(ch)
ms.showTables(ch, "risk")

ms.printSchemas(ch)
ms.printTables(ch, "identity")
ms.listTables(ch, "identity")

ms.Table.Exists(ch, "risk.alerts_blocked_users")
ms.Table.Exists(ch, "risk.asdasd")

df_r = data.frame(col_A=letters[1:10], col_B=1:10)
ms.Table.Drop(ch, "risk.prova_r")
ms.Table.Create(ch, "risk.prova_r", df_r)
df_db = ms.Table.Read(ch, "risk.prova_r")
ms.Table.Truncate(ch, "risk.prova_r")
df_db_empty = ms.Table.Read(ch, "risk.prova_r")
ms.Table.Drop(ch, "risk.prova_r")
ms.Table.Drop(ch, "risk.prova_r")
df_r
df_db
df_db_empty

ms.Table.Create(ch, "risk.prova_r", df_r)
ms.colnames(ch, "risk.prova_r")
ms.Table.Info(ch, "risk.prova_r")
ms.Table.Drop(ch, "risk.prova_r")

df_r = data.frame(col_A=letters[1:10], col_B=1:10)
ms.Table.Drop(ch, "risk.prova_r")
ms.Table.Create(ch, "risk.prova_r", df_r)

df = ms.Table.Read(ch, "risk.prova_r")
df

ms.Table.Delete(ch, "risk.prova_r", "col_A='c'")

df = ms.Table.Read(ch, "risk.prova_r")
df

ms.Table.Drop(ch, "risk.prova_r")

ms.close(ch)