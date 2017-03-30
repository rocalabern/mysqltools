digorig::do.init()

ch_da <- ms.connect(connData$db_mysql_da_ip)

n_rows = 2
df = data.frame(
  A = rep("asd\"asd", n_rows), 
  B = rep("asd;asd", n_rows), 
  C = rep("asd\nasd", n_rows), 
  D = rep("asd\r\nasd", n_rows),
  E = 1:n_rows,
  F = rep("àáèéìíòóùúñç", n_rows)
  )

strTable = "pmt_da_mycollector.prova"
strFile = "data.csv"

ms.Table.ExportToCSV(df, strFile)
ms.Table.Create(ch_da, strTable, df[FALSE, ])
ms.Table.LoadData(ch_da, strTable, strFile)
