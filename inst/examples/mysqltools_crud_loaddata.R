digorig::do.init()

ch_da <- ms.connect(connData$db_mysql_da_ip)

n_rows = 100
df = data.frame(
  col_A = rep("asd\"asd", n_rows), 
  col_B = rep("asd;asd", n_rows), 
  col_C = rep("asd\nasd", n_rows), 
  col_D = rep("asd\r\nasd", n_rows),
  col_E = rep("àáèéìíòóùúñç", n_rows),
  col_F = c("", letters[1:(n_rows-1)]),
  col_G = c(as.character(NA), letters[1:(n_rows-1)]),
  col_H = 1:n_rows,
  col_I = c(as.numeric(NA), 2:n_rows),
  col_J = 1:n_rows
  )

strTable = "pmt_da_mycollector.prova"
strFile = "data.csv"

ms.Table.ExportToCSV(df, strFile, quote = TRUE, na = "{{IS_NA}}")
ms.Table.Drop(ch_da, strTable)
ms.Table.Create(ch_da, strTable, df[FALSE, ])
ms.Table.LoadData(ch_da, strTable, strFile, quote = TRUE, use_tag_null = "{{IS_NA}}")
