# roxygen2::roxygenise()

#' @title ms.sql.Create
#' @export
ms.sql.Create <- function (
  ch,
  strTable, 
  df,
  dot_replacement = "_",
  ...
) {
  colnames(df) = gsub("\\.", dot_replacement, colnames(df))
  strSQL = DBI::sqlCreateTable(ch, strTable, df, ...)
  return(strSQL)
}

#' @title ms.sql.Append
#' @export
ms.sql.Append <- function (
  ch,
  strTable, 
  df,
  dot_replacement = "_",
  ...
) {
  colnames(df) = gsub("\\.", dot_replacement, colnames(df))
  strSQL = DBI::sqlAppendTable(ch, strTable, df, ...)
  return(strSQL)
}