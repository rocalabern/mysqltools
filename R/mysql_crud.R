# roxygen2::roxygenise()

#' @title ms.Table.Remove
#' @export
ms.Table.Remove <- function (
  ch,
  strTable,
  ...
) {
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n Remove ",strTable," table\n"))
  timer = proc.time()
  DBI::dbRemoveTable(ch, strTable, ...)
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.Drop
#' @export
ms.Table.Drop <- function (
  ch,
  strTable,
  check_exists = TRUE
) {
  if (use_log) {
    multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
    if (check_exists) multiplelines.message(paste0("[Query Input]:\n DROP TABLE IF EXISTS ",strTable,"\n"))
    else multiplelines.message(paste0("[Query Input]:\n DROP TABLE ",strTable," table\n"))
  }
  timer = proc.time()
  if (attr(class(ch), "package") == "RJDBC") {
    if (check_exists) RJDBC::dbSendUpdate(ch, paste0("DROP TABLE IF EXISTS ",strTable))
    else RJDBC::dbSendUpdate(ch, paste0("DROP TABLE ",strTable))
  } else {
    if (check_exists) DBI::dbExecute(ch, paste0("DROP TABLE IF EXISTS ",strTable))
    else DBI::dbExecute(ch, paste0("DROP TABLE ",strTable))
  }
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.Truncate
#' @export
ms.Table.Truncate <- function (
  ch,
  strTable,
  check_exists = TRUE
) {
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n TRUNCATE ",strTable,"\n"))
  timer = proc.time()
  execute_command = TRUE
  if (check_exists) {
    test <- try( 
      {
        suppressWarnings({
          res <- DBI::dbSendQuery(ch, paste0("select * from ",strTable," limit 0"))
        })
        DBI::dbClearResult(res)
      }, silent = TRUE )
    if( ('try-error' %in% class(test)) ) {
      execute_command = FALSE
    }
  }
  if (execute_command) {
    if (attr(class(ch), "package") == "RJDBC") {
      RJDBC::dbSendUpdate(ch, paste0("TRUNCATE ",strTable))
    } else {
      DBI::dbExecute(ch, paste0("TRUNCATE ",strTable))
    }
  }
  
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.Delete
#' @export
ms.Table.Delete <- function(ch, strTable, WHERE_condition, strBooleanClause = "AND") {
  WHERE_Clause = paste0(WHERE_condition, collapse =paste0(" ",strBooleanClause," \n"))
  if( !grepl("where",tolower(WHERE_Clause)) ) { 
    WHERE_Clause = paste0(" WHERE ", WHERE_Clause)
  }
  strSQL = paste0("DELETE FROM ", strTable ," ", WHERE_Clause)
  if (use_log) {
    multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
    multiplelines.message(paste0("[Query Input]:\n",strSQL,"\n"))
  }
  timer = proc.time()
  if (attr(class(ch), "package") == "RJDBC") {
    RJDBC::dbSendUpdate(ch, strSQL)
  } else {
    DBI::dbExecute(ch, strSQL)
  }
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.Read
#' @export
ms.Table.Read <- function (
  ch,
  strTable, 
  df,
  asDataTable = mysqltools:::as.data.table.output,
  ...
) {
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n Read ",strTable," table\n"))
  timer = proc.time()
  # df = DBI::dbReadTable(ch, strTable, ...)
  res <- DBI::dbSendQuery(ch, paste0("select * from ",strTable))
  df <- DBI::dbFetch(res, n=-1)
  DBI::dbClearResult(res)
  timer = round(proc.time() - timer)
  if (class(df)=="character") {
    if (sum(nchar(df))>0)
      warning(paste0("[Query Output] Error:\n",paste0(df, collapse="\n")))
    else
      if (use_log) message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  } else {
    if (use_log) message(paste0("[Query Output] Ok: ",nrow(df)," rows returned.\n"))
  }
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  if (class(df)=="character" && sum(nchar(df))==0) {
    invisible(NULL)
  } else if (asDataTable && class(df)!="character") {
    return(data.table::as.data.table(df))
  } else {
    return(df)
  }
}

#' @title ms.Table.Create
#' @export
ms.Table.Create <- function (
  ch,
  strTable, 
  df,
  dot_replacement = "_",
  ...
) {
  colnames(df) = gsub("\\.", dot_replacement, colnames(df))
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n Create ",strTable," table\n"))
  timer = proc.time()
  if (attr(class(ch), "package") == "RJDBC") {
    DBI::dbWriteTable(ch, strTable, df, ...)
  } else {
    n_elements = unlist(strsplit(strTable, ".", fixed=TRUE))
    if (length(n_elements)>1) {
      DBI::dbSendQuery(ch, paste0("USE ", n_elements[1]))
      DBI::dbWriteTable(ch, n_elements[2], df, row.names=FALSE, ...)
    } else {
      DBI::dbWriteTable(ch, strTable, df, row.names=FALSE, ...)
    }
  }
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.Append
#' @export
ms.Table.Append <- function (
  ch,
  strTable, 
  df,
  dot_replacement = "_",
  ...
) {
  colnames(df) = gsub("\\.", dot_replacement, colnames(df))
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n Create ",strTable," table\n"))
  timer = proc.time()
  DBI::dbWriteTable(ch, strTable, df, row.names=FALSE, append=TRUE, overwrite=FALSE, ...)
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  invisible(NULL)
}

#' @title ms.Table.ExportToCSV
#' @export
ms.Table.ExportToCSV <- function(
  df,
  strFile,
  append = FALSE, 
  quote = FALSE, 
  qmethod = "double",
  sep = ";", 
  row.names = FALSE, 
  fileEncoding = "UTF-8",
  check_doublequotes = quote,
  check_sep = TRUE,
  check_newlines = FALSE,
  replacement = " ",
  na = ""
)
{
  if (check_doublequotes) {
    for (col in colnames(df)) {
      if (any(grepl("\"", df[[col]], fixed=FALSE))) {
        warning(paste0(col, " : contains double quotes"))
        df[[col]] = gsub("\"",replacement,df[[col]], fixed=FALSE)
      }
    }
  }
  if (check_sep) {
    for (col in colnames(df)) {
      if (any(grepl(sep, df[[col]], fixed=FALSE))) {
        warning(paste0(col, " : contains separator"))
        df[[col]] = gsub(sep,replacement,df[[col]], fixed=FALSE)
      }
    }
  }
  if (check_newlines) {
    for (col in colnames(df)) {
      if (any(grepl("\n", df[[col]], fixed=FALSE))) {
        warning(paste0(col, " : contains new lines"))
        df[[col]] = gsub("\r\n",replacement,df[[col]], fixed=FALSE)
        df[[col]] = gsub("\n",replacement,df[[col]], fixed=FALSE)
      }
    }
  }
  write.table(df, file=strFile, append=append, quote=quote, sep=sep, row.names=row.names, fileEncoding=fileEncoding, na = na)
  invisible(df)
}

#' @title ms.Table.LoadData
#' @export
ms.Table.LoadData <- function(
  ch, 
  strTable, 
  strFile, 
  encoding = "utf8", 
  skip = 1 ,
  sep = ";",
  quote = FALSE,
  quote_char = "\"",
  local = TRUE,
  use_tag_null = NULL)
{
  strQueryTagNull = ""
  if (!is.null(use_tag_null)) {
    if (local) {
      if (quote) dfTable = data.table::fread(strFile, sep = sep, nrows = 1, quote = quote_char)
      else dfTable = data.table::fread(strFile, sep = sep, nrows = 1, quote = "")
      strQueryTagNull_first = paste0("(", paste0("@col", colnames(dfTable), collapse=", "), ")")
      strQueryTagNull_second = paste0("SET \n", paste0(colnames(dfTable), " = nullif(@col", colnames(dfTable), ", '",use_tag_null,"')", collapse=",\n"))
      strQueryTagNull = paste0(strQueryTagNull_first, strQueryTagNull_second)
    } else {
      warning("File is not local, so tag null cannot be used.")
    }
  }
  
  paste0()
  if (local) strLocal = "LOCAL "
  else strLocal = ""
  ms.Query(ch, paste0(
"LOAD DATA ",strLocal,"INFILE '",strFile,"'", " 
INTO TABLE ",strTable," 
  CHARACTER SET ",encoding," 
  FIELDS TERMINATED BY '",sep,"'","
  ",ifelse(quote, "ENCLOSED BY '\"'","" ),"
  IGNORE ", skip," LINES ","
  ",strQueryTagNull))
}