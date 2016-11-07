# roxygen2::roxygenise()

simplifyText <- function(text) {
  text = tolower(text)
  text = gsub("\r|\n|\t", "", text)
  text = gsub("(", "", text, fixed=TRUE)
  text = gsub(")", "", text, fixed=TRUE)
  text = gsub(" ", "", text, fixed=TRUE)
  return (text)
}

multiplelines.message <- function (strText) {
  #   writeLines(strwrap(strText, width=73))
#   strText = unlist(strsplit(strText, "\r\n"))
  strText = unlist(strsplit(strText, "\n"))
  for (line in strText) message(line)
}

isSelect <- function(text) {
  text = simplifyText(text)
  return (substr(text, 1, 6) == "select")
}

#' @title ms.connect
#' @export
ms.connect <- function (
  host,
  schema = NULL,
  user = connData$IAM_user, 
  pass = connData$IAM_pass,
  ssl_ca_params = connData$db_mysql_ssl_ca_params,
  use_ssl = !is.null(connData$db_mysql_ssl_ca_params),
  jar = system.file("java", "mysql-connector-java-5.1.40-bin.jar", package = "mysqltools")
) {
  library(DBI)
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n Connect \n"))
  if (use_JDBC) {
    library(RJDBC)
    
    strParams = ""
    if (use_ssl) strParams = paste0("?verifyServerCertificate=false&useSSL=true&requireSSL=true")
    
    drv <- JDBC(
      "com.mysql.jdbc.Driver",
      jar)
    
    ch <- dbConnect(
      drv, 
      url = paste0("jdbc:mysql://",connData$db_mysql_pmt_ip,":",connData$db_mysql_pmt_port,"/",connData$db_mysql_pmt_schema,strParams), 
      user = user,
      pass = pass)
  } else {
    library(RMySQL)
    
    drv <- RMySQL::MySQL()
    
    ch <- RMySQL::dbConnect(
      drv, 
      user = user, 
      password = pass, 
      host = host,
      default.file = ssl_ca_params)
  }
  if (!is.null(schema)) {
    multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
    multiplelines.message(paste0("[Query Input]:\n USE ",schema," \n"))
    DBI::dbSendQuery(ch, paste0("use ", schema))
  }
  if (tolower(Sys.info()['sysname']) != "windows") {
    dbGetQuery(ch,'SET NAMES utf8')
  }
  
  return(ch)
}

#' @title ms.Use
#' @export
ms.Use <- function (
  ch,
  schema
) {
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n USe ",schema," \n"))
  DBI::dbSendQuery(ch, paste0("use ", schema))
}

#' @title ms.close
#' @export
ms.close <- function (ch = ch) {
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n Close Connection \n"))
  DBI::dbDisconnect(ch)
}

#' @title ms.Query
#' @export
ms.Query <- function(ch, query, asDataTable=mysqltools:::as.data.table.output, clearResulset=mysqltools:::clear.resulset, limit=-1) {
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n",query,"\n"))
  timer = proc.time()
  if (clearResulset) {
    ms.ClearResults(ch)
  }
  if (isSelect(query)) {
    if (limit>=0) query = paste0(query," limit ",limit)
    suppressWarnings({
    res <- DBI::dbSendQuery(ch, query)
    })
    df <- DBI::dbFetch(res, n=-1)
    DBI::dbClearResult(res)
  } else {
    DBI::dbSendQuery(ch, query)
    df = ""
  }
  timer = round(proc.time() - timer)
  if (class(df)=="character") {
    if (sum(nchar(df))>0)
      warning(paste0("[Query Output] Error:\n",paste0(df, collapse="\n")))
    else
      message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  } else {
    message(paste0("[Query Output] Ok: ",nrow(df)," rows returned.\n"))
  }
  message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  if (class(df)=="character" && sum(nchar(df))==0) {
    invisible(NULL)
  } else if (asDataTable && class(df)!="character") {
    return(data.table::as.data.table(df))
  } else {
    return(df)
  }
}

#' @title ms.ClearResults
#' @export
ms.ClearResults <- function(ch) {
  tryCatch({
    if (class(ch) != "JDBCConnection") {
      listResults = dbListResults(ch)
      if (length(listResults)>0) {
        message(paste0("[Clearing...]"))
        DBI::dbClearResult(dbListResults(ch)[[1]])
        message(paste0("[Cleared]"))
      }
    }
  }, error = function(e) {message(e)})
  invisible(NULL)
}

#' @title db.Table.Info
#' @export
db.Table.Info <- function(ch, strTable) {
  strSQL = paste0("select * from ", strTable)
  result <- ms.Query(ch, strSQL, limit=1)
  listAllVariables <- colnames(result)
  listAllType <- sapply(result, class) # apply(result, 2, class)
  nvar = length(listAllVariables)
  
  tableSplited = strsplit(strTable, ".", fixed = TRUE)[[1]]
  if (length(tableSplited)>1) {
    strTableSchema = tableSplited[1]
    strTableName = paste(tableSplited[-1], collapse=".")
  } else {
    strTableSchema = ""
    strTableName = strTable
  }

  listTYPENAME = listAllType
  listTYPENAME[listTYPENAME=="logical"] = "smallint"
  listTYPENAME[listTYPENAME=="integer"] = "integer"
  listTYPENAME[listTYPENAME=="numeric"] = "numeric"
  listTYPENAME[listTYPENAME=="character"] = "varchar"
  listTYPENAME[listTYPENAME=="factor"] = "varchar"
  info = data.frame(
    TABLE_CAT = character(nvar),
    TABLE_SCHEM = rep(strTableSchema, nvar),
    TABLE_NAME = rep(strTableName, nvar),
    COLUMN_NAME = listAllVariables,
    DATA_TYPE = integer(nvar),
    TYPE_NAME = listTYPENAME,
    COLUMN_SIZE = integer(nvar),
    BUFFER_LENGTH = integer(nvar),
    DECIMAL_DIGITS = integer(nvar),
    NUM_PREC_RADIX = integer(nvar),
    NULLABLE = integer(nvar),
    REMARKS = character(nvar),
    COLUMN_DEF = character(nvar),
    SQL_DATA_TYPE = integer(nvar),
    SQL_DATETIME_SUB = integer(nvar),
    CHAR_OCTET_LENGTH = integer(nvar),
    ORDINAL_POSITION = integer(nvar),
    IS_NULLABLE = character(nvar)
  )
  primaryKeys = data.frame(
    TABLE_CAT = character(),
    TABLE_SCHEM = character(),
    TABLE_NAME = character(),
    COLUMN_NAME = character(),
    KEY_SEQ = integer(),
    PK_NAME = character()
  )

  listCategorical <- character()
  listNumeric <- character()
  for (i in 1:nvar) {
    strVar = listAllVariables[i]
    if (info$TYPE_NAME[i] %in% categoricalTypes) {
      listCategorical <- c(listCategorical, strVar)
    }
    if (info$TYPE_NAME[i] %in% numericTypes) {
      listNumeric <- c(listNumeric, strVar)
    }
  }
  return (list(
    table = strTable,
    info = info,
    primaryKeys = primaryKeys,
    listAllVariables = listAllVariables,
    listAllType = listAllType,
    listCategorical = listCategorical,
    listNumeric = listNumeric,
    nvar = nvar
  ))
}
