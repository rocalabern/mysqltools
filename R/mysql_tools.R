# roxygen2::roxygenise()

#' @title ms.schema
#' @export
ms.schema <- function(ch, schema) {
  ms.Query(ch, paste0("use ", schema))
}

#' @title ms.showSchemas
#' @export
ms.showSchemas <- function (
  ch
) {
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n \tSHOW DATABASES \n"))
  timer = proc.time()
  res = DBI::dbSendQuery(ch, "SHOW SCHEMAS")
  df <- DBI::dbFetch(res, n=-1)
  DBI::dbClearResult(res)
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  df = data.frame(SCHEMA=df[[1]], stringsAsFactors = FALSE)
  return (df)
}

#' @title ms.showTables
#' @export
ms.showTables <- function (
  ch,
  schema = NULL
) {
  if (!is.null(schema)) ms.schema(ch, schema)
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n \tSHOW TABLES \n"))
  timer = proc.time()
  res = DBI::dbSendQuery(ch, "SHOW TABLES")
  df <- DBI::dbFetch(res, n=-1)
  DBI::dbClearResult(res)
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  return (df)
}

#' @title ms.listTables
#' @export
ms.listTables <- function (ch, pattern="", fixed=TRUE) {
  df = ms.showTables(ch)
  listTables = as.character(df[[1]])
  if (!missing(pattern) && !is.null(pattern) && pattern!="")  {
    listTables = listTables[ grep(pattern, listTables, fixed=fixed) ]
  }
  return(listTables)
}

#' @title ms.printSchemas
#' @export
ms.printSchemas <- function(ch, showNumTables=TRUE) {
  listSchemas = as.character(ms.showSchemas(ch)[[1]])
  listSchemas = sort(unique(listSchemas))
  if (showNumTables) {
    for (i in 1:length(listSchemas)) {
      schema = listSchemas[i]
      DBI::dbSendQuery(ch, paste0("USE ", schema))
      res = DBI::dbSendQuery(ch, "SHOW TABLES")
      df <- DBI::dbFetch(res, n=-1)
      DBI::dbClearResult(res)
      message(paste0(schema, paste0(rep(" ", max(1, 21-nchar(schema))),collapse=" "), "\t(",nrow(df)," tables)"))
    }
  } else {
    multiplelines.message(listSchemas)
  }
  message(paste0("(",length(listSchemas), " schemas)"))
  invisible(listSchemas)
}

#' @title ms.printTables
#' @export
ms.printTables <- function(ch, pattern="", fixed=TRUE) {
  listTables = ms.listTables(ch)
  ntotal = length(listTables)
  if (!missing(pattern) && !is.null(pattern) && pattern!="")  {
    listTables = listTables[ grep(pattern, listTables, fixed=fixed) ]
  }
  npart = length(listTables)
  multiplelines.message(listTables)
  message(paste0("(",npart, " of ",ntotal," tables)"))
  invisible(listTables)
}

#' @title ms.colnames
#' @export
ms.colnames <- function (ch, strTable) {
  if (use_log) multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  if (use_log) multiplelines.message(paste0("[Query Input]:\n \tDBI::dbListFields \n"))
  timer = proc.time()
  if (attr(class(ch), "package") == "RJDBC") {
    suppressWarnings({
    res <- DBI::dbSendQuery(ch, paste0("select * from ",strTable," limit 0"))
    })
    df <- DBI::dbFetch(res, n=-1)
    DBI::dbClearResult(res)
    res <- colnames(df)
  } else {
    res = DBI::dbListFields(ch, strTable)
  }
  timer = round(proc.time() - timer)
  if (use_log) message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  if (use_log) message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  return (res)
}

#' @title ms.Table.Exists
#' @export
ms.Table.Exists <- function (ch, strTable) {
  test <- try( ms.Query(ch, paste0("select * from ",strTable," limit 0")), silent = TRUE )
  if( 'try-error' %in% class(test) ) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' @title ms.Table.Info
#' @export
ms.Table.Info <- function(ch, strTable) {
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
    TABLE_SCHEMA = rep(strTableSchema, nvar),
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
    TABLE_SCHEMA = character(),
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
