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
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n \tSHOW DATABASES \n"))
  timer = proc.time()
  res = DBI::dbSendQuery(ch, "SHOW DATABASES")
  df <- DBI::dbFetch(res)
  DBI::dbClearResult(res)
  timer = round(proc.time() - timer)
  message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  df = data.frame(TABLE_SCHEM=df[[1]], stringsAsFactors = FALSE)
  return (df)
}

#' @title ms.showTables
#' @export
ms.showTables <- function (
  ch
) {
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n \tDBI::dbListTables \n"))
  timer = proc.time()
  df = DBI::dbListTables(ch)
  timer = round(proc.time() - timer)
  message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  df = data.frame(TABLE_NAME=df, stringsAsFactors = FALSE)
  return (df)
}

#' @title ms.listTables
#' @export
ms.listTables <- function (ch, pattern="", fixed=TRUE) {
  df = ms.showTables(ch)
  listTables = as.character(df$TABLE_NAME)
  if (!missing(pattern) && !is.null(pattern) && pattern!="")  {
    listTables = listTables[ grep(pattern, listTables, fixed=fixed) ]
  }
  return(listTables)
}

#' @title ms.printSchemas
#' @export
ms.printSchemas <- function(ch, showNumTables=TRUE) {
  listSchemas = as.character(ms.showSchemas(ch)$TABLE_SCHEM)
  if (showNumTables) {
    tableSchemas = table(listSchemas)
    for (i in 1:length(tableSchemas)) {
      message(paste0(names(tableSchemas)[i], paste0(rep(" ", max(1, 21-nchar(names(tableSchemas)[i]))),collapse=" "), "\t(",tableSchemas[i]," tables)"))
    }
    listSchemas = sort(unique(listSchemas))
  } else {
    listSchemas = sort(unique(listSchemas))
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

#' @title ms.ExistTable
#' @export
ms.ExistTable <- function (ch, strTable) {
  test <- try( ms.Query(ch, paste0("select * from ",strTable," limit 1")), silent = TRUE )
  if( 'try-error' %in% class(test) ) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' @title ms.colnames
#' @export
ms.colnames <- function (ch, strTable) {
  multiplelines.message(paste0("[Query Time]: ",format(Sys.time(), "%Y%m%d_%H_%M_%S"),"\n"))
  multiplelines.message(paste0("[Query Input]:\n \tDBI::dbListFields \n"))
  timer = proc.time()
  res = DBI::dbListFields(ch, strTable)
  timer = round(proc.time() - timer)
  message(paste0("[Query Output] Ok: 0 rows returned.\n"))
  message(paste0("[Query Execution Time: ",timer[3]," seconds.]\n"))
  
  return (res)
}
