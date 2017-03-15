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