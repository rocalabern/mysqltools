# roxygen2::roxygenise()

as.data.table.output = TRUE

numericTypes <- c("tinyint", "int",
                  "integer", "numeric")
categoricalTypes <- c("text", "varchar", "varchar(255)", "datetime", "date", 
                      "logical", "factor", "character")

setVar <- function (var, value) {
  strValue = paste(capture.output(dump("value", file="")), collapse = "")
  if (substring(strValue, 1, 9)=="value <- ") {
    strValue = substring(strValue, 10)
  } else if (substring(strValue, 1, 8)=="value<- ") {
    strValue = substring(strValue, 9)
  } else if (substring(strValue, 1, 8)=="value <-") {
    strValue = substring(strValue, 9)
  } else if (substring(strValue, 1, 7)=="value<-") {
    strValue = substring(strValue, 8)
  } else if (substring(strValue, 1, 8)=="value = ") {
    strValue = substring(strValue, 9)
  } else if (substring(strValue, 1, 7)=="value= ") {
    strValue = substring(strValue, 8)
  } else if (substring(strValue, 1, 7)=="value =") {
    strValue = substring(strValue, 8)
  } else if (substring(strValue, 1, 6)=="value=") {
    strValue = substring(strValue, 7)
  }
  unlockBinding(var, env = asNamespace('mysqltools'))
  eval(parse(text=paste0(var," <- ",strValue)), envir = asNamespace('mysqltools'))
  lockBinding(var, env = asNamespace('mysqltools'))
}

#' @title ms.setParamAsDataTableOutput
#' @export
ms.setParamAsDataTableOutput <- function (bool) {
  setVar("as.data.table.output", bool)
}