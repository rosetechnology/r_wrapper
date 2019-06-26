pkg.env <- new.env(parent = emptyenv())
pkg.env$loggedin <- FALSE
pkg.env$base_url <- "https://rose.ai"

#' A Rose Login Function
#' 
#' Login Rose given an email and password
#' @param username Email of the user
#' @param password Password of the user
#' @import httr
#' @import XML
#' @export

rose_login <- function(username,password){
  login_url <- paste(pkg.env$base_url,'/users/auth',sep = "")
  
  pars <- list(
    username = username,
    password = password
  )
  
  r <- POST(login_url, body = pars,encode = "json")
  
  parsedHtml <- htmlParse(content(r, as = "text", encoding = "UTF-8"), asText = TRUE)
  if (r$status_code == 200){
    pkg.env$loggedin <- TRUE
    message("successfully logged in")}
  
  stop_for_status(r)
}

#' A data extraction Function
#' 
#' Pulls the dataset object given a code
#' @param code Code to pull from ROSE
#' @param as_dataframe Boolean to determine return object type
#' @param exact_match Boolean to determine code match type in Rose
#' @import httr
#' @import XML
#' @import jsonlite
#' @examples 
#' pull("chn.agriculture.pgdp.wdi")
#' @export

rose_pull <- function(code, as_dataframe=TRUE, exact_match=TRUE){
  if (pkg.env$loggedin==FALSE){
    stop("User not logged in")
  }
  get_url = paste(pkg.env$base_url,"/objects/",code,"?exact_match=",toString(ifelse(exact_match==TRUE,1,0)),sep = "")
  r = GET(get_url)
  stop_for_status(r)
  
  outs <- content(r, as = "text",encoding = "UTF-8")
  data = fromJSON(outs)
  
  if (as_dataframe==TRUE){
    return(json_to_df(data))
  } else {
    return(data)
  }
}


#' A function to push data to Rose
#' 
#' Creates/updates a dataset in Rose
#' @param code Code of the dataset
#' @param metas Metas dataframe with index as tag names and a single column for tag values
#' @param values Values dataframe with index as dates in 'yyyy-mm-dd' format and a single column for values for each timestamp
#' @param data_type type of the data, "timeseries" or "map" 
#' @import httr
#' @import XML
#' @import jsonlite
#' @export

rose_push <- function(code=NULL, metas=NULL, values=NULL, overwrite=FALSE, data_type = "timeseries"){
  if (pkg.env$loggedin==FALSE){
    stop("User not logged in")
  }

  values <- na.omit(values) # Remove NAs
  
  # convert metadata dataframe to list
  metaslist <- data.frame(lapply(metas, as.character), stringsAsFactors=FALSE)
  metaslist <- as.list(metaslist[,1])
  names(metaslist) <- row.names(metas)
  
  if (data_type == "timeseries"){
    # convert values dataframe to list
    valueslist <- data.frame(lapply(values, as.character), stringsAsFactors=FALSE)
    valueslist <- as.list(valueslist[,1])
    names(valueslist) <- row.names(values)
  } else if (data_type=="map"){
    valueslist <- data.frame(lapply(values, as.character), stringsAsFactors=FALSE)
  } else {
    stop("Datatype not supported")
  }
  
  dataset_list <- list(code=code, metas= metaslist, values= valueslist)
  
  r <- POST(paste(pkg.env$base_url, "/data", sep = ""), body = dataset_list, encode = "json", add_headers("SNOW-OVERWRITE"=toString(as.numeric(overwrite))))
  stop_for_status(r)
}


#' A logic extraction Function
#' 
#' Pulls the logic given a code
#' @param code Code to pull from ROSE
#' @import httr
#' @import XML
#' @import jsonlite
#' @examples 
#' pull_logic(rosecode)
#' @export

rose_pull_logic <- function(code){
  if (pkg.env$loggedin==FALSE){
    stop("User not logged in")
  }
  get_url = paste(pkg.env$base_url,"/logic/",code,sep = "")
  r = GET(get_url)
  stop_for_status(r)
  
  outs <- content(r, as = "text",encoding = "UTF-8")
  data = fromJSON(outs)
  return(data$logic)
}

#' Push logic function
#' 
#' Push the logic given a code
#' @param code Code to push to ROSE
#' @param logic logic for this code
#' @import httr
#' @examples 
#' rose_push_logic(rosecode, logic)
#' @export

rose_push_logic <- function(code, logic){
  if (pkg.env$loggedin==FALSE){
    stop("User not logged in")
  }

  r = POST(paste(pkg.env$base_url,"/logic", sep = ""), body = list('code' = code, 'logic' = logic), encode = "json")
  stop_for_status(r)
}

#' A data conversion Function
#' 
#' Convert json dataset to dataframes
#' @param dataset_json dataset in json to be converted
#' @keywords internal
#' @export

json_to_df <- function(dataset_json){
  dataset_df = dataset_json
  dataset_df[['metas']] <- NULL
  dataset_df[['values']] <- NULL
  dataset_df <- data.frame("value" = unlist(dataset_df))
  
  if (dataset_json$type=="timeseries"){
    values_df <- data.frame("value" = unlist(dataset_json$values))
    metas_df <- data.frame("value" = unlist(dataset_json$metas))
  } else if (dataset_json$type=="map"){
    values_df <- data.frame(dataset_json$values,stringsAsFactors = FALSE)
    metas_df <- data.frame("value" = t(data.frame(dataset_json$metas)),stringsAsFactors = FALSE)
  } else {
    stop("Unknown data type")
  }
  return(list(dataset_df, metas_df, values_df))
}
