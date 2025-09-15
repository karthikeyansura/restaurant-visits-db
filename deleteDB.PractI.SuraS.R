# Title: Part D / Delete Database
# Course Name: CS5200 Database Management Systems
# Author: Sai Karthikeyan, Sura
# Semester: Spring 2025

# Function to install packages on demand
installPackagesOnDemand <- function(packages) {
  # Install packages that are not yet installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

# Function to load required packages
loadRequiredPackages <- function(packages) {
  # Load required packages
  for (package in packages) {
    suppressMessages({
      library(package, character.only = TRUE)
    })
  }
}

# Function to connect and check the connection to the database
connectAndCheckDatabase <- function() {
  dbName <- "DB_Name"
  dbUser <- "DB_USER"
  dbPassword <- "DB_PASSWORD"
  dbHost <- "DB_Host"
  dbPort <- "DB_PORT"
  
  # Try to connect and handle errors
  con <- tryCatch(
    {
      dbConnect(
        RMySQL::MySQL(),
        user = dbUser,
        password = dbPassword,
        dbname = dbName,
        host = dbHost,
        port = dbPort
      )
    },
    error = function(e) {
      return(e$message)
    }
  )
  
  return(con)
}

# Function to check if a table exists in the database
tableExists <- function(con, tableName) {
  query <- paste0("SHOW TABLES LIKE '", tableName, "';")
  result <- dbGetQuery(con, query)
  return(nrow(result) > 0)
}

# Function to drop all tables from the database
dropAllTables <- function(con) {
  tables_to_drop <- c(
    "Bill",
    "Visit",
    "Server",
    "Customer",
    "Restaurant",
    "MealType",
    "PaymentMethod"
  )
  
  # Drop each table one by one in the specified order
  errors <- character()
  for (table in tables_to_drop) {
    # Check if the table exists
    if (!tableExists(con, table)) {
      error_msg <- paste("Error: Table '", table, "' does not exist in the database.", sep = "")
      errors <- c(errors, error_msg)
    } else {
      dropQuery <- paste("DROP TABLE", table, ";")
      tryCatch(
        {
          dbExecute(con, dropQuery)
          print(paste("Dropped table:", table))
        },
        error = function(e) {
          errors <- c(errors, paste("Error dropping table '", table, "': ", e$message, sep = ""))
        }
      )
    }
  }
  
  # Return collected errors if any
  if (length(errors) > 0) {
    return(errors)
  }
  return(invisible(NULL))
}

# Main Function
main <- function() {
  # List of required packages
  required_packages <- c("RMySQL", "DBI")
  
  # Install and load the required packages
  installPackagesOnDemand(required_packages)
  loadRequiredPackages(required_packages)
  
  # Aiven only allows for 16 active connections. So to preserve them, disconnect any active connections prior to connecting
  all_cons <- dbListConnections(RMySQL::MySQL())
  for (con in all_cons) {
    dbDisconnect(con)
  }
  
  # Establish and check connection to the database
  con <- connectAndCheckDatabase()
  if (is.character(con)) {
    # If 'con' is a string (error message), print it
    print(con)
  } else {
    print("Connection to MySQL database established successfully. Dropping all tables...")
    
    # Drop the tables
    dropErrors <- dropAllTables(con)
    if (!is.null(dropErrors) && length(dropErrors) > 0) {
      for (error in dropErrors) {
        print(error)
      }
    }
    
    # Disconnect from the database
    dbDisconnect(con)
    print("Disconnected from the database")
  }
}

main()