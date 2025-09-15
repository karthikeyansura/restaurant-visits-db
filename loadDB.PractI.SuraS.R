# Title: Part E / Populate Database
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

# Function to insert data into database tables
insertData <- function(con, csv_data) {
  # Start a transaction
  dbBegin(con)
  
  # Insert into PaymentMethod
  payment_df <- data.frame(PaymentMethod = unique(csv_data$PaymentMethod))
  payment_values <- paste(sprintf("('%s')", payment_df$PaymentMethod), collapse = ", ")
  dbExecute(con, paste("INSERT INTO PaymentMethod (PaymentMethod) VALUES", payment_values))
  cat("Inserted into PaymentMethod\n")
  
  # Insert into Restaurant
  restaurant_df <- data.frame(RestaurantName = unique(csv_data$Restaurant))
  restaurant_values <- paste(sprintf("('%s')", restaurant_df$RestaurantName), collapse = ", ")
  dbExecute(con, paste("INSERT INTO Restaurant (RestaurantName) VALUES", restaurant_values))
  cat("Inserted into Restaurant\n")
  
  # Insert into MealType
  meal_type_df <- data.frame(MealType = unique(csv_data$MealType))
  meal_type_values <- paste(sprintf("('%s')", meal_type_df$MealType), collapse = ", ")
  dbExecute(con, paste("INSERT INTO MealType (MealType) VALUES", meal_type_values))
  cat("Inserted into MealType\n")
  
  # Insert into Server
  # Default Server row for representing inconsistent values of Server details
  dbExecute(con, "INSERT INTO Server (ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN) 
                  VALUES ('0', 'Unknown Server', NULL, NULL, '0.00', NULL, NULL)")
  # Unique Server details
  unique_servers <- csv_data[!duplicated(csv_data$ServerEmpID) & !is.na(csv_data$ServerEmpID) & csv_data$ServerEmpID != "", ]
  server_values <- paste0(
    "(",
    sprintf("'%s'", unique_servers$ServerEmpID), ", ",
    sprintf("'%s'", unique_servers$ServerName), ", ",
    sprintf("'%s'", format(as.Date(unique_servers$StartDateHired, "%Y-%m-%d"), "%Y-%m-%d")), ", ",
    ifelse(is.na(unique_servers$EndDateHired) | unique_servers$EndDateHired == "", "NULL", sprintf("'%s'", format(as.Date(unique_servers$EndDateHired, "%Y-%m-%d"), "%Y-%m-%d"))), ", ",
    unique_servers$HourlyRate, ", ",
    ifelse(is.na(unique_servers$ServerBirthDate) | unique_servers$ServerBirthDate == "", "NULL", sprintf("'%s'", format(as.Date(unique_servers$ServerBirthDate, "%m/%d/%Y"), "%Y-%m-%d"))), ", ",
    ifelse(is.na(unique_servers$ServerTIN) | unique_servers$ServerTIN == "", "NULL", sprintf("'%s'", unique_servers$ServerTIN)),
    ")"
  )
  server_values_str <- paste(server_values, collapse = ", ")
  dbExecute(con, paste("INSERT INTO Server (ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN) VALUES", server_values_str))
  cat("Inserted into Server\n")
  
  # Insert into Customer
  # Default Customer row for representing inconsistent values of Customer details
  dbExecute(con, "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO';")
  dbExecute(con, "INSERT INTO Customer (CustomerID, CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) 
                  VALUES (0, 'Unknown Customer', NULL, NULL, FALSE)")
  dbExecute(con, "SET SESSION sql_mode = '';")
  # Unique Customer details
  unique_customers <- csv_data[!duplicated(csv_data[, c("CustomerName", "CustomerPhone", "CustomerEmail")]) &
                                 !(is.na(csv_data$CustomerName) & is.na(csv_data$CustomerPhone) & is.na(csv_data$CustomerEmail)) &
                                 !(csv_data$CustomerName == "" & csv_data$CustomerPhone == "" & csv_data$CustomerEmail == ""), ]
  customer_values <- paste0(
    "(",
    sprintf("'%s'", unique_customers$CustomerName), ", ",
    sprintf("'%s'", unique_customers$CustomerPhone), ", ",
    sprintf("'%s'", unique_customers$CustomerEmail), ", ",
    ifelse(unique_customers$LoyaltyMember == "TRUE", "TRUE", "FALSE"),
    ")"
  )
  customer_values_str <- paste(customer_values, collapse = ", ")
  dbExecute(con, paste("INSERT INTO Customer (CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) VALUES", customer_values_str))
  cat("Inserted into Customer\n")
  
  # Pre-fetch Foreign Keys
  customers <- dbGetQuery(con, "SELECT CustomerID, CustomerName, CustomerPhone, CustomerEmail FROM Customer")
  customers$key <- paste(ifelse(is.na(customers$CustomerName) | customers$CustomerName == "", "Unknown Customer", customers$CustomerName),
                         ifelse(is.na(customers$CustomerPhone) | customers$CustomerPhone == "", "Unknown", customers$CustomerPhone),
                         ifelse(is.na(customers$CustomerEmail) | customers$CustomerEmail == "", "Unknown", customers$CustomerEmail), sep = "|")
  restaurants <- dbGetQuery(con, "SELECT RestaurantID, RestaurantName FROM Restaurant")
  meal_types <- dbGetQuery(con, "SELECT MealTypeID, MealType FROM MealType")
  payment_methods <- dbGetQuery(con, "SELECT PaymentID, PaymentMethod FROM PaymentMethod")
  
  # Map Foreign Keys to Visit Data
  csv_data$customer_key <- paste(ifelse(is.na(csv_data$CustomerName) | csv_data$CustomerName == "", "Unknown Customer", csv_data$CustomerName),
                                 ifelse(is.na(csv_data$CustomerPhone) | csv_data$CustomerPhone == "", "Unknown", csv_data$CustomerPhone),
                                 ifelse(is.na(csv_data$CustomerEmail) | csv_data$CustomerEmail == "", "Unknown", csv_data$CustomerEmail), sep = "|")
  csv_data$CustomerID <- customers$CustomerID[match(csv_data$customer_key, customers$key)]
  csv_data$RestaurantID <- restaurants$RestaurantID[match(csv_data$Restaurant, restaurants$RestaurantName)]
  csv_data$MealTypeID <- meal_types$MealTypeID[match(csv_data$MealType, meal_types$MealType)]
  csv_data$PaymentID <- payment_methods$PaymentID[match(csv_data$PaymentMethod, payment_methods$PaymentMethod)]
  
  # Insert into Visit in Batches
  batch_size <- 1000
  num_batches <- ceiling(nrow(csv_data) / batch_size)
  for (batch in 1:num_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(csv_data))
    batch_data <- csv_data[start_idx:end_idx, ]
    
    values <- paste0(
      "(",
      batch_data$VisitID, ", '",
      batch_data$VisitDate, "', ",
      ifelse(is.na(batch_data$VisitTime) | batch_data$VisitTime == "", "NULL", paste0("'", batch_data$VisitTime, "'")), ", ",
      batch_data$CustomerID, ", ",
      batch_data$MealTypeID, ", ",
      batch_data$RestaurantID, ", ",
      ifelse(batch_data$PartySize == 99, "NULL", batch_data$PartySize), ", '",
      batch_data$Genders, "', ",
      pmax(batch_data$WaitTime, 0), ", '",
      ifelse(is.na(batch_data$ServerEmpID) | batch_data$ServerEmpID == "", "0", batch_data$ServerEmpID), "')"
    )
    values_str <- paste(values, collapse = ", ")
    query <- paste("INSERT INTO Visit (VisitID, VisitDate, VisitTime, CustomerID, MealTypeID, RestaurantID, PartySize, Genders, WaitTime, ServerEmpID) VALUES", values_str)
    dbExecute(con, query)
  }
  cat("Inserted into Visit\n")
  
  # Insert into Bill in Batches
  for (batch in 1:num_batches) {
    start_idx <- (batch - 1) * batch_size + 1
    end_idx <- min(batch * batch_size, nrow(csv_data))
    batch_data <- csv_data[start_idx:end_idx, ]
    values <- paste0(
      "(",
      batch_data$VisitID, ", ",
      batch_data$FoodBill, ", ",
      batch_data$TipAmount, ", ",
      batch_data$DiscountApplied, ", ",
      batch_data$PaymentID, ", ",
      ifelse(tolower(batch_data$orderedAlcohol) == "yes", "TRUE", "FALSE"), ", ",
      batch_data$AlcoholBill, ")"
    )
    values_str <- paste(values, collapse = ", ")
    query <- paste("INSERT INTO Bill (VisitID, FoodBill, TipAmount, DiscountApplied, PaymentID, orderedAlcohol, AlcoholBill) VALUES", values_str)
    dbExecute(con, query)
  }
  cat("Inserted into Bill\n")
  
  # Commit the transaction
  dbCommit(con)
}

# Main function
main <- function() {
  # List of required packages
  required_packages <- c("RMySQL", "DBI")
  
  # Install and load the required packages
  installPackagesOnDemand(required_packages)
  loadRequiredPackages(required_packages)
  
  # Establish and check connection to the database
  con <- connectAndCheckDatabase()
  if (is.character(con)) {
    # If 'con' is a string (error message), print it
    print(con)
  } else {
    print("Connection to MySQL database established successfully. Inserting data...")
    
    # Read CSV data
    df.orig <- read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv", 
                        header = TRUE, stringsAsFactors = FALSE)
    
    # Insert data
    insertData(con, df.orig)
    
    # Disconnect from the database
    dbDisconnect(con)
    print("Data insertion complete. Disconnected from the database.")
  }
}

main()