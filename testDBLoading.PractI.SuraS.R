# Title: Part F / Test Data Loading Process
# Course Name: CS5200 Database Management Systems
# Author: Sai Karthikeyan, Sura
# Semester: Spring 2025

# Function to install packages on demand
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

# Function to load required packages
loadRequiredPackages <- function(packages) {
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

# Function to perform the tests
testDataLoading <- function(con, csv_data) {
  # Test 1: Count unique restaurants
  csv_unique_restaurants <- length(unique(csv_data$Restaurant))
  db_restaurant_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM Restaurant")[1, "count"]
  cat("Test 1: Number of unique restaurants\n")
  cat("  CSV:", csv_unique_restaurants, "\n")
  cat("  DB:", db_restaurant_count, "\n")
  if (csv_unique_restaurants == db_restaurant_count) {
    cat("  Result: PASS - Number of unique restaurants matches.\n")
  } else {
    cat("  Result: FAIL - Number of unique restaurants does not match.\n")
  }
  
  # Test 2: Count unique customers
  csv_unique_customers <- length(unique(csv_data$CustomerName[!is.na(csv_data$CustomerName) & csv_data$CustomerName != ""]))
  # CustomerID '0' is a default row to represent inconsistent data so not considering it
  db_customer_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM Customer WHERE CustomerID != 0")[1, "count"]
  cat("Test 2: Number of unique customers\n")
  cat("  CSV:", csv_unique_customers, "\n")
  cat("  DB:", db_customer_count, "\n")
  if (csv_unique_customers == db_customer_count) {
    cat("  Result: PASS - Number of unique customers matches.\n")
  } else {
    cat("  Result: FAIL - Number of unique customers does not match.\n")
  }
  
  # Test 3: Count unique servers
  csv_unique_servers <- length(unique(csv_data$ServerEmpID[!is.na(csv_data$ServerEmpID) & csv_data$ServerEmpID != ""]))
  # ServerEmpID '0' is a default row to represent inconsistent data so not considering it
  db_server_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM Server WHERE ServerEmpID != 0")[1, "count"]
  cat("Test 3: Number of unique servers\n")
  cat("  CSV:", csv_unique_servers, "\n")
  cat("  DB:", db_server_count, "\n")
  if (csv_unique_servers == db_server_count) {
    cat("  Result: PASS - Number of unique servers matches.\n")
  } else {
    cat("  Result: FAIL - Number of unique servers does not match.\n")
  }
  
  # Test 4: Count visits
  csv_visit_count <- nrow(csv_data)
  db_visit_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM Visit")[1, "count"]
  cat("Test 4: Number of visits\n")
  cat("  CSV:", csv_visit_count, "\n")
  cat("  DB:", db_visit_count, "\n")
  if (csv_visit_count == db_visit_count) {
    cat("  Result: PASS - Number of unique visits matches.\n")
  } else {
    cat("  Result: FAIL - Number of unique visits does not match.\n")
  }
  
  # Test 5: Sum of total amount spent on food, alcohol, and tips
  csv_food_sum <- round(sum(csv_data$FoodBill, na.rm = TRUE), 10)
  csv_alcohol_sum <- round(sum(csv_data$AlcoholBill, na.rm = TRUE), 10)
  csv_tip_sum <- round(sum(csv_data$TipAmount, na.rm = TRUE), 10)
  csv_total_sum <- round(csv_food_sum + csv_alcohol_sum + csv_tip_sum, 4)
  db_bill_sums <- dbGetQuery(con, "
    SELECT 
      SUM(FoodBill) as food_sum,
      SUM(AlcoholBill) as alcohol_sum,
      SUM(TipAmount) as tip_sum
    FROM Bill")
  db_food_sum <- round(db_bill_sums[1, "food_sum"], 10)
  db_alcohol_sum <- round(db_bill_sums[1, "alcohol_sum"], 10)
  db_tip_sum <- round(db_bill_sums[1, "tip_sum"], 10)
  db_total_sum <- round(db_food_sum + db_alcohol_sum + db_tip_sum, 4)
  cat("Test 5: Total amount spent (Food + Alcohol + Tips)\n")
  cat("  CSV Food Sum:", format(csv_food_sum, nsmall = 10), "\n")
  cat("  DB Food Sum:", format(db_food_sum, nsmall = 10), "\n")
  cat("  CSV Alcohol Sum:", format(csv_alcohol_sum, nsmall = 10), "\n")
  cat("  DB Alcohol Sum:", format(db_alcohol_sum, nsmall = 10), "\n")
  cat("  CSV Tip Sum:", format(csv_tip_sum, nsmall = 10), "\n")
  cat("  DB Tip Sum:", format(db_tip_sum, nsmall = 10), "\n")
  cat("  CSV Total Sum:", format(csv_total_sum, nsmall = 10), "\n")
  cat("  DB Total Sum:", format(db_total_sum, nsmall = 10), "\n")
  if (csv_food_sum == db_food_sum &&
      csv_alcohol_sum == db_alcohol_sum &&
      csv_tip_sum == db_tip_sum &&
      csv_total_sum == db_total_sum) {
    cat("  Result: PASS - Total amounts spent matches.\n")
  } else {
    cat("  Result: FAIL - Total amounts spent do not matches.\n")
  }
}

# Main function
main <- function() {
  required_packages <- c("RMySQL", "DBI")
  installPackagesOnDemand(required_packages)
  loadRequiredPackages(required_packages)
  
  all_cons <- dbListConnections(RMySQL::MySQL())
  for (con in all_cons) {
    dbDisconnect(con)
  }
  
  con <- connectAndCheckDatabase()
  if (is.character(con)) {
    print(con)
  } else {
    print("Connection to MySQL database established successfully. Testing data loading...")
    
    csv_data <- read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv", 
                         header = TRUE, stringsAsFactors = FALSE)
    
    # Test data loading
    testDataLoading(con, csv_data)
    
    # Disconnect from the database
    dbDisconnect(con)
    print("Testing complete. Disconnected from the database.")
  }
}

# Call the main function to execute the script
main()