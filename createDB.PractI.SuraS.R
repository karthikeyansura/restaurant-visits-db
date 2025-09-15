# Title: Part C / Realize Database
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

# Function to check if a table exists in the database
checkTableExists <- function(dbCon, tableName) {
  query <- paste("SHOW TABLES LIKE '", tableName, "'", sep = "")
  result <- dbGetQuery(dbCon, query)
  return(nrow(result) > 0)
}

# Function to create 'Visit' table
createVisitTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "Visit")) {
    sqlQueryToCreateVisitTable <- "
    CREATE TABLE Visit (
      VisitID INTEGER PRIMARY KEY AUTO_INCREMENT,
      RestaurantID INTEGER NOT NULL,
      ServerEmpID INTEGER NOT NULL,
      VisitDate DATE NOT NULL,
      VisitTime TIME,
      MealTypeID INTEGER NOT NULL,
      PartySize INTEGER,
      Genders TEXT,
      WaitTime INTEGER NOT NULL,
      CustomerID INTEGER NOT NULL,
      FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
      FOREIGN KEY (RestaurantID) REFERENCES Restaurant(RestaurantID),
      FOREIGN KEY (MealTypeID) REFERENCES MealType(MealTypeID),
      FOREIGN KEY (ServerEmpID) REFERENCES Server(ServerEmpID)
    );
    "
    dbExecute(dbCon, sqlQueryToCreateVisitTable)
    print("Visit table created")
  } else {
    print("Visit table already exists")
  }
}

# Function to create 'Customer' table
createCustomerTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "Customer")) {
    sqlQueryToCreateCustomerTable <- "
    CREATE TABLE Customer (
      CustomerID INTEGER PRIMARY KEY AUTO_INCREMENT,
      CustomerName TEXT NOT NULL,
      CustomerPhone TEXT,
      CustomerEmail TEXT,
      LoyaltyMember BOOLEAN NOT NULL
    );
    "
    dbExecute(dbCon, sqlQueryToCreateCustomerTable)
    print("Customer table created")
  } else {
    print("Customer table already exists")
  }
}

# Function to create 'Restaurant' table
createRestaurantTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "Restaurant")) {
    sqlQueryToCreateRestaurantTable <- "
    CREATE TABLE Restaurant (
      RestaurantID INTEGER PRIMARY KEY AUTO_INCREMENT,
      RestaurantName TEXT NOT NULL
    );
    "
    dbExecute(dbCon, sqlQueryToCreateRestaurantTable)
    print("Restaurant table created")
  } else {
    print("Restaurant table already exists")
  }
}

# Function to create 'MealType' table
createMealTypeTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "MealType")) {
    sqlQueryToCreateMealTypeTable <- "
    CREATE TABLE MealType (
      MealTypeID INTEGER PRIMARY KEY AUTO_INCREMENT,
      MealType TEXT NOT NULL
    );
    "
    dbExecute(dbCon, sqlQueryToCreateMealTypeTable)
    print("MealType table created")
  } else {
    print("MealType table already exists")
  }
}

# Function to create 'Server' table
createServerTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "Server")) {
    sqlQueryToCreateServerTable <- "
    CREATE TABLE Server (
      ServerEmpID INTEGER PRIMARY KEY,
      ServerName TEXT NOT NULL,
      StartDateHired DATE,
      EndDateHired DATE,
      HourlyRate NUMERIC(20,10),
      ServerBirthDate DATE,
      ServerTIN TEXT
    );
    "
    dbExecute(dbCon, sqlQueryToCreateServerTable)
    print("Server table created")
  } else {
    print("Server table already exists")
  }
}

# Function to create 'PaymentMethod' table
createPaymentMethodTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "PaymentMethod")) {
    sqlQueryToCreatePaymentMethodTable <- "
    CREATE TABLE PaymentMethod (
      PaymentID INTEGER PRIMARY KEY AUTO_INCREMENT,
      PaymentMethod TEXT NOT NULL
    );
    "
    dbExecute(dbCon, sqlQueryToCreatePaymentMethodTable)
    print("PaymentMethod table created")
  } else {
    print("PaymentMethod table already exists")
  }
}

# Function to create 'Bill' table
createBillTable <- function(dbCon) {
  if (!checkTableExists(dbCon, "Bill")) {
    sqlQueryToCreateBillTable <- "
    CREATE TABLE Bill (
      BillID INTEGER PRIMARY KEY AUTO_INCREMENT,
      VisitID INTEGER NOT NULL,
      FoodBill NUMERIC(20,10) NOT NULL,
      TipAmount NUMERIC(20,10) NOT NULL,
      DiscountApplied NUMERIC(20,10) NOT NULL,
      PaymentID INTEGER NOT NULL,
      orderedAlcohol BOOLEAN NOT NULL,
      AlcoholBill NUMERIC(20,10) NOT NULL,
      FOREIGN KEY (VisitID) REFERENCES Visit(VisitID),
      FOREIGN KEY (PaymentID) REFERENCES PaymentMethod(PaymentID)
    );
    "
    dbExecute(dbCon, sqlQueryToCreateBillTable)
    print("Bill table created")
  } else {
    print("Bill table already exists")
  }
}

# Function to create all tables in the MySQL database
createTables <- function(dbCon) {
  createPaymentMethodTable(dbCon)
  createCustomerTable(dbCon)
  createRestaurantTable(dbCon)
  createMealTypeTable(dbCon)
  createServerTable(dbCon)
  createVisitTable(dbCon)
  createBillTable(dbCon)
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
    print("Connection to MySQL database established successfully. Creating database schema...")
    
    # Create the tables
    createTables(con)
    
    # Disconnect from the database
    dbDisconnect(con)
    print("Disconnected from the database")
  }
}

main()