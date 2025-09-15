# Title: Part H / Add Business Logic
# Course Name: CS5200 Database Management Systems
# Author: Sura Sai Karthikeyan
# Semester: Spring 2025

options(warn = -1)

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

# Create StoreVisit Stored Procedure assuming that the server, customer, and restaurant already exist
createStoreVisitProcedure <- function(con) {
  # Drop the StoreVisit stored procedure if it exists
  dbSendQuery(con, "DROP PROCEDURE IF EXISTS StoreVisit;")
  
  # Create the StoreVisit stored procedure
  queryToCreateStoreVisitProcedure <- "
   CREATE PROCEDURE StoreVisit(
      IN p_RestaurantID INTEGER,
      IN p_CustomerID INTEGER,
      IN p_VisitDate DATE,
      IN p_VisitTime TIME,
      IN p_MealTypeID INTEGER,
      IN p_PartySize INTEGER,
      IN p_Genders TEXT,
      IN p_WaitTime INTEGER,
      IN p_FoodBill NUMERIC(20,10),
      IN p_AlcoholBill NUMERIC(20,10),
      IN p_TipAmount NUMERIC(20,10),
      IN p_DiscountApplied NUMERIC(20,10),
      IN p_OrderedAlcohol BOOLEAN,
      IN p_PaymentID INTEGER,
      IN p_ServerEmpID INTEGER
   )
   
   BEGIN
      DECLARE v_VisitID INTEGER;
      
      INSERT INTO Visit (RestaurantID, CustomerID, VisitDate, VisitTime, MealTypeID, PartySize, Genders, WaitTime, ServerEmpID)
      VALUES (p_RestaurantID, p_CustomerID, p_VisitDate, p_VisitTime, p_MealTypeID, p_PartySize, p_Genders, p_WaitTime, p_ServerEmpID);
      
      SET v_VisitID = LAST_INSERT_ID();
      
      INSERT INTO Bill (VisitID, FoodBill, TipAmount, DiscountApplied, OrderedAlcohol, PaymentID, AlcoholBill) 
      VALUES (v_VisitID, p_FoodBill, p_TipAmount, p_DiscountApplied, p_OrderedAlcohol, p_PaymentID, p_AlcoholBill);
   END
  "
  tryCatch({
    suppressWarnings(dbSendQuery(con, queryToCreateStoreVisitProcedure))
    cat("StoreVisit stored procedure created successfully.", "\n")
  }, error = function(e) {
    stop("Error creating storeVisit stored procedure:", e$message, "\n")
  })
}

# Create StoreNewVisit Stored Procedure assuming that the server, customer, and restaurant does not already exist
createStoreNewVisitProcedure <- function(con) {
  # drop the StoreNewVisit stored procedure if it exists
  dbSendQuery(con, "DROP PROCEDURE IF EXISTS StoreNewVisit;")
  
  # Create the StoreNewVisit stored procedure
  queryToCreateStoreNewVisitProcedure <- "
   CREATE PROCEDURE StoreNewVisit(
      IN p_RestaurantName TEXT,
      IN p_CustomerName TEXT,
      IN p_CustomerPhone TEXT,
      IN p_CustomerEmail TEXT,
      IN p_LoyaltyMember BOOLEAN,
      IN p_ServerName TEXT,
      IN p_VisitDate DATE,
      IN p_MealTypeID INTEGER,
      IN p_PartySize INTEGER,
      IN p_WaitTime INTEGER,
      IN p_FoodBill NUMERIC(20,10),
      IN p_AlcoholBill NUMERIC(20,10),
      IN p_TipAmount NUMERIC(20,10),
      IN p_DiscountApplied NUMERIC(20,10),
      IN p_OrderedAlcohol BOOLEAN,
      IN p_PaymentID INTEGER
   )
   BEGIN
      DECLARE v_RestaurantID INTEGER;
      DECLARE v_CustomerID INTEGER;
      DECLARE v_ServerEmpID INTEGER;
      DECLARE v_VisitID INTEGER;
      DECLARE v_BillingID INTEGER;
      
      SELECT RestaurantID INTO v_RestaurantID FROM Restaurant WHERE RestaurantName = p_RestaurantName;
      IF v_RestaurantID IS NULL THEN
          INSERT INTO Restaurant (RestaurantName) VALUES (p_RestaurantName);
          SET v_RestaurantID = LAST_INSERT_ID();
      END IF;
      
      SELECT CustomerID INTO v_CustomerID FROM Customer 
      WHERE CustomerName = p_CustomerName AND CustomerPhone = p_CustomerPhone AND CustomerEmail = p_CustomerEmail;
      IF v_CustomerID IS NULL THEN
          INSERT INTO Customer (CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) 
          VALUES (p_CustomerName, p_CustomerPhone, p_CustomerEmail, p_LoyaltyMember);
          SET v_CustomerID = LAST_INSERT_ID();
      END IF;
      
      SELECT ServerEmpID INTO v_ServerEmpID FROM Server WHERE ServerName = p_ServerName;
      IF v_ServerEmpID IS NULL THEN
          SELECT COALESCE(MAX(ServerEmpID), 0) + 1 INTO v_ServerEmpID FROM Server;
          INSERT INTO Server (ServerEmpID, ServerName) VALUES (v_ServerEmpId, p_ServerName);
      END IF;
      
      INSERT INTO Visit (RestaurantID, CustomerID, VisitDate, MealTypeID, PartySize, WaitTime, ServerEmpID)
      VALUES (v_RestaurantID, v_CustomerID, p_VisitDate, p_MealTypeID, p_PartySize, p_WaitTime, v_ServerEmpID);
      
      SET v_VisitID = LAST_INSERT_ID();

      INSERT INTO Bill (VisitID, FoodBill, TipAmount, DiscountApplied, orderedAlcohol, PaymentID, AlcoholBill) 
      VALUES (v_VisitID, p_FoodBill, p_TipAmount, p_DiscountApplied, p_OrderedAlcohol, p_PaymentID, p_AlcoholBill);
   END
  "
  tryCatch({
    suppressWarnings(dbSendQuery(con, queryToCreateStoreNewVisitProcedure))
    cat("StoreNewVisit stored procedure created successfully.", "\n")
  }, error = function(e) {
    stop("Error creating StoreNewVisit stored procedure:", e$message, "\n")
  })
}

# Trigger StoreVisit Stored Procedure
callStoreVisit <- function(con, RestaurantID, CustomerID, VisitDate, VisitTime, 
                           MealTypeID, PartySize, Genders, WaitTime, FoodBill, 
                           AlcoholBill, TipAmount, DiscountApplied,
                           OrderedAlcohol, PaymentID, ServerEmpID) {
  
  # Handle NULL for VisitTime if not provided
  visitTimeSql <- if (is.na(VisitTime)) "NULL" else paste0("'", VisitTime, "'")
  
  # trigger the storeVisit stored procedure
  queryStoreVisit <- paste0("CALL storeVisit(", RestaurantID, ", ", 
                            CustomerID, ", '", VisitDate, "', ",
                            visitTimeSql, ", ",
                            MealTypeID, ", ", PartySize, ", '", Genders, "', ", 
                            WaitTime, ", ", FoodBill, ", ",
                            AlcoholBill, ", ", TipAmount, ", ", 
                            DiscountApplied, ", ", OrderedAlcohol, ", ", 
                            PaymentID, ", ", ServerEmpID, ");")
  tryCatch({
    print("Query to trigger storeVisit stored procedure:")
    print(queryStoreVisit)
    suppressWarnings(dbSendQuery(con, queryStoreVisit))
    print("Visit successfully added to the database.")
  }, error = function(e) {
    stop(e$message, "\n")
  })
}

# Trigger StoreNewVisit Stored Procedure 
callStoreNewVisit <- function(con, RestaurantName, CustomerName, CustomerPhone, CustomerEmail, 
                              LoyaltyMember, ServerName, VisitDate, MealTypeID, PartySize, 
                              WaitTime, FoodBill, AlcoholBill, TipAmount, DiscountApplied, 
                              OrderedAlcohol, PaymentID) {
  # trigger the StoreNewVisit stored procedure
  queryStoreNewVisit <- paste0("CALL StoreNewVisit('", RestaurantName, "', '", 
                               CustomerName, "', '", CustomerPhone, "', '", 
                               CustomerEmail, "', ", LoyaltyMember, ", '", 
                               ServerName, "', '", 
                               VisitDate, "', ", MealTypeID, ", ", 
                               PartySize, ", ", WaitTime, ", ", 
                               FoodBill, ", ", AlcoholBill, ", ", 
                               TipAmount, ", ", DiscountApplied, ", ", 
                               OrderedAlcohol, ", ", PaymentID, ");")
  tryCatch({
    print("Query to trigger StoreNewVisit stored procedure:")
    print(queryStoreNewVisit)
    suppressWarnings(dbSendQuery(con, queryStoreNewVisit))
    print("New visit successfully added to the database.")
  }, error = function(e) {
    stop("Error in StoreNewVisit stored procedure - failed inserting new visit: ", e$message, "\n")
  })
}

# Verify Visit Insertion for StoreVisit
verifyVisitInsertionForStoreVisit <- function(con, VisitDetails) {
  queryToGetLastVisit <- "SELECT * FROM Visit ORDER BY VisitID DESC LIMIT 1;"
  result <- dbGetQuery(con, queryToGetLastVisit)
  print("Last inserted row in Visit table:")
  print(result)
  
  # Expected values
  expectedRestaurantID <- as.numeric(VisitDetails[1])
  expectedCustomerID <- as.numeric(VisitDetails[2])
  expectedVisitDate <- VisitDetails[3]
  expectedVisitTime <- VisitDetails[4]
  expectedMealTypeID <- as.numeric(VisitDetails[5])
  expectedPartySize <- as.numeric(VisitDetails[6])
  expectedGenders <- VisitDetails[7]
  expectedWaitTime <- as.numeric(VisitDetails[8])
  expectedServerEmpID <- as.numeric(VisitDetails[15])
  
  print("Testing inserted visit details:")
  
  print("Restaurant ID matches")
  test_that("Restaurant ID matches", {
    outputMsg <- paste("Expected:", expectedRestaurantID, "| DB:", result$RestaurantID)
    expect_equal(result$RestaurantID, expectedRestaurantID, info = outputMsg)
    print(outputMsg)
  })
  
  print("Customer ID matches")
  test_that("Customer ID matches", {
    outputMsg <- paste("Expected:", expectedCustomerID, "| DB:", result$CustomerID)
    expect_equal(result$CustomerID, expectedCustomerID, info = outputMsg)
    print(outputMsg)
  })
  
  print("Visit Date matches")
  test_that("Visit Date matches", {
    outputMsg <- paste("Expected:", expectedVisitDate, "| DB:", result$VisitDate)
    expect_equal(result$VisitDate, expectedVisitDate, info = outputMsg)
    print(outputMsg)
  })
  
  print("Visit Time matches")
  test_that("Visit Time matches", {
    outputMsg <- paste("Expected:", expectedVisitTime, "| DB:", result$VisitTime)
    expect_equal(result$VisitTime, expectedVisitTime, info = outputMsg)
    print(outputMsg)
  })
  
  print("Meal Type matches")
  test_that("Meal Type matches", {
    outputMsg <- paste("Expected:", expectedMealTypeID, "| DB:", result$MealTypeID)
    expect_equal(result$MealTypeID, expectedMealTypeID, info = outputMsg)
    print(outputMsg)
  })
  
  print("Party Size matches")
  test_that("Party Size matches", {
    outputMsg <- paste("Expected:", expectedPartySize, "| DB:", result$PartySize)
    expect_equal(result$PartySize, expectedPartySize, info = outputMsg)
    print(outputMsg)
  })
  
  print("Genders matches")
  test_that("Genders matches", {
    outputMsg <- paste("Expected:", expectedGenders, "| DB:", result$Genders)
    expect_equal(result$Genders, expectedGenders, info = outputMsg)
    print(outputMsg)
  })
  
  print("Wait Time matches")
  test_that("Wait Time matches", {
    outputMsg <- paste("Expected:", expectedWaitTime, "| DB:", result$WaitTime)
    expect_equal(result$WaitTime, expectedWaitTime, info = outputMsg)
    print(outputMsg)
  })
  
  print("Server EmpID matches")
  test_that("Server ID matches", {
    outputMsg <- paste("Expected:", expectedServerEmpID, "| DB:", result$ServerEmpID)
    expect_equal(result$ServerEmpID, expectedServerEmpID, info = outputMsg)
    print(outputMsg)
  })
  
}

# Verify Visit Insertion for StoreNewVisit
verifyStoreNewVisitInsertion <- function(con, expectedRestaurantName, expectedCustomerName, expectedCustomerPhone, expectedCustomerEmail, 
                                         expectedLoyaltyMember, expectedServerName, expectedServerEmpID, expectedVisitDate, expectedMealTypeID, expectedPartySize, 
                                         expectedWaitTime, expectedFoodBill, expectedAlcoholBill, expectedTipAmount, expectedDiscountApplied, 
                                         expectedOrderedAlcohol, expectedPaymentID) {
  queryToGetLastVisit <- "
  SELECT *
  FROM Visit v
  JOIN Restaurant r ON v.RestaurantID = r.RestaurantID
  JOIN Customer c ON v.CustomerID = c.CustomerID
  JOIN Server s ON v.ServerEmpID = s.ServerEmpID
  JOIN Bill b ON v.VisitID = b.VisitID
  JOIN PaymentMethod pm ON b.PaymentID = pm.PaymentID
  ORDER BY v.VisitID DESC
  LIMIT 1;
  "
  VisitResult <- dbGetQuery(con, queryToGetLastVisit)
  cat("Last inserted row in Visit table:", "\n")
  print(VisitResult)
  
  # expected values from the db
  expectedLoyaltyMember <- as.numeric(expectedLoyaltyMember)
  expectedMealTypeID <- as.numeric(expectedMealTypeID)
  expectedPartySize <- as.numeric(expectedPartySize)
  expectedWaitTime <- as.numeric(expectedWaitTime)
  expectedFoodBill <- as.numeric(expectedFoodBill)
  expectedAlcoholBill <- as.numeric(expectedAlcoholBill)
  expectedTipAmount <- as.numeric(expectedTipAmount)
  expectedDiscountApplied <- as.numeric(expectedDiscountApplied)
  expectedOrderedAlcohol <- as.numeric(expectedOrderedAlcohol)
  expectedPaymentID <- as.numeric(expectedPaymentID)
  
  print("Testing inserted visit details")
  
  print("RestaurantName matches")
  test_that("Restaurant Name matches", {
    outputMsg <- paste("Expected:", expectedRestaurantName, "| DB:", VisitResult$RestaurantName)
    expect_equal(VisitResult$RestaurantName, expectedRestaurantName, info = outputMsg)
    print(outputMsg)
  })
  
  print("CustomerName matches")
  test_that("Customer Name matches", {
    outputMsg <- paste("Expected:", expectedCustomerName, "| DB:", VisitResult$CustomerName)
    expect_equal(VisitResult$CustomerName, expectedCustomerName, info = outputMsg)
    print(outputMsg)
  })
  
  print("CustomerPhone matches")
  test_that("Customer Phone matches", {
    outputMsg <- paste("Expected:", expectedCustomerPhone, "| DB:", VisitResult$CustomerPhone)
    expect_equal(VisitResult$CustomerPhone, expectedCustomerPhone, info = outputMsg)
    print(outputMsg)
  })
  
  print("CustomerEmail matches")
  test_that("Customer Email matches", {
    outputMsg <- paste("Expected:", expectedCustomerEmail, "| DB:", VisitResult$CustomerEmail)
    expect_equal(VisitResult$CustomerEmail, expectedCustomerEmail, info = outputMsg)
    print(outputMsg)
  })
  
  print("LoyaltyMember matches")
  test_that("Loyalty Member matches", {
    outputMsg <- paste("Expected:", expectedLoyaltyMember, "| DB:", VisitResult$LoyaltyMember)
    expect_equal(VisitResult$LoyaltyMember, expectedLoyaltyMember, info = outputMsg)
    print(outputMsg)
  })
  
  print("ServerName matches")
  test_that("Server Name matches", {
    outputMsg <- paste("Expected:", expectedServerName, "| DB:", VisitResult$ServerName)
    expect_equal(VisitResult$ServerName, expectedServerName, info = outputMsg)
    print(outputMsg)
  })
  
  print("Visit Date matches")
  test_that("Visit Date matches", {
    outputMsg <- paste("Expected:", expectedVisitDate, "| DB:", VisitResult$VisitDate)
    expect_equal(VisitResult$VisitDate, expectedVisitDate, info = outputMsg)
    print(outputMsg)
  })
  
  print("Meal Type matches")
  test_that("Meal Type matches", {
    outputMsg <- paste("Expected:", expectedMealTypeID, "| DB:", VisitResult$MealTypeID)
    expect_equal(VisitResult$MealTypeID, expectedMealTypeID, info = outputMsg)
    print(outputMsg)
  })
  
  print("Party Size matches")
  test_that("Party Size matches", {
    outputMsg <- paste("Expected:", expectedPartySize, "| DB:", VisitResult$PartySize)
    expect_equal(VisitResult$PartySize, expectedPartySize, info = outputMsg)
    print(outputMsg)
  })
  
  print("Wait Time matches")
  test_that("Wait Time matches", {
    outputMsg <- paste("Expected:", expectedWaitTime, "| DB:", VisitResult$WaitTime)
    expect_equal(VisitResult$WaitTime, expectedWaitTime, info = outputMsg)
    print(outputMsg)
  })
  
  print("Food Bill matches")
  test_that("Food Bill matches", {
    outputMsg <- paste("Expected:", expectedFoodBill, "| DB:", VisitResult$FoodBill)
    expect_equal(VisitResult$FoodBill, expectedFoodBill, info = outputMsg)
    print(outputMsg)
  })
  
  print("Alcohol Bill matches")
  test_that("Alcohol Bill matches", {
    outputMsg <- paste("Expected:", expectedAlcoholBill, "| DB:", VisitResult$AlcoholBill)
    expect_equal(VisitResult$AlcoholBill, expectedAlcoholBill, info = outputMsg)
    print(outputMsg)
  })
  
  print("Tip Amount matches")
  test_that("Tip Amount matches", {
    outputMsg <- paste("Expected:", expectedTipAmount, "| DB:", VisitResult$TipAmount)
    expect_equal(VisitResult$TipAmount, expectedTipAmount, info = outputMsg)
    print(outputMsg)
  })
  
  print("Discount Applied matches")
  test_that("Discount Applied matches", {
    outputMsg <- paste("Expected:", expectedDiscountApplied, "| DB:", VisitResult$DiscountApplied)
    expect_equal(VisitResult$DiscountApplied, expectedDiscountApplied, info = outputMsg)
    print(outputMsg)
  })
  
  print("Ordered Alcohol matches")
  test_that("Ordered Alcohol matches", {
    outputMsg <- paste("Expected:", expectedOrderedAlcohol, "| DB:", VisitResult$orderedAlcohol)
    expect_equal(VisitResult$orderedAlcohol, expectedOrderedAlcohol, info = outputMsg)
    print(outputMsg)
  })
  
  print("Payment Method matches")
  test_that("Payment Method matches", {
    outputMsg <- paste("Expected:", expectedPaymentID, "| DB:", VisitResult$PaymentID)
    expect_equal(VisitResult$PaymentID, expectedPaymentID, info = outputMsg)
    print(outputMsg)
  })
}

# Main function
main <- function() {
  # required packages
  packages <- c("RMySQL", "DBI", "testthat")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to database
  con <- connectAndCheckDatabase()
  
  # create `StoreVisit` and `StoreNewVisit` stored procedure
  createStoreVisitProcedure(con)
  createStoreNewVisitProcedure(con)
  
  # Define VisitDetails with new parameters
  # Order: RestaurantID, CustomerID, VisitDate, VisitTime, MealTypeID, PartySize, Genders, WaitTime, FoodBill, AlcoholBill, TipAmount, DiscountApplied, OrderedAlcohol, PaymentID, ServerEmpID
  VisitDetails <- c(7, 15, "2025-04-11", "12:00:00", 3, 2, "mf", 10, 50.00, 33.00, 0.10, 0, TRUE, 1, 1024)
  
  # call StoreVisit stored procedure
  callStoreVisit(con = con, 
                 RestaurantID = VisitDetails[1], 
                 CustomerID = VisitDetails[2], 
                 VisitDate = VisitDetails[3], 
                 VisitTime = VisitDetails[4], 
                 MealTypeID = VisitDetails[5], 
                 PartySize = VisitDetails[6], 
                 Genders = VisitDetails[7], 
                 WaitTime = VisitDetails[8], 
                 FoodBill = VisitDetails[9], 
                 AlcoholBill = VisitDetails[10], 
                 TipAmount = VisitDetails[11], 
                 DiscountApplied = VisitDetails[12], 
                 OrderedAlcohol = VisitDetails[13], 
                 PaymentID = VisitDetails[14], 
                 ServerEmpID = VisitDetails[15])
  
  # verify Visit insertion assuming that the server, customer, and restaurant already exist
  verifyVisitInsertionForStoreVisit(con, VisitDetails)
  
  # call StoreNewVisit stored procedure
  callStoreNewVisit(con = con, 
                    RestaurantName = "Deuxave", 
                    CustomerName = "Sai Karthikeyan Sura", 
                    CustomerPhone = "(857) 233-8390", 
                    CustomerEmail = "karthik@email.com",
                    LoyaltyMember = TRUE,
                    ServerName = "Oasis",
                    VisitDate = "2025-03-11",
                    MealTypeID = 2,
                    PartySize = 3,
                    WaitTime = 15,
                    FoodBill = 100.00,
                    AlcoholBill = 150.00,
                    TipAmount = 10.00,
                    DiscountApplied = 15.00,
                    OrderedAlcohol = TRUE,
                    PaymentID = 2)
  
  # verify visit insertion
  verifyStoreNewVisitInsertion(con = con, 
                               expectedRestaurantName = "Deuxave", 
                               expectedCustomerName = "Sai Karthikeyan Sura", 
                               expectedCustomerPhone = "(857) 233-8390", 
                               expectedCustomerEmail = "karthik@email.com",
                               expectedLoyaltyMember = TRUE,
                               expectedServerName = "Oasis",
                               expectedVisitDate = "2025-03-11",
                               expectedMealType = 2,
                               expectedPartySize = 3,
                               expectedWaitTime = 15,
                               expectedFoodBill = 100.00,
                               expectedAlcoholBill = 150.00,
                               expectedTipAmount = 10.00,
                               expectedDiscountApplied = 15.00,
                               expectedOrderedAlcohol = TRUE,
                               expectedPaymentID = 2)

  # Disconnect from the database
  dbDisconnect(con)
  print("Disconnected from the database")
}

# execute the script
main()