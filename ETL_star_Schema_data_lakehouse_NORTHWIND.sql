USE master;
GO


IF EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_NORTHWIND'
)
BEGIN
    ALTER DATABASE [ETL_star_Schema_data_lakehouse_NORTHWIND]
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE [ETL_star_Schema_data_lakehouse_NORTHWIND];
END
--------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_NORTHWIND'
)
BEGIN
	create database ETL_star_Schema_data_lakehouse_NORTHWIND;
END

IF EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_NORTHWIND'
)
BEGIN
   USE ETL_star_Schema_data_lakehouse_NORTHWIND;
END

print 'DATABASE [ETL_star_Schema_data_lakehouse_NORTHWIND] HAS BEEN CREATED';
GO

----- creating Bronze, silver and gold layer
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    DROP SCHEMA bronze;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    DROP SCHEMA silver;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    DROP SCHEMA gold;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

print 'bonze, silver & gold Layer has been created succfully';
GO

------------------------------------------------------------------





CREATE OR ALTER PROCEDURE BRONZE_PROCEDURE 
	AS
	BEGIN
		SET NOCOUNT ON;
		 -------------  bronze.Customers
		DROP TABLE IF EXISTS bronze.Customers;
		CREATE TABLE bronze.Customers(
			CustomerID nchar(5) ,
			CompanyName nvarchar(40) ,
			ContactName nvarchar(30) ,
			ContactTitle nvarchar(30) ,
			Address nvarchar(60) ,
			City nvarchar(15) NULL,
			Region nvarchar(15) ,
			PostalCode nvarchar(10) ,
			Country nvarchar(15) ,
			Phone nvarchar(24) ,
			Fax nvarchar(24),
			load_date DATETIME2 DEFAULT GETDATE(),
			source_file NVARCHAR(255),
			is_processed BIT DEFAULT 0
		);

		INSERT INTO bronze.Customers (
			CustomerID, CompanyName, ContactName, ContactTitle,
			Address, City, Region, PostalCode, Country, Phone, Fax, source_file
			)
		SELECT 
			CustomerID, CompanyName, ContactName, ContactTitle,
			Address, City, Region, PostalCode, Country, Phone, Fax, 'NORTHWIND.dbo.Customers'
		FROM NORTHWIND.dbo.Customers;
		



		-----------------------    bronze.Products
		DROP TABLE IF EXISTS bronze.Products;
		CREATE TABLE bronze.Products(
			ProductID int,
			ProductName nvarchar(40) NOT NULL,
			SupplierID int NULL,
			CategoryID int NULL,
			QuantityPerUnit nvarchar(20) NULL,
			UnitPrice money NULL,
			UnitsInStock smallint,
			UnitsOnOrder smallint,
			ReorderLevel smallint ,
			Discontinued bit,
			load_date DATETIME2 DEFAULT GETDATE(),
			source_file NVARCHAR(255),
			is_processed BIT DEFAULT 0
		);

		insert into bronze.Products(
			ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit,
			UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued, source_file
		)
		select ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit,
			UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,  'NORTHWIND.dbo.Products'
		from NORTHWIND.dbo.Products;


		-----------------------    bronze.Categories
		DROP TABLE IF EXISTS bronze.Categories;
		CREATE TABLE bronze.Categories(
			CategoryID int,
			CategoryName nvarchar(15) ,
			Description ntext NULL,
			Picture image NULL,
			load_date DATETIME2 DEFAULT GETDATE(),
			source_file NVARCHAR(255),
			is_processed BIT DEFAULT 0
		);

		insert into bronze.Categories (CategoryID, CategoryName, Description, Picture, source_file)
		select CategoryID, CategoryName, Description, Picture, 'NORTHWIND.dbo.Categories'
		from NORTHWIND.dbo.Categories;
		


		----------------------    bronze.Suppliers
		DROP TABLE IF EXISTS bronze.Suppliers;
		CREATE TABLE bronze.Suppliers (
			SupplierID int ,
			CompanyName nvarchar(40) ,
			ContactName nvarchar(30) ,
			ContactTitle nvarchar(30) ,
			Address nvarchar(60) ,
			City nvarchar(15) ,
			Region nvarchar(15) ,
			PostalCode nvarchar(10) ,
			Country nvarchar(15) ,
			Phone nvarchar(24) ,
			Fax nvarchar(24) ,
			HomePage ntext ,
			load_date DATETIME2 DEFAULT GETDATE(),
			source_file NVARCHAR(255),
			is_processed BIT DEFAULT 0
		);
		

		INSERT INTO bronze.Suppliers(
			SupplierID, CompanyName, ContactName, 
			ContactTitle, Address, City, Region,
			PostalCode, Country, Phone, Fax, HomePage, source_file 
			)
		SELECT 
			SupplierID, CompanyName, ContactName, 
			ContactTitle, Address, City, Region,
			PostalCode, Country, Phone, Fax, HomePage, 'NORTHWIND.dbo.Suppliers' 
		FROM NORTHWIND.dbo.Suppliers;



		----------------------    bronze.Employees
		DROP TABLE IF EXISTS bronze.Employees;
		CREATE TABLE bronze.Employees(
			EmployeeID        INT,
			LastName          NVARCHAR(20),
			FirstName         NVARCHAR(10),
			Title             NVARCHAR(30),
			TitleOfCourtesy   NVARCHAR(25),
			BirthDate         DATETIME,
			HireDate          DATETIME,
			Address           NVARCHAR(60),
			City              NVARCHAR(15),
			Region            NVARCHAR(15),
			PostalCode        NVARCHAR(10),
			Country           NVARCHAR(15),
			HomePhone         NVARCHAR(24),
			Extension         NVARCHAR(4),
			Photo             IMAGE,
			Notes             NTEXT,
			ReportsTo         INT,
			PhotoPath         NVARCHAR(255),
			load_date         DATETIME2 DEFAULT GETDATE(),
			source_file       NVARCHAR(255) DEFAULT 'NORTHWIND.dbo.Employees',
			is_processed      BIT DEFAULT 0
		);
		-- Insert data into bronze.Employees from the Northwind source
		INSERT INTO bronze.Employees (
			EmployeeID, LastName, FirstName, Title, TitleOfCourtesy,
			BirthDate, HireDate, Address, City, Region, PostalCode,
			Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath
		)
		SELECT 
			EmployeeID, LastName, FirstName, Title, TitleOfCourtesy,
			BirthDate, HireDate, Address, City, Region, PostalCode,
			Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath
		FROM NORTHWIND.dbo.Employees;


		--select * from bronze.Employees;


		DROP TABLE IF EXISTS bronze.Orders;
		-- Create the Bronze.Orders table
		CREATE TABLE bronze.Orders (
			OrderID          INT,
			CustomerID       NCHAR(5),
			EmployeeID       INT,
			OrderDate        DATETIME,
			RequiredDate     DATETIME,
			ShippedDate      DATETIME,
			ShipVia          INT,
			Freight          MONEY,
			ShipName         NVARCHAR(40),
			ShipAddress      NVARCHAR(60),
			ShipCity         NVARCHAR(15),
			ShipRegion       NVARCHAR(15),
			ShipPostalCode   NVARCHAR(10),
			ShipCountry      NVARCHAR(15),
			load_date        DATETIME2 DEFAULT GETDATE(),
			source_file      NVARCHAR(255) DEFAULT 'NORTHWIND.dbo.Orders',
			is_processed     BIT DEFAULT 0
		);


		-- Load data into bronze.Orders from the original Orders table
		INSERT INTO bronze.Orders (
			OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate,
			ShippedDate, ShipVia, Freight, ShipName, ShipAddress,
			ShipCity, ShipRegion, ShipPostalCode, ShipCountry
		)
		SELECT 
			OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate,
			ShippedDate, ShipVia, Freight, ShipName, ShipAddress,
			ShipCity, ShipRegion, ShipPostalCode, ShipCountry
		FROM NORTHWIND.dbo.Orders;



		-- Create the new table under the bronze schema
		CREATE TABLE bronze.OrderDetails (
			OrderID        INT NOT NULL,
			ProductID      INT NOT NULL,
			UnitPrice      MONEY NOT NULL,
			Quantity       SMALLINT NOT NULL,
			Discount       REAL NOT NULL,
			load_date      DATETIME2 DEFAULT GETDATE(),
			source_file    NVARCHAR(255) DEFAULT 'NORTHWIND.dbo.[Order Details]',
			is_processed   BIT DEFAULT 0
		);

		-- Insert data into bronze.OrderDetails from the original table
		INSERT INTO bronze.OrderDetails (
			OrderID, ProductID,
			UnitPrice,Quantity,
			Discount
		)
		SELECT 
			OrderID,ProductID,
			UnitPrice,Quantity,
			Discount
		FROM NORTHWIND.dbo.[Order Details];

	END;
GO



CREATE OR ALTER PROCEDURE SILVER_PROCEDURE
	AS
	BEGIN
		-- silver.Customer
		DROP TABLE IF EXISTS silver.Customer;
		CREATE TABLE silver.Customer (
			CustomerID NVARCHAR(5) PRIMARY KEY,
			CompanyName NVARCHAR(40),
			ContactName NVARCHAR(30),
			City NVARCHAR(15),
			Country NVARCHAR(15),
			load_date DATETIME2 DEFAULT GETDATE()
		);
		INSERT INTO silver.Customer (CustomerID, CompanyName, ContactName, City, Country)
		SELECT DISTINCT CustomerID, CompanyName, ContactName, City, Country
		FROM bronze.Customers
		WHERE EXISTS (SELECT 1 FROM bronze.Customers WHERE is_processed = 0);
		UPDATE bronze.Customers SET is_processed = 1;


		-- silver.Products
		DROP TABLE IF EXISTS silver.Products;
		CREATE TABLE silver.Products(
			ProductID INT PRIMARY KEY,
			ProductName NVARCHAR(40) NOT NULL,
			SupplierID INT NULL,
			CategoryID INT NULL,
			load_date DATETIME2 DEFAULT GETDATE()
		);
		INSERT INTO silver.Products (ProductID, ProductName, SupplierID, CategoryID)
		SELECT ProductID, ProductName, SupplierID, CategoryID
		FROM bronze.Products
		WHERE EXISTS (SELECT 1 FROM bronze.Products WHERE is_processed = 0);
		UPDATE bronze.Products SET is_processed = 1;


		-- silver.Categories
		DROP TABLE IF EXISTS silver.Categories;
		CREATE TABLE silver.Categories(
			CategoryID INT PRIMARY KEY,
			CategoryName NVARCHAR(15),
			Description NTEXT NULL,
			load_date DATETIME2 DEFAULT GETDATE()
		);
		INSERT INTO silver.Categories (CategoryID, CategoryName, Description)
		SELECT CategoryID, CategoryName, Description
		FROM bronze.Categories
		WHERE EXISTS (SELECT 1 FROM bronze.Categories WHERE is_processed = 0);
		UPDATE bronze.Categories SET is_processed = 1;


		-- silver.Suppliers
		DROP TABLE IF EXISTS silver.Suppliers;
		CREATE TABLE silver.Suppliers (
			SupplierID INT PRIMARY KEY,
			CompanyName NVARCHAR(40),
			ContactName NVARCHAR(30),
			ContactTitle NVARCHAR(30),
			Address NVARCHAR(60),
			City NVARCHAR(15),
			PostalCode NVARCHAR(10),
			Country NVARCHAR(15),
			Phone NVARCHAR(24),
			load_date DATETIME2 DEFAULT GETDATE()
		);
		INSERT INTO silver.Suppliers (
			SupplierID, CompanyName, ContactName, ContactTitle,
			Address, City, PostalCode, Country, Phone
		)
		SELECT SupplierID, CompanyName, ContactName, ContactTitle,
			   Address, City, PostalCode, Country, Phone
		FROM bronze.Suppliers
		WHERE EXISTS (SELECT 1 FROM bronze.Suppliers WHERE is_processed = 0);
		UPDATE bronze.Suppliers SET is_processed = 1;

				-- Step 3: Add FK constraints outside the procedure
		ALTER TABLE silver.Products
		ADD CONSTRAINT FK_Products_Suppliers FOREIGN KEY (SupplierID) REFERENCES silver.Suppliers(SupplierID);

		ALTER TABLE silver.Products
		ADD CONSTRAINT FK_Products_Categories FOREIGN KEY (CategoryID) REFERENCES silver.Categories(CategoryID);

		-- silver.Employee
		DROP TABLE IF EXISTS silver.Employee;
		CREATE TABLE silver.Employee (
			EmployeeID INT PRIMARY KEY,
			FirstName NVARCHAR(10),
			LastName NVARCHAR(20),
			Title NVARCHAR(30),
			City NVARCHAR(15),
			Country NVARCHAR(15)
		);
		INSERT INTO silver.Employee (EmployeeID, FirstName, LastName, Title, City, Country)
		SELECT DISTINCT EmployeeID, FirstName, LastName, Title, City, Country
		FROM bronze.Employees
		WHERE EXISTS (SELECT 1 FROM bronze.Employees WHERE is_processed = 0);
		UPDATE bronze.Employees SET is_processed = 1;


		 -- silver.Orders
        DROP TABLE IF EXISTS silver.Orders;
        CREATE TABLE silver.Orders (
            OrderID INT PRIMARY KEY,
            CustomerID NVARCHAR(5),
            EmployeeID INT,
            OrderDate DATETIME,
            ShipCountry NVARCHAR(15),
            load_date DATETIME2 DEFAULT GETDATE()
        );
        INSERT INTO silver.Orders (
            OrderID, CustomerID, EmployeeID, OrderDate, ShipCountry
        )
        SELECT OrderID, CustomerID, EmployeeID, OrderDate, ShipCountry
        FROM bronze.Orders
        WHERE is_processed = 0;
        UPDATE bronze.Orders SET is_processed = 1;


        -- silver.OrderDetails
        DROP TABLE IF EXISTS silver.OrderDetails;
        CREATE TABLE silver.OrderDetails (
            OrderID INT,
            ProductID INT,
            UnitPrice MONEY,
            Quantity SMALLINT,
            Discount REAL,
            load_date DATETIME2 DEFAULT GETDATE(),
            PRIMARY KEY (OrderID, ProductID)
        );
        INSERT INTO silver.OrderDetails (
            OrderID, ProductID, UnitPrice, Quantity, Discount
        )
        SELECT OrderID, ProductID, UnitPrice, Quantity, Discount
        FROM bronze.OrderDetails
        WHERE is_processed = 0;
        UPDATE bronze.OrderDetails SET is_processed = 1;
		        -- Foreign keys for Orders and OrderDetails
        ALTER TABLE silver.Orders
        ADD CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES silver.Customer(CustomerID);

        ALTER TABLE silver.Orders
        ADD CONSTRAINT FK_Orders_Employees FOREIGN KEY (EmployeeID) REFERENCES silver.Employee(EmployeeID);

        ALTER TABLE silver.OrderDetails
        ADD CONSTRAINT FK_OrderDetails_Orders FOREIGN KEY (OrderID) REFERENCES silver.Orders(OrderID);

        ALTER TABLE silver.OrderDetails
        ADD CONSTRAINT FK_OrderDetails_Products FOREIGN KEY (ProductID) REFERENCES silver.Products(ProductID);




	END;
GO



CREATE OR ALTER PROCEDURE GOLD_PROCEDURE
	AS
	BEGIN

		DROP TABLE IF EXISTS gold.DimProduct
		CREATE TABLE gold.DimProduct (
			ProductKey INT IDENTITY(1,1) PRIMARY KEY,
			ProductID INT,
			SupplierID int,
			ProductName NVARCHAR(40),
			Category NVARCHAR(15),
			Supplier NVARCHAR(40)
		);
		INSERT INTO gold.DimProduct (ProductID,SupplierID, ProductName, Category, Supplier)
		SELECT p.ProductID,p.SupplierID, p.ProductName, c.CategoryName, s.CompanyName
		FROM silver.Products p
		JOIN silver.Categories c ON p.CategoryID = c.CategoryID
		JOIN silver.Suppliers s ON p.SupplierID = s.SupplierID;





		-- Drop the gold table if it exists
		DROP TABLE IF EXISTS gold.DimCustomer;

		-- Create the gold.DimCustomer table
		CREATE TABLE gold.DimCustomer (
			CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
			CustomerID NVARCHAR(5),
			CompanyName NVARCHAR(40),
			ContactName NVARCHAR(30),
			City NVARCHAR(15),
			Country NVARCHAR(15)
		);

		-- Insert data from silver.DimCustomer excluding the IDENTITY column
		INSERT INTO gold.DimCustomer (CustomerID, CompanyName, ContactName, City, Country)
		SELECT CustomerID, CompanyName, ContactName, City, Country
		FROM silver.Customer;



		
		-- Drop the gold.DimEmployee table if it already exists
		DROP TABLE IF EXISTS gold.DimEmployee;

		-- Create the gold.DimEmployee table
		CREATE TABLE gold.DimEmployee (
			EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
			EmployeeID INT,
			Fullname NVARCHAR(20),
			Title NVARCHAR(30),
			City NVARCHAR(15),
			Country NVARCHAR(15)
		);

		-- Insert data from silver.DimEmployee (excluding the identity column)
		INSERT INTO gold.DimEmployee (EmployeeID, FULLNAME, Title, City, Country)
		SELECT EmployeeID, (FirstName + ' ' + LastName) AS FULLNAME, Title, City, Country
		FROM silver.Employee;


		drop table if exists gold.DimTime;
		-- DimTime
		CREATE TABLE gold.DimTime (
			TimeKey INT PRIMARY KEY,
			FullDate DATE NOT NULL,
			DayNumberOfWeek TINYINT NOT NULL,
			DayName NVARCHAR(10) NOT NULL,
			DayNumberOfMonth TINYINT NOT NULL,
			DayNumberOfYear SMALLINT NOT NULL,
			WeekNumberOfYear TINYINT NOT NULL,
			MonthName NVARCHAR(10) NOT NULL,
			MonthNumberOfYear TINYINT NOT NULL,
			CalendarQuarter TINYINT NOT NULL,
			CalendarYear SMALLINT NOT NULL,
			IsWeekend BIT NOT NULL
		);

		-- Populate DimTime (based on Orders.OrderDate)
		INSERT INTO gold.DimTime (TimeKey, FullDate,
				DayNumberOfWeek,
				DayName,
				DayNumberOfMonth,
				DayNumberOfYear,
				WeekNumberOfYear,
				MonthName,
				MonthNumberOfYear,
				CalendarQuarter,
				CalendarYear,
				IsWeekend)
		SELECT DISTINCT 
			CAST(CONVERT(VARCHAR, OrderDate, 112) AS INT) AS TimeKey,
			OrderDate, -- FullDate
			DATEPART(WEEKDAY, OrderDate), -- DayNumberOfWeek
			DATENAME(WEEKDAY, OrderDate), -- DayName
			DATEPART(DAY, OrderDate), -- DayNumberOfMonth
			DATEPART(DAYOFYEAR, OrderDate), -- DayNumberOfYear
			DATEPART(WEEK, OrderDate), -- WeekNumberOfYear
			DATENAME(MONTH, OrderDate), -- MonthName
			DATEPART(MONTH, OrderDate), -- MonthNumberOfYear
			DATEPART(QUARTER, OrderDate), -- CalendarQuarter
			DATEPART(YEAR, OrderDate), -- CalendarYear
			CASE WHEN DATEPART(WEEKDAY, OrderDate) IN (1, 7) THEN 1 ELSE 0 END -- IsWeekend
		FROM silver.Orders;
		
		drop table if exists gold.DimSupplier;
		CREATE TABLE gold.DimSupplier (
			SupplierKey INT PRIMARY KEY IDENTITY(1,1),
			SupplierID INT,
			CompanyName NVARCHAR(255),
			ContactName NVARCHAR(255),
			Country NVARCHAR(255)
		);

		INSERT INTO gold.DimSupplier (SupplierID, CompanyName, ContactName, Country)
		SELECT DISTINCT SupplierID, CompanyName, ContactName, Country
		FROM bronze.Suppliers;




		-- Drop existing FactSales if exists
		DROP TABLE IF EXISTS gold.FactSales;

		-- Create FactSales table with SupplierKey added
		CREATE TABLE gold.FactSales (
			SalesKey INT IDENTITY(1,1) PRIMARY KEY,
			OrderID INT,
			ProductKey INT,
			CustomerKey INT,
			EmployeeKey INT,
			TimeKey INT,
			SupplierKey INT,
			Quantity SMALLINT,
			UnitPrice MONEY,
			Discount FLOAT,
			TotalAmount AS (Quantity * UnitPrice * (1 - Discount)) PERSISTED
		);

		-- Insert data into FactSales with SupplierKey joined via ProductID
		INSERT INTO gold.FactSales (
			OrderID, ProductKey, CustomerKey, EmployeeKey, TimeKey, SupplierKey, Quantity, UnitPrice, Discount
		)
		SELECT 
			od.OrderID,
			dp.ProductKey,
			dc.CustomerKey,
			de.EmployeeKey,
			CAST(CONVERT(VARCHAR, o.OrderDate, 112) AS INT) AS TimeKey,
			ds.SupplierKey,
			od.Quantity,
			od.UnitPrice,
			od.Discount
		FROM silver.OrderDetails od
			JOIN bronze.Orders o ON od.OrderID = o.OrderID
			JOIN gold.DimCustomer dc ON o.CustomerID = dc.CustomerID
			JOIN gold.DimEmployee de ON o.EmployeeID = de.EmployeeID
			JOIN gold.DimProduct dp ON od.ProductID = dp.ProductID
			JOIN gold.DimSupplier ds ON dp.SupplierID = ds.SupplierID;


		-- Adding foreign key from FactSales to DimProduct (ProductKey)
			ALTER TABLE gold.FactSales
			ADD CONSTRAINT FK_FactSales_ProductKey
			FOREIGN KEY (ProductKey) REFERENCES gold.DimProduct (ProductKey);

			-- Adding foreign key from FactSales to DimCustomer (CustomerKey)
			ALTER TABLE gold.FactSales
			ADD CONSTRAINT FK_FactSales_CustomerKey
			FOREIGN KEY (CustomerKey) REFERENCES gold.DimCustomer (CustomerKey);

			-- Adding foreign key from FactSales to DimEmployee (EmployeeKey)
			ALTER TABLE gold.FactSales
			ADD CONSTRAINT FK_FactSales_EmployeeKey
			FOREIGN KEY (EmployeeKey) REFERENCES gold.DimEmployee (EmployeeKey);

			-- Adding foreign key from FactSales to DimTime (TimeKey)
			ALTER TABLE gold.FactSales
			ADD CONSTRAINT FK_FactSales_TimeKey
			FOREIGN KEY (TimeKey) REFERENCES gold.DimTime (TimeKey);

			-- Adding foreign key from FactSales to DimSupplier (SupplierKey)
			ALTER TABLE gold.FactSales
			ADD CONSTRAINT FK_FactSales_SupplierKey
			FOREIGN KEY (SupplierKey) REFERENCES gold.DimSupplier (SupplierKey);


	END;
GO


CREATE OR ALTER PROCEDURE MEDALION_ARCHITECTURE
	AS
	BEGIN
	PRINT '----------------------------------------------------------------------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '-------------------------           DATA LAKEHOUSE                ----------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '----------------------------------------------------------------------------------------------';

	EXEC BRONZE_PROCEDURE;
	PRINT '----------------------------------------------------------------------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '-------------------------    BRONZE LAYER HAS BEEN CREATED      ------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '----------------------------------------------------------------------------------------------';
	EXEC SILVER_PROCEDURE;
	PRINT '----------------------------------------------------------------------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '-------------------------    SILVER LAYER HAS BEEN CREATED      ------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '----------------------------------------------------------------------------------------------';
	EXEC GOLD_PROCEDURE;
	PRINT '----------------------------------------------------------------------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '-------------------------      GOLD LAYER HAS BEEN CREATED      ------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '----------------------------------------------------------------------------------------------';

	PRINT '----------------------------------------------------------------------------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '-------------------------    DATA LAKEHOUSE HAS BEEN CREATED      ----------------------------';
	PRINT '-------------------------------------                    -------------------------------------';
	PRINT '----------------------------------------------------------------------------------------------';

	END;
GO


EXEC MEDALION_ARCHITECTURE;