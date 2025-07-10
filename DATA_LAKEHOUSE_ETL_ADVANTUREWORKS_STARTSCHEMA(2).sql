USE master;
GO


IF EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_AdventureWorksLT2022'
)
BEGIN
    ALTER DATABASE [ETL_star_Schema_data_lakehouse_AdventureWorksLT2022]
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE [ETL_star_Schema_data_lakehouse_AdventureWorksLT2022];
END
--------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_AdventureWorksLT2022'
)
BEGIN
	create database ETL_star_Schema_data_lakehouse_AdventureWorksLT2022;
END

IF EXISTS (
    SELECT name FROM sys.databases 
    WHERE name = 'ETL_star_Schema_data_lakehouse_AdventureWorksLT2022'
)
BEGIN
   USE ETL_star_Schema_data_lakehouse_AdventureWorksLT2022;
END




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



CREATE OR ALTER PROCEDURE bronze_layer_1
	as
	BEGIN
		-- 1. Create Customer table with primary key
		DROP TABLE IF EXISTS bronze.Customer;
		CREATE TABLE bronze.Customer(
			CustomerID int NOT NULL ,
			NameStyle nvarchar(8),
			Title nvarchar(8),
			FirstName nvarchar(50) NOT NULL,
			MiddleName nvarchar(50),
			LastName nvarchar(50) NOT NULL,
			Suffix nvarchar(10),
			CompanyName nvarchar(128),
			SalesPerson nvarchar(256),
			EmailAddress nvarchar(50),
			Phone nvarchar(25),
			PasswordHash varchar(128),
			PasswordSalt varchar(10),
			rowguid uniqueidentifier NOT NULL,
			ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.Customer',
			is_processed BIT DEFAULT 0
		);

		INSERT INTO bronze.Customer (
			CustomerID, NameStyle, Title, FirstName, MiddleName, LastName,
			Suffix, CompanyName, SalesPerson, EmailAddress, Phone,
			PasswordHash, PasswordSalt, rowguid, ModifiedDate
		)
		SELECT
			CustomerID, NameStyle, Title, FirstName, MiddleName, LastName,
			Suffix, CompanyName, SalesPerson, EmailAddress, Phone,
			PasswordHash, PasswordSalt, rowguid, ModifiedDate
		FROM AdventureWorksLT2022.SalesLT.Customer;
		
		
		
		PRINT 'DATA HAS BEEN INSERTED INTO bronze.Customer FROM AdventureWorksLT2022.SalesLT.Customer';

		-- 2. Create Address table with primary key
		DROP TABLE IF EXISTS bronze.Address;
		CREATE TABLE bronze.Address (
			AddressID int NOT NULL ,
			AddressLine1 nvarchar(60) NOT NULL,
			AddressLine2 nvarchar(60),
			City nvarchar(30) NOT NULL,
			StateProvince nvarchar(50) NOT NULL,
			CountryRegion nvarchar(50) NOT NULL,
			PostalCode nvarchar(15) NOT NULL,
			rowguid uniqueidentifier NOT NULL,
			ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.Address',
			is_processed BIT DEFAULT 0
		);

		INSERT INTO bronze.Address (
			AddressID, AddressLine1, AddressLine2, City,
			StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
		)
		SELECT 
			AddressID, AddressLine1, AddressLine2, City,
			StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate
		FROM AdventureWorksLT2022.SalesLT.Address;

		PRINT 'DATA HAS BEEN INSERTED INTO bronze.Address FROM AdventureWorksLT2022.SalesLT.Address';

		-- 3. Create CustomerAddress table with composite primary key and foreign keys
		DROP TABLE IF EXISTS bronze.CustomerAddress;
		CREATE TABLE bronze.CustomerAddress (
			CustomerID int NOT NULL,
			AddressID int NOT NULL,
			AddressType nvarchar(50) NOT NULL,
			rowguid uniqueidentifier NOT NULL,
			ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.CustomerAddress',
			is_processed BIT DEFAULT 0,
		);
		INSERT INTO bronze.CustomerAddress(
			CustomerID, AddressID, AddressType, rowguid, ModifiedDate
		)
		SELECT CustomerID, AddressID, AddressType, rowguid, ModifiedDate 
		FROM AdventureWorksLT2022.SalesLT.CustomerAddress;
		PRINT 'DATA HAS BEEN INSERTED INTO bronze.Address FROM AdventureWorksLT2022.SalesLT.CustomerAddress';
		-- 1. Create ProductCategory table
		DROP TABLE IF EXISTS bronze.ProductCategory;
		CREATE TABLE bronze.ProductCategory (
			ProductCategoryID int NOT NULL ,
			ParentProductCategoryID int,
			Name nvarchar(50),
			rowguid uniqueidentifier,
			ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.ProductCategory',
			is_processed BIT DEFAULT 0
		);

		-- 2. Create ProductModel table
		DROP TABLE IF EXISTS bronze.ProductModel;
		CREATE TABLE bronze.ProductModel (
			ProductModelID int NOT NULL,
			Name nvarchar(50),
			CatalogDescription xml,
			rowguid uniqueidentifier,
			ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.ProductModel',
			is_processed BIT DEFAULT 0
		);

		-- 3. Create Product table
		DROP TABLE IF EXISTS bronze.Product;
		CREATE TABLE bronze.Product (
			ProductID int NOT NULL,
			Name nvarchar(50),
			ProductNumber nvarchar(25),
			Color nvarchar(15),
			StandardCost money,
			ListPrice money,
			Size nvarchar(5),
			Weight decimal(8, 2),
			ProductCategoryID int,
			ProductModelID int,
			SellStartDate datetime,
			SellEndDate datetime,
			DiscontinuedDate datetime,
			ThumbNailPhoto varbinary(max),
			ThumbnailPhotoFileName nvarchar(50),
			rowguid uniqueidentifier,
			ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.Product',
			is_processed BIT DEFAULT 0
		);
		-- 4. Insert data into ProductCategory
			INSERT INTO bronze.ProductCategory (
				ProductCategoryID, ParentProductCategoryID, Name,
				rowguid, ModifiedDate
			)
			SELECT
				ProductCategoryID, ParentProductCategoryID, Name,
				rowguid, ModifiedDate
			FROM AdventureWorksLT2022.SalesLT.ProductCategory;

			-- 5. Insert data into ProductModel
			INSERT INTO bronze.ProductModel (
				ProductModelID, Name, CatalogDescription, rowguid, ModifiedDate
			)
			SELECT
				ProductModelID, Name, CatalogDescription, rowguid, ModifiedDate
			FROM AdventureWorksLT2022.SalesLT.ProductModel;

			-- 6. Insert data into Product
			INSERT INTO bronze.Product (
				ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
				Size, Weight, ProductCategoryID, ProductModelID, SellStartDate,
				SellEndDate, DiscontinuedDate, ThumbNailPhoto, ThumbnailPhotoFileName, 
				rowguid, ModifiedDate
			)
			SELECT
				ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
				Size, Weight, ProductCategoryID, ProductModelID, SellStartDate,
				SellEndDate, DiscontinuedDate, ThumbNailPhoto, ThumbnailPhotoFileName, 
				rowguid, ModifiedDate
			FROM AdventureWorksLT2022.SalesLT.Product;

			----------------------------------------------------------------------------------------------

			DROP TABLE IF EXISTS bronze.SalesOrderDetail;
		CREATE TABLE bronze.SalesOrderDetail (
			SalesOrderID int,
			SalesOrderDetailID int,
			OrderQty smallint,
			ProductID int,
			UnitPrice money,
			UnitPriceDiscount money,
			LineTotal numeric(38,6),
			ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.SalesOrderDetail',
			is_processed BIT DEFAULT 0
		);
		INSERT INTO bronze.SalesOrderDetail (
			SalesOrderID, SalesOrderDetailID, OrderQty,
			ProductID, UnitPrice, UnitPriceDiscount,
			LineTotal, ModifiedDate
		)
		SELECT
			SalesOrderID, SalesOrderDetailID, OrderQty,
			ProductID, UnitPrice, UnitPriceDiscount,
			LineTotal, ModifiedDate
		FROM AdventureWorksLT2022.SalesLT.SalesOrderDetail;
		PRINT 'DATA HAS BEEN INSERTED INTO bronze.SalesOrderDetail FROM AdventureWorksLT2022.SalesLT.SalesOrderDetail';


		DROP TABLE IF EXISTS bronze.SalesOrderHeader;
		CREATE TABLE bronze.SalesOrderHeader (
			SalesOrderID int,
			RevisionNumber tinyint,
			OrderDate datetime,
			DueDate datetime,
			ShipDate datetime,
			Status tinyint,
			OnlineOrderFlag bit, -- Assuming [dbo].[Flag] = BIT
			PurchaseOrderNumber nvarchar(25), -- Assuming [dbo].[OrderNumber] = NVARCHAR(25)
			AccountNumber nvarchar(15),       -- Assuming [dbo].[AccountNumber] = NVARCHAR(15)
			CustomerID int,
			ShipToAddressID int,
			BillToAddressID int,
			ShipMethod nvarchar(50),
			CreditCardApprovalCode varchar(15),
			SubTotal money,
			TaxAmt money,
			Freight money,
			Comment nvarchar(max),
			rowguid uniqueidentifier ROWGUIDCOL,
			ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'AdventureWorksLT2022.SalesLT.SalesOrderHeader',
			is_processed BIT DEFAULT 0
		);
		INSERT INTO bronze.SalesOrderHeader (
			SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
			Status, OnlineOrderFlag, PurchaseOrderNumber, AccountNumber,
			CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
			CreditCardApprovalCode, SubTotal, TaxAmt, Freight,
			Comment, rowguid, ModifiedDate
		)
		SELECT
			SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
			Status, OnlineOrderFlag, PurchaseOrderNumber, AccountNumber,
			CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
			CreditCardApprovalCode, SubTotal, TaxAmt, Freight,
			Comment, rowguid, ModifiedDate
		FROM AdventureWorksLT2022.SalesLT.SalesOrderHeader;
		PRINT 'DATA HAS BEEN INSERTED INTO bronze.SalesOrderHeader FROM AdventureWorksLT2022.SalesLT.SalesOrderHeader';

		PRINT '----------------------------------------------------------------------------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '-------------------------       BRONZE LAYER HAS BEEN CREATED     ----------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '----------------------------------------------------------------------------------------------';

		
	END;
GO
CREATE OR ALTER PROCEDURE silver_layer
	as
	BEGIN
		-- 1. Create Customer table with primary key
		DROP TABLE IF EXISTS silver.Customer;
		CREATE TABLE silver.Customer(
			CustomerID int NOT NULL PRIMARY KEY,
			--NameStyle nvarchar(8),
			Title nvarchar(8),
			FullName NVARCHAR(150) NOT NULL,
			--MiddleName NVARCHAR(20),
			--Suffix nvarchar(10),
			CompanyName nvarchar(128),
			SalesPerson nvarchar(256),
			EmailAddress nvarchar(50),
			Phone nvarchar(25),
			--PasswordHash varchar(128),
			--PasswordSalt varchar(10),
			--rowguid uniqueidentifier NOT NULL,
			--ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'bronze.Customer',
			is_processed BIT DEFAULT 0
		);

		INSERT INTO silver.Customer (
			CustomerID, /*NameStyle,*/ Title, FullName, /*MiddleName,*/
			/*Suffix,*/ CompanyName, SalesPerson, EmailAddress, Phone/*,
			PasswordHash, PasswordSalt, rowguid, ModifiedDate*/
		)
		SELECT
			C.CustomerID, /*C.NameStyle,*/ C.Title, (C.FirstName + ' ' + C.LastName)  AS FullName, /*MiddleName,*/
			/*C.Suffix,*/ C.CompanyName, C.SalesPerson, C.EmailAddress, C.Phone/*,
			C.PasswordHash, C.PasswordSalt, C.rowguid, C.ModifiedDate*/
		FROM bronze.Customer C;

		UPDATE bronze.Customer SET is_processed = 1;

		PRINT 'DATA HAS BEEN INSERTED INTO silver.Customer FROM bronze.Customer';

		-- 2. Create Address table with primary key
		DROP TABLE IF EXISTS silver.Address;
		CREATE TABLE silver.Address (
			AddressID int NOT NULL PRIMARY KEY,
			AddressLine1 nvarchar(60) NOT NULL,
			AddressLine2 nvarchar(60),
			City nvarchar(30) NOT NULL,
			StateProvince nvarchar(50) NOT NULL,
			CountryRegion nvarchar(50) NOT NULL,
			PostalCode nvarchar(15) NOT NULL,
			--rowguid uniqueidentifier NOT NULL,
			--ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'bronze.Address',
			is_processed BIT DEFAULT 0
		);

		INSERT INTO silver.Address (
			AddressID, AddressLine1, AddressLine2, City,
			StateProvince, CountryRegion, PostalCode--, rowguid, ModifiedDate
		)
		SELECT 
			AddressID, AddressLine1, AddressLine2, City,
			StateProvince, CountryRegion, PostalCode--, rowguid, ModifiedDate
		FROM bronze.Address;

		UPDATE bronze.Address SET is_processed = 1;

		PRINT 'DATA HAS BEEN INSERTED INTO silver.Address FROM bronze.Address';

		-- 3. Create CustomerAddress table with composite primary key and foreign keys
		DROP TABLE IF EXISTS silver.CustomerAddress;
		CREATE TABLE silver.CustomerAddress (
			CustomerID int NOT NULL,
			AddressID int NOT NULL,
			--AddressType nvarchar(50) NOT NULL,
			--rowguid uniqueidentifier NOT NULL,
			--ModifiedDate datetime NOT NULL,
			source_file NVARCHAR(255) DEFAULT 'bronze.CustomerAddress',
			is_processed BIT DEFAULT 0,
			PRIMARY KEY (CustomerID, AddressID)
		);

		-- Add foreign key to Customer table
		ALTER TABLE silver.CustomerAddress
		ADD CONSTRAINT FK_CustomerAddress_Customer
			FOREIGN KEY (CustomerID) 
			REFERENCES silver.Customer(CustomerID);

		-- Add foreign key to Address table
		ALTER TABLE silver.CustomerAddress
		ADD CONSTRAINT FK_CustomerAddress_Address
			FOREIGN KEY (AddressID) 
			REFERENCES silver.Address(AddressID);

		INSERT INTO silver.CustomerAddress(
			CustomerID, AddressID--, AddressType, rowguid, ModifiedDate
		)
		SELECT CustomerID, AddressID--, AddressType, rowguid, ModifiedDate 
		FROM bronze.CustomerAddress;

		UPDATE bronze.CustomerAddress SET is_processed = 1;


		PRINT 'DATA HAS BEEN INSERTED INTO silver.CustomerAddress FROM bronze.CustomerAddress';

		-- 4. Create indexes for better performance
		CREATE INDEX IX_Customer_Email ON silver.Customer(EmailAddress);
		CREATE INDEX IX_Address_PostalCode ON silver.Address(PostalCode);
		CREATE INDEX IX_CustomerAddress_CustomerID ON silver.CustomerAddress(CustomerID);
		CREATE INDEX IX_CustomerAddress_AddressID ON silver.CustomerAddress(AddressID);

		PRINT 'All tables created with proper primary and foreign keys';

		PRINT 'DATA HAS BEEN INSERTED INTO silver.Address FROM bronze.Address';


		-- 1. Create ProductCategory table
		DROP TABLE IF EXISTS silver.ProductCategory;
		CREATE TABLE silver.ProductCategory (
			ProductCategoryID int NOT NULL PRIMARY KEY,
			ParentProductCategoryID int,
			Name nvarchar(50),
			--rowguid uniqueidentifier,
			--ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'bronze.ProductCategory',
			is_processed BIT DEFAULT 0
		);

		-- 2. Create ProductModel table
		DROP TABLE IF EXISTS silver.ProductModel;
		CREATE TABLE silver.ProductModel (
			ProductModelID int NOT NULL PRIMARY KEY,
			Name nvarchar(50),
			CatalogDescription xml,
			--rowguid uniqueidentifier,
			--ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'bronze.ProductModel',
			is_processed BIT DEFAULT 0
		);

		-- 3. Create Product table
		DROP TABLE IF EXISTS silver.Product;
		CREATE TABLE silver.Product (
			ProductID int NOT NULL PRIMARY KEY,
			Name nvarchar(50),
			ProductNumber nvarchar(25),
			Color nvarchar(15),
			StandardCost money,
			ListPrice money,
			Size nvarchar(5),
			Weight decimal(8, 2),
			ProductCategoryID int,
			ProductModelID int,
			SellStartDate datetime,
			SellEndDate datetime,
			DiscontinuedDate datetime,
			--ThumbNailPhoto varbinary(max),
			--ThumbnailPhotoFileName nvarchar(50),
			--rowguid uniqueidentifier,
			--ModifiedDate datetime,
			source_file NVARCHAR(255) DEFAULT 'bronze.Product',
			is_processed BIT DEFAULT 0
		);

			-- 4. Insert data into ProductCategory
			INSERT INTO silver.ProductCategory (
				ProductCategoryID, ParentProductCategoryID, Name --,rowguid, ModifiedDate
			)
			SELECT
				ProductCategoryID, ParentProductCategoryID, Name --, rowguid, ModifiedDate
			FROM bronze.ProductCategory;

			UPDATE bronze.ProductCategory SET is_processed = 1;

			-- 5. Insert data into ProductModel
			INSERT INTO silver.ProductModel (
				ProductModelID, Name, CatalogDescription--, rowguid, ModifiedDate
			)
			SELECT
				ProductModelID, Name, CatalogDescription--, rowguid, ModifiedDate
			FROM bronze.ProductModel;

			UPDATE bronze.ProductModel SET is_processed = 1;

			-- 6. Insert data into Product
			INSERT INTO silver.Product (
				ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
				Size, Weight, ProductCategoryID, ProductModelID, SellStartDate,
				SellEndDate, DiscontinuedDate--, ThumbNailPhoto, ThumbnailPhotoFileName, rowguid, ModifiedDate
			)
			SELECT
				ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
				Size, Weight, ProductCategoryID, ProductModelID, SellStartDate,
				SellEndDate, DiscontinuedDate--, ThumbNailPhoto, ThumbnailPhotoFileName, rowguid, ModifiedDate
			FROM bronze.Product;

			UPDATE bronze.Product SET is_processed = 1;

			-- 7. Add foreign key constraints
			ALTER TABLE silver.Product
			ADD CONSTRAINT FK_Product_ProductCategory
				FOREIGN KEY (ProductCategoryID)
				REFERENCES silver.ProductCategory(ProductCategoryID);

			ALTER TABLE silver.Product
			ADD CONSTRAINT FK_Product_ProductModel
				FOREIGN KEY (ProductModelID)
				REFERENCES silver.ProductModel(ProductModelID);

			-- 8. Create basic indexes
			CREATE INDEX IX_Product_Category ON silver.Product(ProductCategoryID);
			CREATE INDEX IX_Product_Model ON silver.Product(ProductModelID);

			PRINT 'All tables created and data loaded successfully';

			-- Drop and create silver.SalesOrderHeader (parent table) first
			DROP TABLE IF EXISTS silver.SalesOrderHeader;

			CREATE TABLE silver.SalesOrderHeader (
				SalesOrderID int PRIMARY KEY,
				RevisionNumber tinyint,
				OrderDate datetime,
				DueDate datetime,
				ShipDate datetime,
				Status tinyint,
				OnlineOrderFlag bit,
				PurchaseOrderNumber nvarchar(25),
				AccountNumber nvarchar(15),
				CustomerID int,
				ShipToAddressID int,
				BillToAddressID int,
				ShipMethod nvarchar(50),
				CreditCardApprovalCode varchar(15),
				SubTotal money,
				TaxAmt money,
				Freight money,
				Comment nvarchar(max),
				rowguid uniqueidentifier ROWGUIDCOL,
				ModifiedDate datetime,
				source_file NVARCHAR(255) DEFAULT 'bronze.SalesOrderHeader',
				is_processed BIT DEFAULT 0
			);

			-- Drop and create silver.SalesOrderDetail (child table with FK)
			DROP TABLE IF EXISTS silver.SalesOrderDetail;

			CREATE TABLE silver.SalesOrderDetail (
				SalesOrderID int,
				SalesOrderDetailID int,
				OrderQty smallint,
				ProductID int,
				UnitPrice money,
				UnitPriceDiscount money,
				LineTotal numeric(38,6),
				ModifiedDate datetime,
				source_file NVARCHAR(255) DEFAULT 'bronze.SalesOrderDetail',
				is_processed BIT DEFAULT 0,
				CONSTRAINT PK_SalesOrderDetail PRIMARY KEY (SalesOrderID, SalesOrderDetailID),
				CONSTRAINT FK_SalesOrderDetail_SalesOrderHeader FOREIGN KEY (SalesOrderID)
					REFERENCES silver.SalesOrderHeader(SalesOrderID)
			);

			-- Insert into silver.SalesOrderHeader
			INSERT INTO silver.SalesOrderHeader (
				SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
				Status, OnlineOrderFlag, PurchaseOrderNumber, AccountNumber,
				CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
				CreditCardApprovalCode, SubTotal, TaxAmt, Freight,
				Comment, rowguid, ModifiedDate
			)
			SELECT
				SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
				Status, OnlineOrderFlag, PurchaseOrderNumber, AccountNumber,
				CustomerID, ShipToAddressID, BillToAddressID, ShipMethod,
				CreditCardApprovalCode, SubTotal, TaxAmt, Freight,
				Comment, rowguid, ModifiedDate
			FROM bronze.SalesOrderHeader;
			UPDATE bronze.SalesOrderHeader SET is_processed = 1;

			PRINT 'DATA HAS BEEN INSERTED INTO silver.SalesOrderHeader FROM bronze.SalesOrderHeader';

			-- Insert into silver.SalesOrderDetail
			INSERT INTO silver.SalesOrderDetail (
				SalesOrderID, SalesOrderDetailID, OrderQty,
				ProductID, UnitPrice, UnitPriceDiscount,
				LineTotal, ModifiedDate
			)
			SELECT
				SalesOrderID, SalesOrderDetailID, OrderQty,
				ProductID, UnitPrice, UnitPriceDiscount,
				LineTotal, ModifiedDate
			FROM bronze.SalesOrderDetail;
			UPDATE bronze.SalesOrderDetail SET is_processed = 1;

			PRINT 'DATA HAS BEEN INSERTED INTO silver.SalesOrderDetail FROM bronze.SalesOrderDetail';








		
		PRINT '----------------------------------------------------------------------------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '-------------------------       SILVER LAYER HAS BEEN CREATED     ----------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '----------------------------------------------------------------------------------------------';

	END;
GO

CREATE OR ALTER PROCEDURE gold_layer
	AS
	BEGIN
	
		DROP TABLE IF EXISTS gold.DimDate;
				-- DimDate
		CREATE TABLE gold.DimDate (
			DateKey INT PRIMARY KEY,
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
		-- Date dimension population
		DECLARE @StartDate DATE = '2005-01-01';
		DECLARE @EndDate DATE = '2025-12-31';

		WHILE @StartDate <= @EndDate
		BEGIN
			INSERT INTO gold.DimDate (
				DateKey,
				FullDate,
				DayNumberOfWeek,
				DayName,
				DayNumberOfMonth,
				DayNumberOfYear,
				WeekNumberOfYear,
				MonthName,
				MonthNumberOfYear,
				CalendarQuarter,
				CalendarYear,
				IsWeekend
			)
			VALUES (
				CONVERT(INT, CONVERT(VARCHAR(8), @StartDate, 112)), -- DateKey as yyyyMMdd
				@StartDate, -- FullDate
				DATEPART(WEEKDAY, @StartDate), -- DayNumberOfWeek
				DATENAME(WEEKDAY, @StartDate), -- DayName
				DATEPART(DAY, @StartDate), -- DayNumberOfMonth
				DATEPART(DAYOFYEAR, @StartDate), -- DayNumberOfYear
				DATEPART(WEEK, @StartDate), -- WeekNumberOfYear
				DATENAME(MONTH, @StartDate), -- MonthName
				DATEPART(MONTH, @StartDate), -- MonthNumberOfYear
				DATEPART(QUARTER, @StartDate), -- CalendarQuarter
				DATEPART(YEAR, @StartDate), -- CalendarYear
				CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END -- IsWeekend
			);
    
			SET @StartDate = DATEADD(DAY, 1, @StartDate);
		END;
			   
		DROP TABLE IF EXISTS gold.DimProduct;
		CREATE TABLE gold.DimProduct (
			ProductKey INT IDENTITY(1,1) PRIMARY KEY,
			ProductID INT NOT NULL,
			ProductName NVARCHAR(50) NOT NULL,
			ProductNumber NVARCHAR(25) NOT NULL,
			Color NVARCHAR(15) NULL,
			StandardCost MONEY NOT NULL,
			ListPrice MONEY NOT NULL,
			Size NVARCHAR(5) NULL,
			Weight DECIMAL(8,2) NULL,
			ProductCategoryID INT NULL,
			ProductCategoryName NVARCHAR(50) NULL,
			ProductModelName NVARCHAR(50) NULL,
			source_file NVARCHAR(255) DEFAULT 'silver.Product && silver.ProductCategory && silver.ProductModel',
			time_to_create DATETIME2(7) NOT NULL,
			IsCurrent BIT NOT NULL DEFAULT 0
		);
		INSERT INTO gold.DimProduct (
				ProductID, ProductName, ProductNumber,Color,
				StandardCost,ListPrice,Size, Weight, ProductCategoryID,
				ProductCategoryName, ProductModelName, time_to_create, IsCurrent
		)
		SELECT 
				p.ProductID, p.Name, p.ProductNumber, p.Color,
				p.StandardCost, p.ListPrice, p.Size, p.Weight, p.ProductCategoryID,
				pc.Name AS ProductCategoryName, pm.Name AS ProductModelName,
				GETDATE() AS time_to_create, 1 AS IsCurrent
		FROM silver.Product as p
			LEFT JOIN silver.ProductCategory as pc ON p.ProductCategoryID = pc.ProductCategoryID
			LEFT JOIN silver.ProductModel as pm ON p.ProductModelID = pm.ProductModelID
		WHERE NOT EXISTS (
			SELECT 1 FROM gold.DimProduct dp 
				WHERE dp.ProductID = p.ProductID AND dp.IsCurrent = 1
			)
		OR EXISTS (
			SELECT 1 FROM gold.DimProduct dp 
				WHERE dp.ProductID = p.ProductID 
				AND dp.IsCurrent = 1
				AND (
					dp.ProductName <> p.Name
					OR dp.ProductNumber <> p.ProductNumber
					OR ISNULL(dp.Color, '') <> ISNULL(p.Color, '')
					OR dp.StandardCost <> p.StandardCost
					OR dp.ListPrice <> p.ListPrice
				)
		);		
		UPDATE silver.Product SET is_processed = 1;
		UPDATE silver.ProductCategory SET is_processed = 1;
		UPDATE silver.ProductModel SET is_processed = 1;


		DROP TABLE IF EXISTS gold.DimCustomer;
		-- DimCustomer (Type 2 SCD)
		CREATE TABLE gold.DimCustomer (
			CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
			CustomerID INT NOT NULL,
			CompanyName NVARCHAR(128) NULL,
			FullName NVARCHAR(150) NOT NULL,
			EmailAddress NVARCHAR(50) NULL,
			Phone NVARCHAR(25) NULL,
			AddressLine1 NVARCHAR(60) NULL,
			AddressLine2 NVARCHAR(60) NULL,
			City NVARCHAR(30) NULL,
			StateProvince NVARCHAR(50) NULL,
			CountryRegion NVARCHAR(50) NULL,
			PostalCode NVARCHAR(15) NULL,
			source_file NVARCHAR(255) DEFAULT 'silver.Customer && silver.CustomerAddress && silver.Address',
			time_to_create DATETIME2(7) NOT NULL,
			IsCurrent BIT NOT NULL DEFAULT 0
		);


		-- Insert new customers and update changed ones (SCD Type 2)
			INSERT INTO gold.DimCustomer (
				CustomerID,
				CompanyName,
				FullName,
				EmailAddress,
				Phone,
				AddressLine1,
				AddressLine2,
				City,
				StateProvince,
				CountryRegion,
				PostalCode,
				time_to_create,
				IsCurrent
			)
			SELECT 
				c.CustomerID,
				c.CompanyName,
				c.FullName,
				c.EmailAddress,
				c.Phone,
				a.AddressLine1,
				a.AddressLine2,
				a.City,
				a.StateProvince,
				a.CountryRegion,
				a.PostalCode,
				GETDATE() AS time_to_create,
				1 AS IsCurrent
			FROM silver.Customer c
			LEFT JOIN silver.CustomerAddress ca ON c.CustomerID = ca.CustomerID
			LEFT JOIN silver.Address a ON ca.AddressID = a.AddressID
			WHERE NOT EXISTS (
				SELECT 1 FROM gold.DimCustomer dc 
				WHERE dc.CustomerID = c.CustomerID AND dc.IsCurrent = 1
			)
			OR EXISTS (
				SELECT 1 FROM gold.DimCustomer dc 
				WHERE dc.CustomerID = c.CustomerID 
				AND dc.IsCurrent = 1
				AND (
					ISNULL(dc.CompanyName, '') <> ISNULL(c.CompanyName, '')
					OR dc.FullName <> c.FullName
					OR ISNULL(dc.EmailAddress, '') <> ISNULL(c.EmailAddress, '')
					OR ISNULL(dc.Phone, '') <> ISNULL(c.Phone, '')
					-- Add other fields to compare
				)
			);
			UPDATE silver.Customer SET is_processed = 1 ;
			UPDATE silver.CustomerAddress SET is_processed = 1 ;
			UPDATE silver.Address SET is_processed = 1 ;




			DROP table IF  EXISTS gold.FactSales;
			-- FactSales
			CREATE TABLE gold.FactSales (
				SalesOrderID INT NOT NULL,
				SalesOrderDetailID INT NOT NULL,
				CustomerKey INT NOT NULL,
				ProductKey INT NOT NULL,
				OrderDateKey INT NOT NULL,
				DueDateKey INT NOT NULL,
				ShipDateKey INT NULL,
				OrderQty SMALLINT NOT NULL,
				UnitPrice MONEY NOT NULL,
				UnitPriceDiscount MONEY NOT NULL,
				LineTotal MONEY NOT NULL,
				PRIMARY KEY (SalesOrderID, SalesOrderDetailID),
				CONSTRAINT FK_FactSales_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES gold.DimCustomer(CustomerKey),
				CONSTRAINT FK_FactSales_DimProduct FOREIGN KEY (ProductKey) REFERENCES gold.DimProduct(ProductKey),
				CONSTRAINT FK_FactSales_DimDate_OrderDate FOREIGN KEY (OrderDateKey) REFERENCES gold.DimDate(DateKey),
				CONSTRAINT FK_FactSales_DimDate_DueDate FOREIGN KEY (DueDateKey) REFERENCES gold.DimDate(DateKey),
				CONSTRAINT FK_FactSales_DimDate_ShipDate FOREIGN KEY (ShipDateKey) REFERENCES gold.DimDate(DateKey)
			);






	 
				-- Insert sales facts
				INSERT INTO gold.FactSales (
					SalesOrderID,
					SalesOrderDetailID,
					CustomerKey,
					ProductKey,
					OrderDateKey,
					DueDateKey,
					ShipDateKey,
					OrderQty,
					UnitPrice,
					UnitPriceDiscount,
					LineTotal
				)
				SELECT 
					sod.SalesOrderID,
					sod.SalesOrderDetailID,
					dc.CustomerKey,
					dp.ProductKey,
					CONVERT(INT, CONVERT(VARCHAR(8), so.OrderDate, 112)) AS OrderDlateKey,
					CONVERT(INT, CONVERT(VARCHAR(8), so.DueDate, 112)) AS DueDateKey,
					CONVERT(INT, CONVERT(VARCHAR(8), so.ShipDate, 112)) AS ShipDateKey,
					sod.OrderQty,
					sod.UnitPrice,
					sod.UnitPriceDiscount,
					sod.LineTotal
				FROM silver.SalesOrderDetail sod
				INNER JOIN silver.SalesOrderHeader so ON sod.SalesOrderID = so.SalesOrderID
				INNER JOIN gold.DimCustomer dc ON so.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
				INNER JOIN gold.DimProduct dp ON sod.ProductID = dp.ProductID AND dp.IsCurrent = 1;

				UPDATE silver.SalesOrderDetail SET is_processed =1;
				UPDATE silver.SalesOrderHeader SET is_processed =1;
				PRINT '----------------------------------------------------------------------------------------------';
				PRINT '-------------------------------------                    -------------------------------------';
				PRINT '-------------------------       GOLD LAYER HAS BEEN CREATED       ----------------------------';
				PRINT '-------------------------------------                    -------------------------------------';
				PRINT '----------------------------------------------------------------------------------------------';

	END;
go

CREATE OR ALTER PROCEDURE ETL_START_SCHEMA
	AS
	BEGIN

		EXEC bronze_layer_1;
		EXEC silver_layer;
		EXEC gold_layer;

		PRINT ' ';
		PRINT ' ';
		PRINT ' ';
		PRINT '----------------------------------------------------------------------------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '-------------------------    DATA LAKEHOUSE HAS BEEN CREATED      ----------------------------';
		PRINT '-------------------------------------                    -------------------------------------';
		PRINT '----------------------------------------------------------------------------------------------';
	END;
GO


EXEC ETL_START_SCHEMA;

--SELECT * FROM bronze.Customer;
--SELECT * FROM bronze.Address;
--SELECT * FROM bronze.CustomerAddress;
--select * from gold.DimCustomer;
--SELECT * FROM bronze.Product;
--SELECT * FROM bronze.ProductCategory;
--SELECT * FROM bronze