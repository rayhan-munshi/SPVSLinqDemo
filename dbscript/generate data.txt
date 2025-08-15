-- Departments
CREATE TABLE Departments (
    Id INT IDENTITY PRIMARY KEY,
    Name NVARCHAR(50)
);
INSERT INTO Departments (Name)
VALUES ('IT'), ('HR'), ('Finance'), ('Marketing'), ('Sales');

-- Employees
CREATE TABLE Employees (
    Id INT IDENTITY PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    DepartmentId INT FOREIGN KEY REFERENCES Departments(Id)
);

-- Insert 100k Employees
INSERT INTO Employees (FirstName, LastName, DepartmentId)
SELECT TOP (100000)
    'First' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR),
    'Last' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR),
    (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 5) + 1
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;

-- Salaries (simulate monthly records for each employee)
CREATE TABLE Salaries (
    Id BIGINT IDENTITY PRIMARY KEY,
    EmployeeId INT FOREIGN KEY REFERENCES Employees(Id),
    SalaryAmount DECIMAL(18,2),
    PayDate DATE
);

-- Insert 1M+ Salary records
INSERT INTO Salaries (EmployeeId, SalaryAmount, PayDate)
SELECT 
    e.Id,
    50000 + (e.Id % 5000),
    DATEADD(MONTH, (n.number % 24), '2020-01-01')
FROM Employees e
CROSS JOIN master.dbo.spt_values n
WHERE n.type = 'P' AND n.number < 12;  -- 12 months each
GO

-- Stored Procedure
CREATE PROCEDURE GetLatestSalariesByDepartment
    @DeptName NVARCHAR(50)
AS
BEGIN
    SELECT e.Id, e.FirstName, e.LastName, d.Name AS Department, s.SalaryAmount, s.PayDate
    FROM Employees e
    INNER JOIN Departments d ON e.DepartmentId = d.Id
    INNER JOIN Salaries s ON s.EmployeeId = e.Id
    WHERE d.Name = @DeptName
      AND s.PayDate = (
          SELECT MAX(PayDate) FROM Salaries WHERE EmployeeId = e.Id
      );
END;
