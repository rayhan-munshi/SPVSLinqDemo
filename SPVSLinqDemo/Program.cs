using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Diagnostics;

public class Department
{
    public int Id { get; set; }
    public string Name { get; set; }
    public ICollection<Employee> Employees { get; set; }
}

public class Employee
{
    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public int DepartmentId { get; set; }
    public Department Department { get; set; }
    public ICollection<Salary> Salaries { get; set; }
}

public class Salary
{
    public long Id { get; set; }
    public int EmployeeId { get; set; }
    public decimal SalaryAmount { get; set; }
    public DateTime PayDate { get; set; }
    public Employee Employee { get; set; }
}

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<Employee> Employees { get; set; }
    public DbSet<Department> Departments { get; set; }
    public DbSet<Salary> Salaries { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseSqlServer("Server=.;Database=Demodb;Trusted_Connection=True;TrustServerCertificate=True");
}
public class BenchmarkRunner
{
    private readonly AppDbContext _context;
    private readonly string _connectionString;

    public BenchmarkRunner(AppDbContext context, string connectionString)
    {
        _context = context;
        _connectionString = connectionString;
    }

    public void Run(string departmentName)
    {
        OriginalLinq(departmentName);
        OptimizedLinq(departmentName);
        StoredProc(departmentName);

    }

    private void OriginalLinq(string departmentName)
    {
        var sw = Stopwatch.StartNew();
        var result = _context.Employees
            .Where(e => e.Department.Name == departmentName).OrderBy(e=>e.FirstName)
            .Select(e => new
            {
                e.Id,
                e.FirstName,
                e.LastName,
                Department = e.Department.Name,
                LatestSalary = e.Salaries
                    .OrderByDescending(s => s.PayDate)
                    .Select(s => new { s.SalaryAmount, s.PayDate })
                    .FirstOrDefault()
            })
            .ToList();
        Console.WriteLine($"{result.FirstOrDefault().FirstName}");
        sw.Stop();
        Console.WriteLine($"Original LINQ: {result.Count} records in {sw.ElapsedMilliseconds} ms");
    }

    private void OptimizedLinq(string departmentName)
    {
        var sw = Stopwatch.StartNew();
        var query =
            from e in _context.Employees
            join d in _context.Departments on e.DepartmentId equals d.Id
            where d.Name == departmentName
            join s in _context.Salaries on e.Id equals s.EmployeeId
            join latest in
                (
                    from s2 in _context.Salaries
                    group s2 by s2.EmployeeId into g
                    select new
                    {
                        EmployeeId = g.Key,
                        LatestPayDate = g.Max(x => x.PayDate)
                    }
                ) on new { s.EmployeeId, s.PayDate } equals new { EmployeeId = latest.EmployeeId, PayDate = latest.LatestPayDate }
            select new
            {
                e.Id,
                e.FirstName,
                e.LastName,
                Department = d.Name,
                s.SalaryAmount,
                s.PayDate
            };

        var result = query.OrderBy(e=>e.FirstName).ToList();
        Console.WriteLine($"{result.FirstOrDefault().FirstName}");
        sw.Stop();
        Console.WriteLine($"Optimized LINQ: {result.Count} records in {sw.ElapsedMilliseconds} ms");
    }

    private void StoredProc(string departmentName)
    {
        var sw = Stopwatch.StartNew();
        var list = new List<dynamic>();
        using (var conn = new SqlConnection(_connectionString))
        using (var cmd = new SqlCommand("GetLatestSalariesByDepartment", conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@DeptName", departmentName);
            conn.Open();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    list.Add(new
                    {
                        Id = reader.GetInt32(0),
                        FirstName = reader.GetString(1),
                        LastName = reader.GetString(2),
                        Department = reader.GetString(3),
                        Salary = reader.GetDecimal(4),
                        PayDate = reader.GetDateTime(5)
                    });
                }
            }
        }
        
        Console.WriteLine($"{list.FirstOrDefault().FirstName}");
        sw.Stop();
        Console.WriteLine($"Stored Procedure: {list.Count} records in {sw.ElapsedMilliseconds} ms");
    }
}
class Program
{
    static void Main()
    {
        var connectionString = "Server=.;Database=Demodb;Trusted_Connection=True;TrustServerCertificate=True";
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlServer(connectionString)
            .Options;

        using var context = new AppDbContext(options);
        var runner = new BenchmarkRunner(context, connectionString);

        runner.Run("Finance"); // test with IT department
    }
}