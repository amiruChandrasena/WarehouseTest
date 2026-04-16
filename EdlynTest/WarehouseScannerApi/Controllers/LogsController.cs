using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Linq;

[Route("api/[controller]")]
[ApiController]
public class LogsController : ControllerBase
{
    private readonly string _logDirectory;

    public LogsController(IConfiguration configuration)
    {
        // get from appsettings.json
        _logDirectory = configuration["Logging:LogFolder"];
    }

    // GET: api/logs
    [HttpGet]
    public IActionResult GetLogFiles()
    {
        if (!Directory.Exists(_logDirectory))
            return NotFound("Log directory not found");

        var files = Directory.GetFiles(_logDirectory, "*.txt")
                             .Select(f => new FileInfo(f))
                             .OrderByDescending(f => f.LastWriteTime)
                             .Select(f => new
                             {
                                 FileName = f.Name,
                                 Url = $"{Request.Scheme}://{Request.Host}/api/logs/{f.Name}",
                                 SizeInKB = (f.Length / 1024.0).ToString("F2"), // file size in KB
                                 LastModified = f.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
                             })
                             .ToList();

        return Ok(files);
    }

    // GET: api/logs/{fileName}
    [HttpGet("{fileName}")]
    public IActionResult GetLogFile(string fileName)
    {
        string filePath = Path.Combine(_logDirectory, fileName);

        if (!System.IO.File.Exists(filePath))
            return NotFound("File not found");

        string content = System.IO.File.ReadAllText(filePath);
        return Content(content, "text/plain");
    }
}
