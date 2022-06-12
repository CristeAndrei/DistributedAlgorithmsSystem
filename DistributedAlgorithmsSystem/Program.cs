using System.Net;
using DistributedAlgorithmsSystem;
using DistributedAlgorithmsSystem.Helpers;
using Microsoft.AspNetCore.Connections;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddCommandLine(args);

builder.Services.AddLogging()
    .AddSingleton<EventQueue>()
    .AddTransient<EventLoop>()
    .AddSingleton<AppsManager>()
    .CreateEventLoops(builder.Configuration.GetValue<string>("ip"), builder.Configuration.GetValue<string>("ports"));

builder.WebHost.UseKestrel((context, options) => {
    var ip = IPAddress.Parse(context.Configuration.GetValue<string>("ip"));
    var ports = context.Configuration.GetValue<string>("ports").Split(';').Select(int.Parse);

    foreach (var port in ports)
        options.Listen(ip, port, listenOptions => { listenOptions.UseConnectionHandler<ListenerHandler>(); });
});

var app = builder.Build();

await app.Services.GetRequiredService<AppsManager>().OnInitAsync();

await app.RunAsync();