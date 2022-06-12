using System.Net;

namespace DistributedAlgorithmsSystem.Helpers;

public static class ServiceCollectionExtensions {
    public static void CreateEventLoops(this IServiceCollection serviceCollection, string ip,
        string ports) {
        foreach (var port in ports.Split(';'))
            serviceCollection.AddSingleton<IHostedService>(provider => {
                var newLoop = provider.GetRequiredService<EventLoop>();
                newLoop.EndPoint = new IPEndPoint(IPAddress.Parse(ip), int.Parse(port));
                return newLoop;
            });
    }
}