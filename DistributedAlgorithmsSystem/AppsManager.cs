using System.Net;
using DistributedAlgorithmsSystem.Abstractions;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem;

public class AppsManager {
    private readonly IPEndPoint _hub;
    private readonly ILogger<App> _logger;

    public AppsManager(IConfiguration configuration, EventQueue eventQueue,
        ILogger<App> logger) {
        _logger = logger;
        var owner = configuration.GetValue<string>("owner");
        var ip = IPAddress.Parse(configuration.GetValue<string>("ip"));
        var ports = configuration.GetValue<string>("ports").Split(';').Select(int.Parse).ToArray();
        var indexes = configuration.GetValue<string>("indexes").Split(';').Select(int.Parse).ToArray();
        _hub = new IPEndPoint(IPAddress.Parse(configuration.GetValue<string>("hubIp")),
            int.Parse(configuration.GetValue<string>("hubPort")));

        foreach (var (port, index) in ports.Zip(indexes)) {
            var endPoint = new IPEndPoint(ip, port);
            Apps.Add(endPoint, new App(endPoint, owner, index, eventQueue.GetWriter(port), _logger, _hub));
        }
    }

    public Dictionary<IPEndPoint, App> Apps { get; } = new();

    public async Task OnInitAsync() {
        foreach (var (endPoint, app) in Apps) {
            var message = new Message {
                Type = Message.Types.Type.NetworkMessage,
                NetworkMessage = new NetworkMessage {
                    SenderListeningPort = endPoint.Port, Message = new Message {
                        Type = Message.Types.Type.ProcRegistration,
                        ProcRegistration = new ProcRegistration {Index = app.Index, Owner = app.Owner}
                    }
                }
            };
            await MessageSender.SendMessage(message, _hub);
        }
    }
}