using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class BestEffortBroadcast {
    private readonly string _abstractionId;
    private readonly IPEndPoint _endPoint;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;

    public BestEffortBroadcast(string abstractionId, IPEndPoint endPoint, ChannelWriter<Message> eventQueueWriter,
        ILogger<App> logger, Dictionary<IPEndPoint, ProcessId> processes) {
        _abstractionId = abstractionId;
        _endPoint = endPoint;
        _eventQueueWriter = eventQueueWriter;
        _logger = logger;
        _processes = processes;
    }

    private async Task Broadcast(Message message) {
        foreach (var (_, process) in _processes) {
            var messageSendPl = new Message {
                Type = Message.Types.Type.PlSend, FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.pl",
                PlSend = new PlSend {
                    Message = message.BebBroadcast.Message,
                    Destination = process
                }
            };
            await _eventQueueWriter.WriteAsync(messageSendPl);
        }
    }

    private async Task Deliver(Message message) {
        await _eventQueueWriter.WriteAsync(message);
    }

    public async Task BebInterpretMessage(Message message) {

        switch (message.Type) {
            case Message.Types.Type.BebBroadcast:
                await Broadcast(message);
                break;
            case Message.Types.Type.PlDeliver:
                var appMessage = CreateBebDeliverMessage(message);
                await Deliver(appMessage);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}", _abstractionId,_endPoint,
                    message);
                break;
        }
    }

    private Message CreateBebDeliverMessage(Message message) =>
        new () {
            FromAbstractionId = _abstractionId,
            ToAbstractionId = message.ToAbstractionId.RemoveEndAbstraction(), SystemId = message.SystemId,
            Type = Message.Types.Type.BebDeliver,
            BebDeliver = new BebDeliver {Sender = message.PlDeliver.Sender, Message = message.PlDeliver.Message}
        };
}