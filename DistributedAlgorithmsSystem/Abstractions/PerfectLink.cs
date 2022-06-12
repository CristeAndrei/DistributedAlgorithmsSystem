using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class PerfectLink {
    private readonly string _abstractionId;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly IPEndPoint _plEndPoint;
    private readonly string _systemId;

    public PerfectLink(string abstractionId, IPEndPoint plEndPoint,
        ChannelWriter<Message> eventQueueWriter,
        ILogger<App> logger, string systemId) {
        _abstractionId = abstractionId;
        _plEndPoint = plEndPoint;
        _eventQueueWriter = eventQueueWriter;
        _logger = logger;
        _systemId = systemId;
    }

    private async Task Deliver(Message message) {
        await _eventQueueWriter.WriteAsync(message);
    }

    private async Task Send(Message sendMessage, IPEndPoint destination) {
        if (sendMessage?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            sendMessage?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest)
            _logger.LogInformation("Message send by {Abstraction} at {EndPoint} to {Destination} is {Message}",
                _abstractionId, _plEndPoint, destination,
                sendMessage);
        await MessageSender.SendMessage(sendMessage, destination);
    }

    public async Task PlInterpretMessage(Message message) {
        Message? appMessage;
        switch (message.Type) {
            case Message.Types.Type.NetworkMessage:
                appMessage = InterpretNetworkMessage(message);
                if (appMessage is not null) await Deliver(appMessage);
                break;
            case Message.Types.Type.PlSend:
                (appMessage, var destination) = PlSendMessage(message);
                if (appMessage is not null) await Send(appMessage, destination);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _plEndPoint,
                    message);
                break;
        }
    }

    private Message? InterpretNetworkMessage(Message message) {
        var interpretedNetworkMessage =
            message.Type is Message.Types.Type.NetworkMessage ? PlDeliverMessage(message) : null;

        if (interpretedNetworkMessage is null)
            _logger.LogWarning(
                "{AbstractionId} on {EndPoint} could not deliver message {Message}",
                _abstractionId, _plEndPoint,
                message);

        return interpretedNetworkMessage;
    }

    private Message PlDeliverMessage(Message message) {
        return new() {
            ToAbstractionId = message.ToAbstractionId.RemoveEndAbstraction(),
            FromAbstractionId = _abstractionId, SystemId = message.SystemId,
            Type = Message.Types.Type.PlDeliver,
            PlDeliver = new PlDeliver {
                Sender = new ProcessId {
                    Host = message.NetworkMessage.SenderHost, Port = message.NetworkMessage.SenderListeningPort
                },
                Message = message.NetworkMessage.Message
            }
        };
    }


    private (Message? sendMessage, IPEndPoint destination) PlSendMessage(Message message) {
        var destination = new IPEndPoint(IPAddress.Parse(message.PlSend.Destination.Host),
            message.PlSend.Destination.Port);

        var interpretedPlSendMessage = message.Type is Message.Types.Type.PlSend
            ? new Message {
                MessageUuid = Guid.NewGuid().ToString(),
                FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId,
                SystemId = _systemId, Type = Message.Types.Type.NetworkMessage,
                NetworkMessage = new NetworkMessage {
                    Message = message.PlSend.Message, SenderListeningPort = _plEndPoint.Port,
                    SenderHost = _plEndPoint.Address.ToString()
                }
            }
            : null;

        if (interpretedPlSendMessage is null)
            _logger.LogWarning("Pl Send {Abstraction} on {EndPoint} could not interpret message {Message}",
                _abstractionId, _plEndPoint,
                message);


        return (interpretedPlSendMessage, destination);
    }
}