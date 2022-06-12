using DistributedAlgorithmsSystem.Protos;
using Microsoft.AspNetCore.Connections;

namespace DistributedAlgorithmsSystem;

public class ListenerHandler : ConnectionHandler {
    private readonly EventQueue _eventQueue;
    private readonly ILogger<ListenerHandler> _logger;

    public ListenerHandler(ILogger<ListenerHandler> logger, EventQueue eventQueue) {
        _logger = logger;
        _eventQueue = eventQueue;
    }

    public override async Task OnConnectedAsync(ConnectionContext connection) {
        var localPort = int.Parse(connection.LocalEndPoint?.ToString()?.Split(':')[1] ??
                                  throw new InvalidOperationException());
        var eventQueueWriter = _eventQueue.GetWriter(localPort);

        var stream = connection.Transport.Input.AsStream();
        var sizeData = new byte[4];
        _ = await stream.ReadAsync(sizeData.AsMemory(0, 4));

        Array.Reverse(sizeData);
        var messageData = new byte[BitConverter.ToInt32(sizeData)];
        _ = await stream.ReadAsync(messageData);

        var message = Message.Parser.ParseFrom(messageData);
        await eventQueueWriter.WriteAsync(message);
        if (message?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            message?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest 
           )
            _logger.LogInformation("Message Received to {LocalEndPoint} {message} from {RemoteEndPoint}",
                connection.LocalEndPoint, message, connection.RemoteEndPoint);
    }
}