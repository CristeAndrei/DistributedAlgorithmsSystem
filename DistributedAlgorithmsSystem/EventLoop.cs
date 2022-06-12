using System.Net;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem;

public class EventLoop : BackgroundService {
    private readonly AppsManager _appsManager;
    private readonly EventQueue _eventQueue;
    private readonly ILogger<EventLoop> _logger;

    public EventLoop(EventQueue eventQueue, ILogger<EventLoop> logger, AppsManager appsManager) {
        _logger = logger;
        _appsManager = appsManager;
        _eventQueue = eventQueue;
    }

    public IPEndPoint EndPoint { get; set; } = null!;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var eventQueueReader = _eventQueue.GetReader(EndPoint.Port);


        await foreach (var message in eventQueueReader.ReadAllAsync(stoppingToken)) {
            LogMessage(message);

            _appsManager.Apps[EndPoint].VerifyRequiredAbstractions(message.ToAbstractionId);

            var abstractionDestination = message.ToAbstractionId.GetEndAbstraction();
            switch (abstractionDestination) {
                case "app":
                    await _appsManager.Apps[EndPoint].AppInterpretMessage(message);
                    break;
                case "pl":
                    if (await ResendMessageIfAbstractionDoesntExists(message, abstractionDestination)) break;
                    await _appsManager.Apps[EndPoint].PerfectLinks[message.ToAbstractionId].PlInterpretMessage(message);
                    break;
                case "beb":
                    if (await ResendMessageIfAbstractionDoesntExists(message, abstractionDestination)) break;
                    await _appsManager.Apps[EndPoint].BestEffortBroadcasts[message.ToAbstractionId]
                        .BebInterpretMessage(message);
                    break;
                case "nnar":
                    await _appsManager.Apps[EndPoint].AtomicRegisters[message.ToAbstractionId]
                        .NnarInterpretMessage(message);
                    break;
                case "uc":
                    await _appsManager.Apps[EndPoint].UniformConsensus[message.ToAbstractionId]
                        .UcInterpretMessage(message);
                    break;
                case "ec":
                    await _appsManager.Apps[EndPoint].EpochChanges[message.ToAbstractionId]
                        .EcInterpretMessage(message);
                    break;
                case "eld":
                    await _appsManager.Apps[EndPoint].EventualLeaderDetectors[message.ToAbstractionId]
                        .EldInterpretMessage(message);
                    break;
                case "epfd":
                    await _appsManager.Apps[EndPoint].EventuallyPerfectFailureDetectors[message.ToAbstractionId]
                        .EpfdInterpretMessage(message);
                    break;
                case "ep":
                    if (await ResendMessageIfAbstractionDoesntExists(message, abstractionDestination)) break;
                    await _appsManager.Apps[EndPoint].EpochConsensuses[message.ToAbstractionId]
                        .EpInterpretMessage(message);
                    break;
                default:
                    _logger.LogWarning("Message {Message} could not be interpreted by the event loop", message);
                    break;
            }
        }
    }

    private void LogMessage(Message message) {
        if (message?.PlSend?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            message?.PlSend?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest &&
            message?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            message?.NetworkMessage?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest &&
            message?.PlDeliver?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            message?.PlDeliver?.Message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest &&
            message?.Type is not Message.Types.Type.EpfdInternalHeartbeatReply &&
            message?.Type is not Message.Types.Type.EpfdInternalHeartbeatRequest)
            _logger.LogInformation("Event loop on port {EndPoint} processing message {Message}", EndPoint, message);
    }

    private async Task<bool> ResendMessageIfAbstractionDoesntExists(Message message, string abstractionDestination) {
        var abstractionExists = abstractionDestination switch {
            "pl" =>
                _appsManager.Apps[EndPoint].PerfectLinks.ContainsKey(message.ToAbstractionId),
            "beb" =>
                _appsManager.Apps[EndPoint].BestEffortBroadcasts.ContainsKey(message.ToAbstractionId),
            "ep" =>
                _appsManager.Apps[EndPoint].EpochConsensuses.ContainsKey(message.ToAbstractionId),
            _ => false,
        };
        
        if (abstractionExists) return false;

        await _eventQueue.GetWriter(EndPoint.Port).WriteAsync(message);
        _logger.LogWarning(
            "Event loop on port {EndPoint} could not process message {Message}, {Abstraction} doesn't exists", EndPoint,
            message, message.ToAbstractionId);
        return true;
    }
}