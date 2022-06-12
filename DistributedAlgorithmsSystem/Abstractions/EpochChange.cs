using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class EpochChange {
    private readonly string _abstractionId;
    private readonly IPEndPoint _appEndPoint;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;
    private int _lastTimestamp;
    private int _timestamp;
    private ProcessId _trusted;

    public EpochChange(string abstractionId, IPEndPoint appEndPoint, Dictionary<IPEndPoint, ProcessId> processes,
        ILogger<App> logger, ChannelWriter<Message> eventQueueWriter) {
        _abstractionId = abstractionId;
        _appEndPoint = appEndPoint;
        _timestamp = processes[appEndPoint].Rank;
        _lastTimestamp = 0;
        _processes = processes;
        _logger = logger;
        _eventQueueWriter = eventQueueWriter;
        _trusted = MaxRank(processes);
    }

    public async Task EcInterpretMessage(Message message) {
        switch (message.Type) {
            case Message.Types.Type.EldTrust:
                await ReceivedTrust(message);
                break;
            case Message.Types.Type.BebDeliver:
                await InterpretBebDeliver(message);
                break;
            case Message.Types.Type.PlDeliver:
                await InterpretPlDeliver(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private async Task InterpretPlDeliver(Message message) {
        switch (message.PlDeliver.Message.Type) {
            case Message.Types.Type.EcInternalNack:
                await ReceiveAcknowledgement();
                break;
            default:
                _logger.LogWarning(
                    "Interpret Pl Deliver {AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private async Task ReceiveAcknowledgement() {
        if (_trusted.Host == _processes[_appEndPoint].Host && _trusted.Port == _processes[_appEndPoint].Port) {
            _timestamp +=  _processes.Count;
            await BroadcastNewEpoch();
        }
    }

    private async Task InterpretBebDeliver(Message message) {
        switch (message.BebDeliver.Message.Type) {
            case Message.Types.Type.EcInternalNewEpoch:
                await ReceivedNewEpoch(message);
                break;
            default:
                _logger.LogWarning(
                    "Interpret Beb Deliver {AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private async Task ReceivedNewEpoch(Message message) {
        var newTimestamp = message.BebDeliver.Message.EcInternalNewEpoch.Timestamp;

        if (message.BebDeliver.Sender.Host == _trusted.Host &&message.BebDeliver.Sender.Port == _trusted.Port && 
            newTimestamp > _lastTimestamp) {
            _lastTimestamp = newTimestamp;
            await _eventQueueWriter.WriteAsync(new Message {
                ToAbstractionId = _abstractionId.RemoveEndAbstraction(), FromAbstractionId = _abstractionId,
                Type = Message.Types.Type.EcStartEpoch,
                EcStartEpoch = new EcStartEpoch {
                    NewLeader = _trusted,
                    NewTimestamp = newTimestamp
                }
            });
        }
        else {
            await _eventQueueWriter.WriteAsync(new Message {
                ToAbstractionId = $"{_abstractionId}.pl", FromAbstractionId = _abstractionId,
                Type = Message.Types.Type.PlSend, PlSend = new PlSend {
                    Destination = message.BebDeliver.Sender,
                    Message = new Message {
                        ToAbstractionId = _abstractionId, FromAbstractionId = _abstractionId,
                        Type = Message.Types.Type.EcInternalNack, EcInternalNack = new EcInternalNack()
                    }
                }
            });
        }
    }

    private async Task ReceivedTrust(Message message) {
        _trusted = message.EldTrust.Process;
        if (_trusted.Host == _processes[_appEndPoint].Host && _trusted.Port == _processes[_appEndPoint].Port) {
            _timestamp += _processes[_appEndPoint].Rank;
            await BroadcastNewEpoch();
        }
    }

    private async Task BroadcastNewEpoch() {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.beb",
            Type = Message.Types.Type.BebBroadcast, BebBroadcast = new BebBroadcast {
                Message = new Message {
                    FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId,Type = Message.Types.Type.EcInternalNewEpoch,
                    EcInternalNewEpoch = new EcInternalNewEpoch {Timestamp = _timestamp}
                }
            }
        });
    }

    public static ProcessId MaxRank(Dictionary<IPEndPoint, ProcessId> processes) {
        var maxRank = -1;
        IPEndPoint trustedEndpoint = null!;
        foreach (var (endpoint, process) in processes) {
            if (process.Rank < maxRank)
                continue;
            maxRank = process.Rank;
            trustedEndpoint = endpoint;
        }

        return processes[trustedEndpoint];
    }
}