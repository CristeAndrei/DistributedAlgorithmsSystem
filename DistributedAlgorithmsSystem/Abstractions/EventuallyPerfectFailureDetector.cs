using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class EventuallyPerfectFailureDetector {
    private readonly string _abstractionId;
    private readonly Dictionary<IPEndPoint, ProcessId> _alive = new();

    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;
    private readonly Dictionary<IPEndPoint, ProcessId> _suspected = new();
    private int _timer;

    public EventuallyPerfectFailureDetector(string abstractionId, Dictionary<IPEndPoint, ProcessId> processes,
        ILogger<App> logger,
        ChannelWriter<Message> eventQueueWriter) {
        _abstractionId = abstractionId;
        _processes = processes;
        _logger = logger;
        _eventQueueWriter = eventQueueWriter;
        foreach (var (endPoint, process) in processes) _alive.Add(endPoint, process);
        _timer = 100;
        Task.Run(StartTimer);
    }

    private IPEndPoint AppEndPoint { get; } = null!;

    public async Task EpfdInterpretMessage(Message message) {
        switch (message.Type) {
            case Message.Types.Type.PlDeliver:
                await InterpretPlDeliver(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, AppEndPoint, message);
                break;
        }
    }

    private async Task InterpretPlDeliver(Message message) {
        switch (message.PlDeliver.Message.Type) {
            case Message.Types.Type.EpfdInternalHeartbeatReply:
                ReceivedHeartbeatReplay(message);
                break;
            case Message.Types.Type.EpfdInternalHeartbeatRequest:
                await ReceiveHeartbeatRequest(message);
                break;
            default:
                _logger.LogWarning(
                    "Interpret PlDeliver {AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, AppEndPoint, message);
                break;
        }
    }

    private async Task ReceiveHeartbeatRequest(Message message) {
        var destinationEndPoint =
            new IPEndPoint(IPAddress.Parse(message.PlDeliver.Sender.Host), message.PlDeliver.Sender.Port);
        
        await _eventQueueWriter.WriteAsync(new Message {
            Type = Message.Types.Type.PlSend, FromAbstractionId = _abstractionId,
            ToAbstractionId = $"{_abstractionId}.pl",
            PlSend = new PlSend {
                Destination = _processes[destinationEndPoint], Message = new Message {
                    ToAbstractionId = _abstractionId, FromAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpfdInternalHeartbeatReply,
                    EpfdInternalHeartbeatReply = new EpfdInternalHeartbeatReply()
                }
            }
        });
    }

    private async Task StartTimer() {
            await Task.Delay(_timer);
            await Timeout();
    }

    private async Task Timeout() {
        if (_alive.Intersect(_suspected).Count() is not 0)
            _timer += 100;
        foreach (var (endPoint, process) in _processes) {
            if (!_alive.ContainsKey(endPoint) && !_suspected.ContainsKey(endPoint)) {
                _suspected[endPoint] = process;
                await Suspect(process);
            }
            else if (_alive.ContainsKey(endPoint) && _suspected.ContainsKey(endPoint)) {
                _suspected.Remove(endPoint);
                await Restore(process);
            }

            await SendHeartbeatRequest(process);
        }
        _alive.Clear();

        _ = Task.Run(StartTimer);
    }

    private async Task SendHeartbeatRequest(ProcessId process) {
        await _eventQueueWriter.WriteAsync(new Message {
            Type = Message.Types.Type.PlSend, FromAbstractionId = _abstractionId,
            ToAbstractionId = $"{_abstractionId}.pl",
            PlSend = new PlSend {
                Destination = process, Message = new Message {
                    Type = Message.Types.Type.EpfdInternalHeartbeatRequest,
                    ToAbstractionId = _abstractionId,
                    FromAbstractionId = _abstractionId,
                    EpfdInternalHeartbeatRequest = new EpfdInternalHeartbeatRequest()
                }
            }
        });
    }

    private async Task Restore(ProcessId process) {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId.RemoveEndAbstraction()}",
            Type = Message.Types.Type.EpfdRestore, EpfdRestore = new EpfdRestore {Process = process}
        });
    }

    private async Task Suspect(ProcessId process) {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId.RemoveEndAbstraction()}",
            Type = Message.Types.Type.EpfdSuspect, EpfdSuspect = new EpfdSuspect {Process = process}
        });
    }

    private void ReceivedHeartbeatReplay(Message message) {
        var senderEndPoint =
            new IPEndPoint(IPAddress.Parse(message.PlDeliver.Sender.Host), message.PlDeliver.Sender.Port);
        
        _alive[senderEndPoint] = _processes[senderEndPoint];
    }
}