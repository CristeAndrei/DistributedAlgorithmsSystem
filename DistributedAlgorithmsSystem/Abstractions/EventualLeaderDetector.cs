using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class EventualLeaderDetector {
    private readonly string _abstractionId;
    private readonly IPEndPoint _appEndPoint;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;
    private readonly Dictionary<IPEndPoint, ProcessId> _suspected = new();
    private ProcessId _leader;

    public EventualLeaderDetector(string abstractionId, IPEndPoint appEndPoint, ILogger<App> logger,
        ChannelWriter<Message> eventQueueWriter, Dictionary<IPEndPoint, ProcessId> processes) {
        _abstractionId = abstractionId;
        _appEndPoint = appEndPoint;
        _leader = null!;
        _logger = logger;
        _eventQueueWriter = eventQueueWriter;
        _processes = processes;
    }

    public async Task EldInterpretMessage(Message message) {
        switch (message.Type) {
            case Message.Types.Type.EpfdSuspect:
                await Suspect(message);
                break;
            case Message.Types.Type.EpfdRestore:
               await Restore(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private bool DecideLeader(Dictionary<IPEndPoint, ProcessId> processes,
        Dictionary<IPEndPoint, ProcessId> suspected) {
        var alive = processes.Except(suspected);
        var maxRank = -1;
        ProcessId? leader = null;
        foreach (var (_, process) in alive) {
            if (process.Rank <= maxRank)
                continue;
            maxRank = process.Rank;
            leader = process;
        }

        if (leader is null)
            return false;
        
        _leader = leader;
        return true;
    }

    private async Task Trust() {
        await _eventQueueWriter.WriteAsync(new Message {
            ToAbstractionId = _abstractionId.RemoveEndAbstraction(),
            FromAbstractionId = _abstractionId,
            Type = Message.Types.Type.EldTrust,
            EldTrust = new EldTrust {Process = _leader}
        });
    }

    private async Task Restore(Message message) {
        var processEndPoint = new IPEndPoint(IPAddress.Parse(message.EpfdRestore.Process.Host),
            message.EpfdRestore.Process.Port);
        _suspected.Remove(processEndPoint);
        
        var newLeader = DecideLeader(_processes, _suspected);
        if(newLeader) await Trust();
    }

    private async Task Suspect(Message message) {
        var processEndPoint = new IPEndPoint(IPAddress.Parse(message.EpfdSuspect.Process.Host),
            message.EpfdSuspect.Process.Port);
        _suspected[processEndPoint] = message.EpfdSuspect.Process;
        
        var newLeader = DecideLeader(_processes, _suspected);
        if(newLeader) await Trust();
    }
    
}