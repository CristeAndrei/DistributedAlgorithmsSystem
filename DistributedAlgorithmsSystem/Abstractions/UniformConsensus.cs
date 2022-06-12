using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class UniformConsensus {
    private readonly string _abstractionId;
    private readonly IPEndPoint _appEndPoint;
    private readonly Action<EpInternalState, int, ProcessId, string> _createEp;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;
    private bool _decided;
    private int _ets;
    private ProcessId _leader;
    private int _newTs;
    private ProcessId _newLeader;
    private bool _proposed;
    private Value _val;

    public UniformConsensus(string abstractionId, IPEndPoint appEndPoint, Dictionary<IPEndPoint, ProcessId> processes,
        ILogger<App> logger, ChannelWriter<Message> eventQueueWriter,
        Action<EpInternalState, int, ProcessId, string> createEp) {
        _abstractionId = abstractionId;
        _appEndPoint = appEndPoint;
        _processes = processes;
        _logger = logger;
        _eventQueueWriter = eventQueueWriter;
        _createEp = createEp;

        _val = new Value() {Defined = false, V = -1};
        _proposed = false;
        _decided = false;
        
        _leader = EpochChange.MaxRank(processes);
        _ets = 0;

        _newTs = 0;
        _newLeader = null!;
        
        _createEp.Invoke(
            new EpInternalState {Value =  new Value() {Defined = false, V = -1}, ValueTimestamp = 0},
            _ets, _leader, _abstractionId);
    }

    public async Task UcInterpretMessage(Message message) {
        
        switch (message.Type) {
            case Message.Types.Type.UcPropose:
                _val = message.UcPropose.Value;
                await Propose();
                break;
            case Message.Types.Type.EcStartEpoch:
                await StartEpoch(message);
                break;
            case Message.Types.Type.EpAborted:
                await Aborted(message);
                break;
            case Message.Types.Type.EpDecide:
                await Decide(message);
                break;
            default:
                _logger.LogWarning("{AbstarctionId} on {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint,
                    message);
                break;
        }
    }

    private async Task Decide(Message message) {
        if (message.EpDecide.Ets != _ets) {
            await _eventQueueWriter.WriteAsync(message);
            return;
        }

        if (_decided is false) {
            _decided = true;
            await _eventQueueWriter.WriteAsync(new Message {
                FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId.RemoveEndAbstraction(),
                Type = Message.Types.Type.UcDecide, UcDecide = new UcDecide {Value = message.EpDecide.Value}
            });
        }
    }

    private async Task Aborted(Message message) {
        if (message.EpAborted.Ets != _ets) {
            await _eventQueueWriter.WriteAsync(message);
            return;
        }

        _ets = _newTs;
        _leader = _newLeader;
        _proposed = false;
        
        _createEp.Invoke(
            new EpInternalState {Value = message.EpAborted.Value, ValueTimestamp = message.EpAborted.ValueTimestamp},
            _ets, _leader, _abstractionId);

        await Propose();
    }

    private async Task StartEpoch(Message message) {
        _newTs = message.EcStartEpoch.NewTimestamp;
        _newLeader = message.EcStartEpoch.NewLeader;
        
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.ep[{_ets}]",
            Type = Message.Types.Type.EpAbort, EpAbort = new EpAbort()
        });
    }

    private async Task Propose() {
        if (_leader.Host == _processes[_appEndPoint].Host && _leader.Port == _processes[_appEndPoint].Port && _val.Defined && _proposed is false) {
            _proposed = true;
            await _eventQueueWriter.WriteAsync(new Message {
                FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.ep[{_ets}]",
                Type = Message.Types.Type.EpPropose, EpPropose = new EpPropose() {Value = _val}
            });
        }
    }
}