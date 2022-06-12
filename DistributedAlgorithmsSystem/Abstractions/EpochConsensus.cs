using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class EpochConsensus {
    private readonly string _abstractionId;
    private readonly IPEndPoint _appEndPoint;
    private readonly int _ets;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes;
    private readonly Dictionary<ProcessId, EpInternalState> _states ;
    private int _accepted;
    private Value _tmpValue;
    private Value _value;
    private int _valueTimestamp;
    private bool _halt;

    public EpochConsensus(string abstractionId, IPEndPoint appEndPoint, EpInternalState state,
        ChannelWriter<Message> eventQueueWriter, ILogger<App> logger, int ets,
        Dictionary<IPEndPoint, ProcessId> processes) {
        _abstractionId = abstractionId;
        _appEndPoint = appEndPoint;
        _eventQueueWriter = eventQueueWriter;
        _logger = logger;
        
        _value = state.Value;
        _valueTimestamp = state.ValueTimestamp;
        _tmpValue = new Value() {Defined = false};

        _states = new Dictionary<ProcessId, EpInternalState>();
        _accepted = 0;
        
        _ets = ets;
        _processes = processes;
    }


    public async Task EpInterpretMessage(Message message) {
        if(_halt) return;
        
        switch (message.Type) {
            case Message.Types.Type.EpPropose:
                await ReceivePropose(message);
                break;
            case Message.Types.Type.EpAbort:
                await AbortEpoch();
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
            case Message.Types.Type.EpInternalState:
                _states[message.PlDeliver.Sender] = message.PlDeliver.Message.EpInternalState;
                await CheckStates();
                break;
            case Message.Types.Type.EpInternalAccept:
                _accepted++;
                await CheckAccepted();
                break;
            default:
                _logger.LogWarning(
                    "Interpret Pl Deliver {AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private async Task CheckAccepted() {
        if (_accepted <= _processes.Count / 2) return;
        _accepted = 0;
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.beb",
            Type = Message.Types.Type.BebBroadcast, BebBroadcast = new BebBroadcast {
                Message = new Message {
                    ToAbstractionId = _abstractionId, FromAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpInternalDecided, EpInternalDecided = new EpInternalDecided {
                        Value = _tmpValue
                    }
                }
            }
        });
    }

    private async Task InterpretBebDeliver(Message message) {
        switch (message.BebDeliver.Message.Type) {
            case Message.Types.Type.EpInternalRead:
                await Read(message);
                break;
            case Message.Types.Type.EpInternalWrite:
                await BebReceiveWrite(message);
                break;
            case Message.Types.Type.EpInternalDecided:
                await Decide(message);
                break;
            default:
                _logger.LogWarning(
                    "Interpret Beb Deliver {AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _appEndPoint, message);
                break;
        }
    }

    private async Task Decide(Message message) {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId.RemoveEndAbstraction(),
            Type = Message.Types.Type.EpDecide,
            EpDecide = new EpDecide {Value = message.BebDeliver.Message.EpInternalDecided.Value, Ets = _ets}
        });
    }

    private async Task BebReceiveWrite(Message message) {
        _valueTimestamp = _ets;
        _value = message.BebDeliver.Message.EpInternalWrite.Value;
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.pl",
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = message.BebDeliver.Sender,
                Message = new Message {
                    FromAbstractionId = _abstractionId,
                    ToAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpInternalAccept,
                    EpInternalAccept = new EpInternalAccept()
                }
            }
        });
    }

    private async Task Read(Message message) {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.pl",
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = message.BebDeliver.Sender,
                Message = new Message {
                    ToAbstractionId = _abstractionId, FromAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpInternalState,
                    EpInternalState = new EpInternalState {Value = _value, ValueTimestamp = _valueTimestamp}
                }
            }
        });
    }

    private async Task AbortEpoch() {
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId.RemoveEndAbstraction(),
            Type = Message.Types.Type.EpAborted,
            EpAborted = new EpAborted {Value = _value, ValueTimestamp = _valueTimestamp, Ets = _ets}
        });
        _halt = true;
    }

    private async Task ReceivePropose(Message message) {
        _tmpValue = message.EpPropose.Value;
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.beb",
            Type = Message.Types.Type.BebBroadcast, BebBroadcast = new BebBroadcast {
                Message = new Message {
                    ToAbstractionId = _abstractionId, FromAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpInternalRead, EpInternalRead = new EpInternalRead()
                }
            }
        });
    }

    private async Task CheckStates() {
        if (_states.Count <= _processes.Count / 2) return;
        var (ts, v) = Highest(_states);
        if (v.Defined)
            _tmpValue = v;
        _states.Clear();
        await _eventQueueWriter.WriteAsync(new Message {
            FromAbstractionId = _abstractionId, ToAbstractionId = $"{_abstractionId}.beb",
            Type = Message.Types.Type.BebBroadcast, BebBroadcast = new BebBroadcast {
                Message = new Message {
                    FromAbstractionId = _abstractionId, ToAbstractionId = _abstractionId,
                    Type = Message.Types.Type.EpInternalWrite,
                    EpInternalWrite = new EpInternalWrite {Value = _tmpValue}
                }
            }
        });
    }

    private (int, Value) Highest(Dictionary<ProcessId, EpInternalState> states) {
        var (maxTs, val) = (-1, new Value {V = -1, Defined = false});
        foreach (var (_, state) in states) {
            if (maxTs < state.ValueTimestamp)
                (maxTs, val) = (state.ValueTimestamp,
                    state.Value);
            else if (maxTs == state.ValueTimestamp && val.V < state.Value.V)
                (maxTs, val) = (state.ValueTimestamp,
                    state.Value);
        }

        return (maxTs, val);
    }
}