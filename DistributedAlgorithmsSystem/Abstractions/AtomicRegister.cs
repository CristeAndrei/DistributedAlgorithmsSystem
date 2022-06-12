using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class AtomicRegister {
    private int _timestamp;
    private int _readId;
    private Value _value = new() {V = -1,Defined = false};
    private readonly Dictionary<IPEndPoint, (int, int, Value)> _readList = new();
    private int _acknowledgements;
    private bool _reading ;
    private Value _readValue = new() {V = -1,Defined = false};
    private Value _writeValue = new() {V = -1,Defined = false};
    private readonly string _abstractionId;
    private readonly ILogger<App> _logger;
    private readonly IPEndPoint _endPoint;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private  int _writerRank ;
    private readonly int _processesCount;
    private readonly int _appRank;

    public AtomicRegister(string abstractionId, ILogger<App> logger, IPEndPoint endPoint,
        ChannelWriter<Message> eventQueueWriter, int processesCount, int? appRank) {
        _abstractionId = abstractionId;
        _logger = logger;
        _endPoint = endPoint;
        _eventQueueWriter = eventQueueWriter;
        _processesCount = processesCount;
        _appRank = appRank ?? -1;
    }

    public async Task NnarInterpretMessage(Message message) {

        switch (message.Type) {
            case Message.Types.Type.PlDeliver:
                await AtomicRegisterPlDeliver(message);
                break;
            case Message.Types.Type.BebDeliver:
                await AtomicRegisterBebDeliver(message);
                break;
            case Message.Types.Type.NnarRead:
                await Read(message);
                break;
            case Message.Types.Type.NnarWrite:
                await Write(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on app {EndPoint} could not interpret message {Message}",
                    _abstractionId, _endPoint,
                    message);
                break;
        }
    }

    private async Task Write(Message message) {
        _readId++;
        _writeValue = message.NnarWrite.Value;
        _acknowledgements = 0;
        _readList.Clear();
        await _eventQueueWriter.WriteAsync(NnarInternalReadMessage(message));
    }

    private Message NnarInternalReadMessage(Message message) {
        return new Message() {
            Type = Message.Types.Type.BebBroadcast,
            FromAbstractionId = _abstractionId,
            ToAbstractionId = $"{_abstractionId}.beb",
            SystemId = message.SystemId,
            BebBroadcast = new BebBroadcast() {
                Message = new Message() {
                    FromAbstractionId = _abstractionId,
                    ToAbstractionId = _abstractionId,
                    SystemId = message.SystemId,
                    Type = Message.Types.Type.NnarInternalRead,
                    NnarInternalRead = new NnarInternalRead() {
                        ReadId = _readId
                    }
                }
            }
        };
    }

    private async Task AtomicRegisterPlDeliver(Message message) {
        switch (message.PlDeliver.Message.Type) {
            case Message.Types.Type.NnarInternalValue:
                await AtomicRegisterPlDeliverValue(message);
                break;
            case Message.Types.Type.NnarInternalAck:
                await AcknowledgeWrite(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on {EndPoint} could not interpret PlDeliver message {Message}",
                    _abstractionId, _endPoint, message);
                break;
        }
    }

    private async Task AcknowledgeWrite(Message message) {
        if (message.PlDeliver.Message.NnarInternalAck.ReadId != _readId) {
            await _eventQueueWriter.WriteAsync(message);
            return;
        }

        _acknowledgements++;
        if (_acknowledgements > _processesCount / 2) {
            _acknowledgements = 0;
            if (_reading) {
                _reading = false;
                await _eventQueueWriter.WriteAsync(new Message {
                    Type = Message.Types.Type.NnarReadReturn, SystemId = message.SystemId,
                    ToAbstractionId = "app",
                    FromAbstractionId = _abstractionId,
                    NnarReadReturn = new NnarReadReturn {
                        Value = _readValue
                    },
                });
            }
            else
                await _eventQueueWriter.WriteAsync(new Message {
                    Type = Message.Types.Type.NnarWriteReturn, SystemId = message.SystemId,
                    ToAbstractionId = "app",
                    FromAbstractionId = _abstractionId,
                    NnarWriteReturn = new NnarWriteReturn(){}
                    });
        }
    }

    private async Task AtomicRegisterBebDeliver(Message message) {
        switch (message.BebDeliver.Message.Type) {
            case Message.Types.Type.NnarInternalRead:
                await SendInternalValueMessage(message);
                break;
            case Message.Types.Type.NnarInternalWrite:
                await WriteAndSendAck(message);
                break;
            default:
                _logger.LogWarning("{AbstractionId} on {EndPoint} could not beb deliver message {Message}",
                    _abstractionId, _endPoint, message);
                break;
        }
    }

    private async Task WriteAndSendAck(Message message) {
        if (message.BebDeliver.Message.NnarInternalWrite.Timestamp > _timestamp || (message.BebDeliver.Message.NnarInternalWrite.Timestamp > _timestamp &&
            message.BebDeliver.Message.NnarInternalWrite.WriterRank > _writerRank)) {
            (_timestamp, _writerRank, _value) = (message.BebDeliver.Message.NnarInternalWrite.Timestamp,
                message.BebDeliver.Message.NnarInternalWrite.WriterRank,
                message.BebDeliver.Message.NnarInternalWrite.Value);
        }

        await _eventQueueWriter.WriteAsync(new Message() {
            Type = Message.Types.Type.PlSend, SystemId = message.SystemId, ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            PlSend = new PlSend() {
                Message = new Message() {
                    Type = Message.Types.Type.NnarInternalAck, SystemId = message.SystemId,
                    ToAbstractionId = $"{_abstractionId}.pl", FromAbstractionId = _abstractionId,
                    NnarInternalAck = new NnarInternalAck() {
                        ReadId = message.BebDeliver.Message.NnarInternalWrite.ReadId,
                    }
                },
                Destination = message.BebDeliver.Sender
            }
        });
    }

    private async Task SendInternalValueMessage(Message message) =>
        await _eventQueueWriter.WriteAsync(new Message() {
            Type = Message.Types.Type.PlSend, SystemId = message.SystemId, ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            PlSend = new PlSend() {
                Message = new Message() {
                    Type = Message.Types.Type.NnarInternalValue, SystemId = message.SystemId,
                    ToAbstractionId = $"{_abstractionId}.pl", FromAbstractionId = _abstractionId,
                    NnarInternalValue = new NnarInternalValue() {
                        Timestamp = _timestamp,
                        Value = _value,
                        ReadId = message.BebDeliver.Message.NnarInternalRead.ReadId,
                        WriterRank = _writerRank
                    }
                },
                Destination = message.BebDeliver.Sender
            }
        });


    private async Task AtomicRegisterPlDeliverValue(Message message) {
        if (message.PlDeliver.Message.NnarInternalValue.ReadId != _readId) {
            await _eventQueueWriter.WriteAsync(message);
            return;
        }

        var senderEndPoint =
            new IPEndPoint(IPAddress.Parse(message.PlDeliver.Sender.Host), message.PlDeliver.Sender.Port);

        _readList.Add(senderEndPoint,
            (message.PlDeliver.Message.NnarInternalValue.Timestamp,
                message.PlDeliver.Message.NnarInternalValue.WriterRank,
                message.PlDeliver.Message.NnarInternalValue.Value));

        if (_readList.Count > _processesCount / 2) {
            (var maxTimestamp,var maxRank, _readValue) = FindMaxRead();
            _readList.Clear();
            if (_reading)
                await _eventQueueWriter.WriteAsync(new Message() {
                    Type = Message.Types.Type.BebBroadcast, FromAbstractionId = _abstractionId,
                    ToAbstractionId = $"{_abstractionId}.beb", SystemId = message.SystemId,
                    BebBroadcast = new BebBroadcast() {
                        Message = new Message() {
                            Type = Message.Types.Type.NnarInternalWrite, FromAbstractionId = _abstractionId,
                            ToAbstractionId = _abstractionId, SystemId = message.SystemId,
                            NnarInternalWrite = new NnarInternalWrite() {
                                Timestamp = maxTimestamp,
                                ReadId = _readId,
                                WriterRank = maxRank,
                                Value = _readValue
                            }
                        }
                    }
                });
            else
                await _eventQueueWriter.WriteAsync(new Message() {
                    Type = Message.Types.Type.BebBroadcast, FromAbstractionId = _abstractionId,
                    ToAbstractionId = $"{_abstractionId}.beb", SystemId = message.SystemId,
                    BebBroadcast = new BebBroadcast() {
                        Message = new Message() {
                            Type = Message.Types.Type.NnarInternalWrite, FromAbstractionId = _abstractionId,
                            ToAbstractionId = _abstractionId, SystemId = message.SystemId,
                            NnarInternalWrite = new NnarInternalWrite() {
                                Timestamp = maxTimestamp + 1,
                                ReadId = _readId,
                                WriterRank = _appRank,
                                Value = _writeValue
                            }
                        }
                    }
                });
        }
    }

    private (int, int, Value) FindMaxRead() {
        var (maxTimestamp, maxRank, readValue) = (0, 0, new Value() {V = -1,Defined = false});
        foreach (var (_, (ts, mr, v)) in _readList) {
            if (ts > maxTimestamp)
                (maxTimestamp, maxRank, readValue) = (ts, mr, v);
            else if (ts == maxTimestamp && mr > maxRank)
                (maxTimestamp, maxRank, readValue) = (ts, mr, v);
        }

        return (maxTimestamp, maxRank, readValue);
    }

    private async Task Read(Message message) {
        _readId++;
        _acknowledgements = 0;
        _readList.Clear();
        _reading = true;

        await _eventQueueWriter.WriteAsync(NnarInternalReadMessage(message));
    }
}