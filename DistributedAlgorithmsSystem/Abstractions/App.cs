using System.Net;
using System.Threading.Channels;
using DistributedAlgorithmsSystem.Helpers;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem.Abstractions;

public class App {
    private readonly string _abstractionId;
    private readonly IPEndPoint _appEndPoint;
    private readonly ChannelWriter<Message> _eventQueueWriter;
    private readonly IPEndPoint _hub;
    private readonly ILogger<App> _logger;
    private readonly Dictionary<IPEndPoint, ProcessId> _processes = new();
    private int? _rank;
    private string _systemId = "";

    public App(IPEndPoint appEndPoint, string owner, int index, ChannelWriter<Message> eventQueueWriter,
        ILogger<App> logger, IPEndPoint hub) {
        _abstractionId = "app";
        _eventQueueWriter = eventQueueWriter;
        _logger = logger;
        _hub = hub;
        Index = index;
        Owner = owner;
        _appEndPoint = appEndPoint;
        PerfectLinks["app.pl"] = new PerfectLink("app.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);
    }

    public int Index { get; }
    public string Owner { get; }

    public Dictionary<string, PerfectLink> PerfectLinks { get; } = new();
    public Dictionary<string, BestEffortBroadcast> BestEffortBroadcasts { get; } = new();
    public Dictionary<string, AtomicRegister> AtomicRegisters { get; } = new();
    public Dictionary<string, EventuallyPerfectFailureDetector> EventuallyPerfectFailureDetectors { get; } = new();

    public Dictionary<string, EventualLeaderDetector> EventualLeaderDetectors { get; } = new();

    public Dictionary<string, EpochChange> EpochChanges { get; } = new();
    public Dictionary<string, EpochConsensus> EpochConsensuses { get; } = new();
    public Dictionary<string, UniformConsensus> UniformConsensus { get; } = new();

    public async Task AppInterpretMessage(Message message) {
        Message? appMessage = null;
        switch (message.Type) {
            case Message.Types.Type.PlDeliver:
                appMessage = InterpretPlDeliver(message);
                break;
            case Message.Types.Type.BebDeliver:
                appMessage = NotifyHubPlSend(message);
                break;
            case Message.Types.Type.NnarReadReturn:
                appMessage = NotifyHubAppRead(message);
                break;
            case Message.Types.Type.NnarWriteReturn:
                appMessage = NotifyHubAppWrite(message);
                break;
            case Message.Types.Type.UcDecide:
                appMessage = NotifyHubAppDecide(message);
                break;
            default:
                _logger.LogWarning("App on {EndPoint} could not interpret message {Message}", _appEndPoint,
                    message);
                break;
        }

        if (appMessage is not null)
            await _eventQueueWriter.WriteAsync(appMessage);
    }

    private Message NotifyHubAppDecide(Message message) {
        return new Message {
            ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            SystemId = message.SystemId,
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = new ProcessId {Host = _hub.Address.ToString(), Port = _hub.Port},
                Message =
                    new Message {
                        ToAbstractionId = "hub", FromAbstractionId = "app", SystemId = _systemId,
                        Type = Message.Types.Type.AppDecide,
                        AppDecide = new AppDecide {
                            Value = message.UcDecide.Value
                        }
                    }
            }
        };
    }

    private Message NotifyHubAppWrite(Message message) {
        return new Message {
            ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            SystemId = message.SystemId,
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = new ProcessId {Host = _hub.Address.ToString(), Port = _hub.Port},
                Message =
                    new Message {
                        ToAbstractionId = "hub", FromAbstractionId = "app", SystemId = _systemId,
                        Type = Message.Types.Type.AppWriteReturn,
                        AppWriteReturn = new AppWriteReturn {
                            Register = message.FromAbstractionId.GetTopic()
                        }
                    }
            }
        };
    }

    private Message NotifyHubAppRead(Message message) {
        return new Message {
            ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            SystemId = message.SystemId,
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = new ProcessId {Host = _hub.Address.ToString(), Port = _hub.Port},
                Message =
                    new Message {
                        ToAbstractionId = "hub", FromAbstractionId = "app", SystemId = _systemId,
                        Type = Message.Types.Type.AppReadReturn,
                        AppReadReturn = new AppReadReturn {
                            Value = message.NnarReadReturn.Value,
                            Register = message.FromAbstractionId.GetTopic()
                        }
                    }
            }
        };
    }

    private Message NotifyHubPlSend(Message message) {
        return new Message {
            ToAbstractionId = $"{_abstractionId}.pl",
            FromAbstractionId = _abstractionId,
            SystemId = message.SystemId,
            Type = Message.Types.Type.PlSend,
            PlSend = new PlSend {
                Destination = new ProcessId {Host = _hub.Address.ToString(), Port = _hub.Port},
                Message =
                    new Message {
                        ToAbstractionId = "hub", FromAbstractionId = "app", SystemId = _systemId,
                        Type = Message.Types.Type.AppValue,
                        AppValue = message.BebDeliver.Message.AppValue
                    }
            }
        };
    }

    private Message? InterpretPlDeliver(Message message) {
        Message? appMessage = null;

        switch (message.PlDeliver.Message.Type) {
            case Message.Types.Type.ProcInitializeSystem:
                _systemId = message.SystemId;
                AddProcesses(message.PlDeliver.Message.ProcInitializeSystem.Processes);
                AddSystemToAppPl();
                CreateAppBebPl();
                _logger.LogInformation("SystemId with id {System} was set on app {EndPoint}", _systemId, _appEndPoint);
                break;
            case Message.Types.Type.ProcDestroySystem:
                DestroySystem();
                break;
            case Message.Types.Type.AppBroadcast:
                appMessage = AppBebMessage(message);
                break;
            case Message.Types.Type.AppRead:
                appMessage = AppReadMessage(message);
                break;
            case Message.Types.Type.AppWrite:
                appMessage = AppWriteMessage(message);
                break;
            case Message.Types.Type.AppPropose:
                appMessage = AppProposeMessage(message);
                break;
            default:
                _logger.LogWarning("App on {EndPoint} could not interpret PlDeliver message {Message}", _appEndPoint,
                    message);
                break;
        }

        return appMessage;
    }

    private void AddSystemToAppPl() {
        PerfectLinks["app.pl"] = new PerfectLink("app.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);
    }

    private Message AppProposeMessage(Message message) {
        return new Message {
            Type = Message.Types.Type.UcPropose,
            ToAbstractionId = $"app.uc[{message.PlDeliver.Message.AppPropose.Topic}]",
            FromAbstractionId = "app",
            SystemId = _systemId,
            UcPropose = new UcPropose() {Value = message.PlDeliver.Message.AppPropose.Value}
        };
    }

    private Message AppWriteMessage(Message message) {
        return new Message {
            Type = Message.Types.Type.NnarWrite,
            ToAbstractionId = $"app.nnar[{message.PlDeliver.Message.AppWrite.Register}]",
            FromAbstractionId = "app",
            SystemId = _systemId,
            NnarWrite = new NnarWrite {Value = message.PlDeliver.Message.AppWrite.Value}
        };
    }

    private Message AppReadMessage(Message message) {
        return new Message {
            Type = Message.Types.Type.NnarRead,
            ToAbstractionId = $"app.nnar[{message.PlDeliver.Message.AppRead.Register}]",
            FromAbstractionId = "app",
            SystemId = _systemId,
            NnarRead = new NnarRead()
        };
    }

    private Message AppBebMessage(Message message) {
        return new Message {
            Type = Message.Types.Type.BebBroadcast, FromAbstractionId = _abstractionId,
            ToAbstractionId = "app.beb",
            SystemId = message.SystemId,
            BebBroadcast = new BebBroadcast {
                Message = new Message {
                    Type = Message.Types.Type.AppValue, FromAbstractionId = _abstractionId, ToAbstractionId = "app",
                    AppValue = new AppValue {Value = message.PlDeliver.Message.AppBroadcast.Value}
                }
            }
        };
    }

    private void CreateAppBebPl() {
        PerfectLinks["app.beb.pl"] =
            new PerfectLink("app.beb.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);
        BestEffortBroadcasts["app.beb"] =
            new BestEffortBroadcast("app.beb", _appEndPoint, _eventQueueWriter, _logger, _processes);
    }

    private void DestroySystem() {
        _processes.Clear();
        PerfectLinks.Clear();
        PerfectLinks["app.pl"] = new PerfectLink("app.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);
        BestEffortBroadcasts.Clear();
        _rank = null;
        _systemId = null;
        _logger.LogInformation("System {EndPoint} destroyed", _appEndPoint);
    }

    private void AddProcesses(IEnumerable<ProcessId> processIds) {
        foreach (var process in processIds) {
            var procEndPoint = new IPEndPoint(IPAddress.Parse(process.Host), process.Port);
            _processes[procEndPoint] = process;
        }

        _rank = _processes[_appEndPoint].Rank;
    }

    private void VerifyAtomicRegister(string toAbstractionId) {
        var nnarAbstraction = toAbstractionId.GetAbstraction("nnar");
        if (!PerfectLinks.ContainsKey($"{nnarAbstraction}.pl"))
            PerfectLinks[$"{nnarAbstraction}.pl"] =
                new PerfectLink($"{nnarAbstraction}.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!PerfectLinks.ContainsKey($"{nnarAbstraction}.beb.pl"))
            PerfectLinks[$"{nnarAbstraction}.beb.pl"] =
                new PerfectLink($"{nnarAbstraction}.beb.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!BestEffortBroadcasts.ContainsKey($"{nnarAbstraction}.beb"))
            BestEffortBroadcasts[$"{nnarAbstraction}.beb"] = new BestEffortBroadcast($"{nnarAbstraction}.beb",
                _appEndPoint, _eventQueueWriter, _logger,
                _processes);

        if (!AtomicRegisters.ContainsKey(nnarAbstraction))
            AtomicRegisters[nnarAbstraction] =
                new AtomicRegister(nnarAbstraction, _logger, _appEndPoint, _eventQueueWriter, _processes.Count, _rank);
    }

    public void VerifyRequiredAbstractions(string toAbstractionId) {
        if (toAbstractionId.Contains("nnar")) VerifyAtomicRegister(toAbstractionId);
        if (toAbstractionId.Contains("uc")) VerifyUniformConsensus(toAbstractionId);
    }

    private void VerifyUniformConsensus(string toAbstractionId) {
        var ucAbstraction = toAbstractionId.GetAbstraction("uc");

        if (!UniformConsensus.ContainsKey(ucAbstraction))
            UniformConsensus[ucAbstraction] = new UniformConsensus(ucAbstraction, _appEndPoint, _processes, _logger,
                _eventQueueWriter, CreateEpochConsensus);

        if (toAbstractionId.Contains("ec")) VerifyEpochChange(toAbstractionId);
    }

    private void VerifyEpochChange(string toAbstractionId) {
        var ecAbstraction = toAbstractionId.GetAbstraction("ec");

        if (!EpochChanges.ContainsKey(ecAbstraction))
            EpochChanges[ecAbstraction] =
                new EpochChange(ecAbstraction, _appEndPoint, _processes, _logger, _eventQueueWriter);

        if (!PerfectLinks.ContainsKey($"{ecAbstraction}.pl"))
            PerfectLinks[$"{ecAbstraction}.pl"] =
                new PerfectLink($"{ecAbstraction}.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!PerfectLinks.ContainsKey($"{ecAbstraction}.beb.pl"))
            PerfectLinks[$"{ecAbstraction}.beb.pl"] =
                new PerfectLink($"{ecAbstraction}.beb.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!BestEffortBroadcasts.ContainsKey($"{ecAbstraction}.beb"))
            BestEffortBroadcasts[$"{ecAbstraction}.beb"] = new BestEffortBroadcast($"{ecAbstraction}.beb",
                _appEndPoint, _eventQueueWriter, _logger, _processes
            );

        if (!EventualLeaderDetectors.ContainsKey($"{ecAbstraction}.eld"))
            EventualLeaderDetectors[$"{ecAbstraction}.eld"] = new EventualLeaderDetector($"{ecAbstraction}.eld",
                _appEndPoint, _logger, _eventQueueWriter, _processes
            );
        if (!EventuallyPerfectFailureDetectors.ContainsKey($"{ecAbstraction}.eld.epfd"))
            EventuallyPerfectFailureDetectors[$"{ecAbstraction}.eld.epfd"] = new EventuallyPerfectFailureDetector(
                $"{ecAbstraction}.eld.epfd", _processes,
                _logger, _eventQueueWriter
            );

        if (!PerfectLinks.ContainsKey($"{ecAbstraction}.eld.epfd.pl"))
            PerfectLinks[$"{ecAbstraction}.eld.epfd.pl"] =
                new PerfectLink($"{ecAbstraction}.eld.epfd.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);
    }

    private void CreateEpochConsensus(EpInternalState state, int ets, ProcessId leader, string ucAbstractionId) {
        var epochAbstraction = $"{ucAbstractionId}.ep[{ets}]";

        if (!EpochConsensuses.ContainsKey(epochAbstraction))
            EpochConsensuses[epochAbstraction] = new EpochConsensus(epochAbstraction, _appEndPoint, state,
                _eventQueueWriter, _logger, ets, _processes);
        
        if (!PerfectLinks.ContainsKey($"{epochAbstraction}.pl"))
            PerfectLinks[$"{epochAbstraction}.pl"] =
                new PerfectLink($"{epochAbstraction}.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!PerfectLinks.ContainsKey($"{epochAbstraction}.beb.pl"))
            PerfectLinks[$"{epochAbstraction}.beb.pl"] =
                new PerfectLink($"{epochAbstraction}.beb.pl", _appEndPoint, _eventQueueWriter, _logger, _systemId);

        if (!BestEffortBroadcasts.ContainsKey($"{epochAbstraction}.beb"))
            BestEffortBroadcasts[$"{epochAbstraction}.beb"] = new BestEffortBroadcast($"{epochAbstraction}.beb",
                _appEndPoint, _eventQueueWriter, _logger, _processes);
    }

}