using System.Threading.Channels;
using DistributedAlgorithmsSystem.Protos;

namespace DistributedAlgorithmsSystem;

public class EventQueue {
    private readonly Dictionary<int, Channel<Message>> _channels = new();

    public EventQueue(IConfiguration configuration) {
        var ports = configuration.GetValue<string>("ports");
        foreach (var port in ports.Split(';')) _channels.Add(int.Parse(port), Channel.CreateUnbounded<Message>());
    }

    public ChannelWriter<Message> GetWriter(int port) {
        return _channels[port].Writer;
    }

    public ChannelReader<Message> GetReader(int port) {
        return _channels[port].Reader;
    }
}