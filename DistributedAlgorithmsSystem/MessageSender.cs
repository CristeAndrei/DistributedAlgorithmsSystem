using System.Net;
using System.Net.Sockets;
using DistributedAlgorithmsSystem.Protos;
using Google.Protobuf;

namespace DistributedAlgorithmsSystem;

public static class MessageSender {
    public static async Task SendMessage(Message message, IPEndPoint endPoint) {
        var tcpClient = new TcpClient();

        await tcpClient.ConnectAsync(endPoint);

        var networkStream = tcpClient.GetStream();

        var messageSize = BitConverter.GetBytes(message.CalculateSize());
        Array.Reverse(messageSize);

        await networkStream.WriteAsync(messageSize);
        await networkStream.WriteAsync(message.ToByteArray());

        networkStream.Close();
        tcpClient.Close();
    }
}