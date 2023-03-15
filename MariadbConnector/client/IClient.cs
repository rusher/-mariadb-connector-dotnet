using System.Data;
using MariadbConnector.client.util;
using MariadbConnector.message;
using MariadbConnector.utils;

namespace MariadbConnector.client;

public interface IClient
{
    IContext? Context { get; set; }

    ExceptionFactory ExceptionFactory { get; }
    HostAddress HostAddress { get; }

    List<ICompletion> Execute(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message,
        bool canRedo);

    List<ICompletion> Execute(IoBehavior ioBehavior, CancellationToken cancellationToken, IClientMessage message,
        MariaDbCommand stmt,
        bool canRedo);

    List<ICompletion> Execute(
        IoBehavior ioBehavior, CancellationToken cancellationToken,
        IClientMessage message,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        bool canRedo);

    List<ICompletion> ExecutePipeline(
        IoBehavior ioBehavior, CancellationToken cancellationToken,
        IClientMessage[] messages,
        MariaDbCommand stmt,
        CommandBehavior behavior,
        bool canRedo);

    void ReadStreamingResults(
        IoBehavior ioBehavior, CancellationToken cancellationToken,
        List<ICompletion> completions,
        CommandBehavior behavior);

    void ClosePrepare(IPrepare prepare);

    Task CloseAsync();

    void SetReadOnly(bool readOnly);

    bool IsClosed();

    void Reset();

    bool IsPrimary();
}