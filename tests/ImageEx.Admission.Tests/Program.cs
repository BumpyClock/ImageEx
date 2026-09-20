using ImageEx.Cache;

static void Require(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}

using (var gate = new ImageAdmissionGate(2))
{
    await gate.WaitAsync(default);
    await gate.WaitAsync(default);
    var speculative = gate.WaitAsync(default, () => false);
    var visible = gate.WaitAsync(default);
    Require(!speculative.IsCompleted && !visible.IsCompleted, "Capacity must stay at two.");
    gate.Release();
    await visible.WaitAsync(TimeSpan.FromSeconds(2));
    Require(!speculative.IsCompleted, "Visible work must precede queued speculative work.");
    gate.Release();
    await speculative.WaitAsync(TimeSpan.FromSeconds(2));
    gate.Release();
    gate.Release();
}
Console.WriteLine("PASS fixed capacity and visible priority");

using (var gate = new ImageAdmissionGate(1))
{
    await gate.WaitAsync(default);
    var promoted = false;
    var firstSpeculative = gate.WaitAsync(default, () => false);
    var shared = gate.WaitAsync(default, () => promoted);
    promoted = true;
    gate.Release();
    await shared.WaitAsync(TimeSpan.FromSeconds(2));
    Require(!firstSpeculative.IsCompleted, "Shared demand must promote the existing queue entry.");
    gate.Release();
    await firstSpeculative.WaitAsync(TimeSpan.FromSeconds(2));
    gate.Release();
}
Console.WriteLine("PASS shared request priority promotion");

using (var gate = new ImageAdmissionGate(1))
{
    await gate.WaitAsync(default);
    using var cancellation = new CancellationTokenSource();
    var cancelled = gate.WaitAsync(cancellation.Token);
    var next = gate.WaitAsync(default, () => false);
    cancellation.Cancel();
    try { await cancelled; throw new Exception("Cancellation was lost."); }
    catch (OperationCanceledException) { }
    gate.Release();
    await next.WaitAsync(TimeSpan.FromSeconds(2));
    gate.Release();
    await gate.WaitAsync(default);
    gate.Release();
}
Console.WriteLine("PASS queued cancellation preserves permits");

using (var gate = new ImageAdmissionGate(1))
{
    await gate.WaitAsync(default);
    var visible = true;
    var first = gate.WaitAsync(default, () => false);
    var demoted = gate.WaitAsync(default, () => visible);
    visible = false;
    gate.Release();
    await first.WaitAsync(TimeSpan.FromSeconds(2));
    Require(!demoted.IsCompleted, "Withdrawing visible demand must restore FIFO speculation.");
    gate.Release();
    await demoted.WaitAsync(TimeSpan.FromSeconds(2));
    gate.Release();
}
Console.WriteLine("PASS withdrawn visible interest demotes shared request");
