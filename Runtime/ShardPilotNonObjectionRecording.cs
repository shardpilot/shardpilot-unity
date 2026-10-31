#nullable disable annotations
using NonObjectionRecord = System.Collections.Generic.IDictionary<string, object>;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ShardPilot
{
    public sealed partial class ShardPilotClient
    {
        private long _nonObjectionRecordingGeneration;

#nullable enable annotations
        /// <summary>Submits one host declaration. The latest host call supersedes any pending one,
        /// which completes with non_objection_superseded. A recorded acknowledgement does not grant consent.</summary>
        public void RecordNonObjection(NonObjectionRecord? record, Action<ShardPilotResult> callback)
#nullable restore annotations
        {
            if (callback == null) throw new ArgumentNullException(nameof(callback));
            var elapsed = Stopwatch.StartNew();
            long generation, denialEpoch;
            string user, anonymous, workspace, app, environment, endpoint, publishable;
            TimeSpan budget;
            lock (_gate)
            {
                generation = ++_nonObjectionRecordingGeneration; denialEpoch = _consentDenialEpoch;
                RetireNonObjectionAdmissionLocked();
                _nonObjectionRetryNotBefore = DateTime.MinValue;
                user = _config.UserId; anonymous = _config.AnonymousId;
                workspace = _config.WorkspaceKey; app = _config.AppKey; environment = _config.EnvironmentKey;
                endpoint = _consentEndpoint; publishable = _config.PublishableKey; budget = _config.HttpTimeout;
            }
            var finished = 0; var lastRetryAfter = TimeSpan.Zero;
            Action<string> cancel = null; CancellationTokenSource ownedTimeout = null;
            ShardPilotConsentOutboxEntry retained = null;
            void finish(ShardPilotResult result)
            {
                if (Interlocked.Exchange(ref finished, 1) != 0) return;
                Interlocked.CompareExchange(ref _nonObjectionCancel, null, cancel);
                lock (_gate) if (generation == _nonObjectionRecordingGeneration) _nonObjectionRetryNotBefore = DateTime.MinValue;
                try { callback(result); } catch { /* Host callbacks cannot break completion. */ }
            }
            string refusal()
            {
                lock (_gate)
                {
                    if (_closed || _closeRequested || _closeStarted || HostRetiring) return "shutdown";
                    if (_consentConfiguredActorInvalid) return "non_objection_invalid";
                    if (generation != _nonObjectionRecordingGeneration || denialEpoch != _consentDenialEpoch || _consentOutbox.IsDisowned ||
                        (retained != null && !_consentOutbox.OwnScope.Contains(retained.IdempotencyKey)) ||
                        _consentState != ShardPilotConsentState.Unknown || ConsentPurgeOwed ||
                        !string.Equals(user, _config.UserId, StringComparison.Ordinal) ||
                        !string.Equals(anonymous, _config.AnonymousId, StringComparison.Ordinal) ||
                        !string.Equals(workspace, _config.WorkspaceKey, StringComparison.Ordinal) ||
                        !string.Equals(app, _config.AppKey, StringComparison.Ordinal) ||
                        !string.Equals(environment, _config.EnvironmentKey, StringComparison.Ordinal)) return "non_objection_superseded";
                }
                return elapsed.Elapsed >= budget ? "non_objection_timeout" : null;
            }
            ShardPilotResult refused(string reason) => new ShardPilotResult(false, reason, reason == "non_objection_timeout", reason == "non_objection_timeout" ? lastRetryAfter : TimeSpan.Zero);
            cancel = code => { finish(refused(code)); try { ownedTimeout?.Cancel(); } catch (ObjectDisposedException) { } };
            Action<string> previous = null;
            lock (_gate) if (generation == _nonObjectionRecordingGeneration) { previous = _nonObjectionCancel; _nonObjectionCancel = cancel; }
            previous?.Invoke("non_objection_superseded");
            var reason = refusal(); if (reason != null) { finish(refused(reason)); return; }
            var snapshot = ShardPilotNonObjection.Snapshot(record, out var error);
            if (snapshot == null) { finish(refused(error)); return; }
            var provider = _tokenManager != null;
            var verified = provider && ShardPilotConsentOutbox.IsValidIdentifier(user);
            retained = RetainNonObjection(snapshot, generation, verified ? user : anonymous, verified ? "user_verified" : "anon", anonymous, workspace, app, environment, out error);
            if (retained == null) { finish(new ShardPilotResult(false, error, error == "consent_outbox_write_failed")); return; }
            var body = retained.NonObjectionBody;
            _ = submit();

            async Task submit()
            {
                var minting = provider;
                var callerDeadline = CancellationToken.None; var retryAttempt = 0;
                try
                {
                    var current = refusal(); if (current != null) { finish(refused(current)); return; }
                    using (var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(Math.Max(1, (budget - elapsed.Elapsed).TotalMilliseconds))))
                    {
                        callerDeadline = timeout.Token; ownedTimeout = timeout;
                        while (Volatile.Read(ref finished) == 0)
                        {
                            bool deferred; lock (_gate) deferred = DateTime.UtcNow < _nonObjectionRetryNotBefore;
                            if (deferred) await WaitNonObjectionRetry(generation, timeout.Token).ConfigureAwait(false);
                            var completion = new TaskCompletionSource<ShardPilotHttpResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
                            using (timeout.Token.Register(() => completion.TrySetCanceled()))
                            {
                                minting = provider;
                                var bearer = provider ? await _tokenManager.GetTokenAsync(timeout.Token).ConfigureAwait(false) : publishable;
                                minting = false;
                                current = refusal(); if (current != null) { finish(refused(current)); return; }
                                var request = new ShardPilotHttpRequest
                                {
                                    Method = "POST", Url = endpoint, Bearer = bearer, ContentType = "application/json", Body = body,
                                    Timeout = budget - elapsed.Elapsed, RefuseRedirects = true
                                };
                                request.BeforeSend = () =>
                                {
                                    if (Volatile.Read(ref finished) != 0) return false;
                                    var blocked = refusal();
                                    if (blocked != null) { finish(refused(blocked)); return false; }
                                    lock (_gate) if (DateTime.UtcNow < _nonObjectionRetryNotBefore) return false;
                                    request.Timeout = TimeSpan.FromMilliseconds(Math.Max(1, (budget - elapsed.Elapsed).TotalMilliseconds));
                                    return true;
                                };
                                try { (_httpSender ?? new ShardPilotUnityHttpRequestSender()).Send(request, response => completion.TrySetResult(response)); }
                                catch { completion.TrySetResult(new ShardPilotHttpResponse { TransportError = true }); }
                                var response = await completion.Task.ConfigureAwait(false);
                                current = refusal(); if (current != null) { finish(refused(current)); return; }
                                var status = response == null || response.TransportError ? 0 : response.StatusCode;
                                if (status >= 200 && status < 300)
                                {
                                    var valid = false;
                                    try
                                    {
                                        System.Collections.Generic.Dictionary<string, object> decoded;
                                        valid = response.Body != null && new System.Text.UTF8Encoding(false, true).GetByteCount(response.Body) <= 1024 &&
                                            ShardPilotJsonParser.TryParseUniqueObject(response.Body, out decoded, out _) &&
                                            ShardPilotNonObjection.Response(decoded, out _) != null;
                                    }
                                    catch { /* Invalid acknowledgement encoding is terminal. */ }
                                    current = refusal();
                                    if (valid)
                                    {
                                        lock (_gate)
                                        {
                                            if (refusal() == null)
                                                _nonObjectionAdmission = new NonObjectionAdmission(generation, denialEpoch,
                                                    user, anonymous, workspace, app, environment, retained.ActorIdentifier, retained.Kind);
                                        }
                                    }
                                    if (current == null && !RetireNonObjection(retained, refusal)) current = refusal() ?? "non_objection_superseded";
                                    finish(current != null ? refused(current) : valid ? ShardPilotResult.Ok : refused("non_objection_response_invalid"));
                                    return;
                                }
                                if (status == 401 && provider) { _tokenManager.Invalidate(); lock (_gate) _consentAuthEpoch++; }
                                var retryable = status == 0 || status == 408 || status == 429 || status >= 500 || (status == 401 && provider);
                                var code = status == 401 ? "unauthorized" : (status == 408 || status == 429 || status >= 500 ? "transient_" : "http_") + status;
                                var retryAfter = status == 429 || status >= 500 ? Math.Min(86400, Math.Max(0, response.RetryAfterSeconds)) : 0;
                                if (!retryable)
                                {
                                    var retired = RetireNonObjection(retained, refusal);
                                    finish(retired ? new ShardPilotResult(false, code, false) : refused(refusal() ?? "non_objection_superseded")); return;
                                }
                                lastRetryAfter = TimeSpan.FromSeconds(retryAfter);
                                lock (_gate)
                                {
                                    var wait = lastRetryAfter > TimeSpan.Zero ? lastRetryAfter : PublishBackoffWait(++retryAttempt, _consentBackoffJitter.NextDouble());
                                    var next = DateTime.UtcNow + wait;
                                    if (generation == _nonObjectionRecordingGeneration && next > _nonObjectionRetryNotBefore)
                                        _nonObjectionRetryNotBefore = next;
                                }
                                await WaitNonObjectionRetry(generation, timeout.Token).ConfigureAwait(false);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { finish(refused(refusal() ?? "non_objection_timeout")); }
                catch { var blocked = refusal() ?? (callerDeadline.IsCancellationRequested ? "non_objection_timeout" : null); finish(blocked != null ? refused(blocked) : new ShardPilotResult(false, minting ? "token_unavailable" : "http_0", true)); }
            }
        }
    }
}
