using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine.Networking;

namespace ShardPilot
{
    internal sealed class ShardPilotTransportRequest
    {
        public string Endpoint { get; set; }
        public string ApiKey { get; set; }
        public string Payload { get; set; }

        public TimeSpan Timeout { get; set; }

        internal Func<Task<bool>> BeforeSend;
        internal Func<bool> BeforeSendStillCurrent;
        internal bool DeliveryEvidenceKnown;
        internal bool DefinitelyNotDelivered;

        /// <summary>
        /// Optional last-moment veto, consulted before the transport starts a
        /// SECOND request for this batch (the uncompressed fallback). Null
        /// means no veto.
        ///
        /// The first request is past recall the moment it is handed over — the
        /// caller's consent-epoch check ran before that, and a denial landing
        /// afterwards is the documented residual window. The RETRY is a
        /// different matter: it is a fresh request begun after the caller
        /// stopped looking, so without this a denial that completed mid-flight
        /// — wiping the queue and spool as it went — would be followed by the
        /// condemned batch going onto the wire anyway, stretching that window
        /// past the request it was defined for.
        /// </summary>
        public Func<bool> StillPermitted { get; set; }
    }

    /// <summary>
    /// A single non-accepted per-event outcome surfaced by the 202 batch
    /// response (<c>events[]</c>): an <c>observed</c>, <c>rejected</c>, or
    /// <c>suppressed_no_consent</c> verdict the server reached for one event
    /// even though the batch as a whole succeeded. Accepted events are not
    /// reported here.
    /// </summary>
    internal sealed class ShardPilotEventOutcome
    {
        public ShardPilotEventOutcome(string eventId, string status, string code, string message = null)
        {
            EventId = eventId;
            Status = status;
            Code = code;
            Message = message;
        }

        public string EventId { get; private set; }
        public string Status { get; private set; }
        public string Code { get; private set; }
        public string Message { get; private set; }
    }

    internal sealed class ShardPilotBatchResult
    {
        public int Accepted { get; set; }
        public int Rejected { get; set; }
        public int Duplicates { get; set; }

        /// <summary>
        /// Per-event statuses the server reported that were not
        /// <c>accepted</c>. Empty when every event was accepted. Surfaced
        /// through the SDK logger so integrators learn their events were
        /// observed (unregistered), rejected (blocked/unknown/schema), or
        /// suppressed by consent, instead of silently lost inside a 202.
        /// </summary>
        public IReadOnlyList<ShardPilotEventOutcome> NonAcceptedOutcomes { get; set; }
    }

    internal interface IShardPilotTransport
    {
        Task<ShardPilotBatchResult> PublishAsync(ShardPilotTransportRequest request, CancellationToken cancellationToken);
    }

    // Implementations must invoke and honor BeforeSend before each request API
    // call. This allows analytics to fence its first attempt at submission,
    // without persisting a speculative clock while waiting for the dispatcher.
    internal interface IShardPilotFinalSendGate { }

    internal sealed class ShardPilotHttpStatusException : Exception
    {
        public int StatusCode { get; private set; }

        /// <summary>
        /// The server's <c>{ "error": { code, ... } }</c> envelope code on a
        /// non-2xx response, or null when the body carried no envelope. Lets
        /// the SDK surface <c>blocked_event</c> / <c>requests_rate_limited</c>
        /// and the like instead of a bare "status 400".
        /// </summary>
        public string ErrorCode { get; private set; }

        /// <summary>
        /// The per-field detail codes from <c>error.details[]</c>, joined for
        /// the diagnostic message. Empty when absent.
        /// </summary>
        public string DetailCodes { get; private set; }

        /// <summary>
        /// The whole-seconds <c>Retry-After</c> the server attached to a retryable response,
        /// or zero when the header was absent or unparseable. The worker defers
        /// its next publish attempt by at least this long.
        /// </summary>
        public int RetryAfterSeconds { get; private set; }

        public ShardPilotHttpStatusException(int statusCode)
            : this(statusCode, null, null, 0)
        {
        }

        public ShardPilotHttpStatusException(int statusCode, string errorCode, string detailCodes, int retryAfterSeconds)
            : base(BuildMessage(statusCode, errorCode, detailCodes))
        {
            StatusCode = statusCode;
            ErrorCode = errorCode;
            DetailCodes = detailCodes;
            RetryAfterSeconds = retryAfterSeconds < 0 ? 0 : retryAfterSeconds;
        }

        public bool Retryable
        {
            get { return StatusCode <= 0 || StatusCode == 408 || StatusCode == 429 || StatusCode >= 500; }
        }

        private static string BuildMessage(int statusCode, string errorCode, string detailCodes)
        {
            var message = "shardpilot ingest returned status " + statusCode;
            if (!string.IsNullOrEmpty(errorCode))
            {
                message += " (" + errorCode + ")";
            }

            if (!string.IsNullOrEmpty(detailCodes))
            {
                message += " [" + detailCodes + "]";
            }

            return message;
        }
    }

    internal sealed class ShardPilotTransportException : Exception
    {
        public ShardPilotTransportException(string message, bool retryable) : base(message)
        {
            Retryable = retryable;
        }

        public bool Retryable { get; private set; }
    }

    internal sealed class ShardPilotUnityWebRequestTransport : IShardPilotTransport, IShardPilotFinalSendGate
    {
        /// <summary>
        /// The request header carrying the batch's declared envelope-schema-set
        /// revision (the schema-revision handshake). A batch-only, per-request
        /// declaration: the generic sender used by consent, remote-config, and
        /// crash requests never sets it. The ingest service ignores the header
        /// entirely while its handshake mode is off (the dark default) and
        /// always admits an undeclared batch in every mode.
        /// </summary>
        internal const string SchemaRevisionHeaderName = "X-ShardPilot-Schema-Revision";

        private readonly ShardPilotDispatcher _dispatcher;
        private readonly ShardPilotCompressionPolicy _compression;

        public ShardPilotUnityWebRequestTransport()
            : this(ShardPilotDispatcher.Instance)
        {
        }

        internal ShardPilotUnityWebRequestTransport(ShardPilotDispatcher dispatcher)
            : this(dispatcher, true)
        {
        }

        internal ShardPilotUnityWebRequestTransport(ShardPilotDispatcher dispatcher, bool compressionEnabled)
        {
            _dispatcher = dispatcher;
            _compression = new ShardPilotCompressionPolicy(compressionEnabled);
        }

        public Task<ShardPilotBatchResult> PublishAsync(ShardPilotTransportRequest request, CancellationToken cancellationToken)
        {
            request.DeliveryEvidenceKnown = true;
            request.DefinitelyNotDelivered = true;
            var completion = new TaskCompletionSource<ShardPilotBatchResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _dispatcher.Run(Publish(request, _compression, completion, cancellationToken), () => completion.TrySetCanceled());
            return completion.Task;
        }

        /// <summary>
        /// Drives one publish, plus the single uncompressed retry that keeps a
        /// compressed body from becoming data loss against a deployment or an
        /// edge that cannot carry it. See ShardPilotCompressionPolicy for why
        /// the transport-error arm exists (WebGL preflight) and why it is
        /// probationary.
        /// </summary>
        private static IEnumerator Publish(
            ShardPilotTransportRequest request,
            ShardPilotCompressionPolicy compression,
            TaskCompletionSource<ShardPilotBatchResult> completion,
            CancellationToken cancellationToken)
        {
            var compress = compression != null && compression.ShouldCompress;
            var attempt = new ShardPilotPublishAttempt();

            yield return Send(request, compress, attempt, cancellationToken);

            if (attempt.Canceled)
            {
                completion.TrySetCanceled();
                yield break;
            }

            if (attempt.Error == null)
            {
                if (attempt.Compressed && compression != null)
                {
                    compression.RecordCompressedSuccess();
                }

                completion.TrySetResult(attempt.Result);
                yield break;
            }

            if (!attempt.Compressed || compression == null || !compression.ShouldRetryUncompressed(attempt.Error))
            {
                completion.TrySetException(attempt.Error);
                yield break;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                // The publish's own HttpTimeout ran out while the compressed
                // attempt was failing. Send() only observes the token from
                // inside its `while (!operation.isDone)` loop — AFTER
                // SendWebRequest() — so entering the fallback here would put a
                // fresh batch request on the wire past the deadline that was
                // supposed to bound the whole publish, and abort it a tick
                // later, possibly after ingest had already taken it.
                completion.TrySetCanceled();
                yield break;
            }

            if (request.StillPermitted != null && !request.StillPermitted())
            {
                // A consent denial completed while the compressed attempt was
                // in flight. The batch is condemned either way — the worker's
                // epoch reconciliation drops it — but what is KNOWN about the
                // wire differs, and the caller's accounting is built on that.
                //
                // A definitive response proves this request did not land. A
                // bare cancellation carries no status, so a batch that WAS
                // handed to the transport is classified as wire-ambiguous and
                // the exposure facts riding it are burned rather than left
                // re-armable — for a request the server explicitly refused.
                // The veto is what stopped the fallback; it should not also
                // erase what the first attempt established.
                var received = attempt.Error as ShardPilotHttpStatusException;
                if (received != null && received.StatusCode > 0)
                {
                    completion.TrySetException(attempt.Error);
                    yield break;
                }

                // Nothing definitive to preserve — the probationary
                // transport-error path arrives here with no status at all, and
                // that failure genuinely IS ambiguous. Report it the way a
                // cancelled publish is reported.
                completion.TrySetCanceled();
                yield break;
            }

            var retry = new ShardPilotPublishAttempt();
            yield return Send(request, false, retry, cancellationToken);

            if (retry.Canceled)
            {
                completion.TrySetCanceled();
                yield break;
            }

            if (retry.Error != null)
            {
                if (ShardPilotRequestCompression.IsEncodingRefusal(attempt.Error))
                {
                    // The coding was DEFINITIVELY refused, and
                    // ShouldRetryUncompressed already latched compression off,
                    // so the original 400 has nothing left to tell the caller —
                    // while the retry's failure is the live one. Surfacing the
                    // 400 here would have the caller classify the batch as
                    // permanently rejected and drop it (and remove its spooled
                    // copy) over a transient 503 on a request that is now sent
                    // the way the server accepts.
                    completion.TrySetException(retry.Error);
                    yield break;
                }

                // The probationary case: nothing proved the coding was at
                // fault, so surface the ORIGINAL error (the caller's retry and
                // backoff classification are built on it) and leave
                // compression armed for the next publish.
                completion.TrySetException(attempt.Error);
                yield break;
            }

            // Compressed failed, uncompressed worked: that is the evidence.
            compression.Disable();
            completion.TrySetResult(retry.Result);
        }

        private static IEnumerator Send(
            ShardPilotTransportRequest request,
            bool compress,
            ShardPilotPublishAttempt attempt,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                attempt.Canceled = true;
                yield break;
            }
            if (request.BeforeSend != null)
            {
                do
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        attempt.Canceled = true;
                        yield break;
                    }
                    Task<bool> preparation = null;
                    try { preparation = request.BeforeSend(); }
                    catch (Exception ex) { attempt.Error = ex; }
                    if (attempt.Error != null) yield break;
                    while (!preparation.IsCompleted) yield return null;
                    var permitted = false;
                    try { permitted = preparation.GetAwaiter().GetResult(); }
                    catch (Exception ex) { attempt.Error = ex; }
                    if (attempt.Error != null) yield break;
                    if (!permitted)
                    {
                        attempt.Result = new ShardPilotBatchResult();
                        yield break;
                    }
                }
                // Preparation performs durable I/O away from the frame thread.
                // A later frame must not spend an expired preparation result.
                while (request.BeforeSendStillCurrent != null && !request.BeforeSendStillCurrent());
            }
            if (cancellationToken.IsCancellationRequested)
            {
                attempt.Canceled = true;
                yield break;
            }
            var body = Encoding.UTF8.GetBytes(request.Payload);
            byte[] compressedBody;
            if (compress && ShardPilotRequestCompression.TryCompress(body, out compressedBody))
            {
                body = compressedBody;
                attempt.Compressed = true;
            }

            using (var webRequest = new UnityWebRequest(request.Endpoint, UnityWebRequest.kHttpVerbPOST))
            {
                webRequest.uploadHandler = new UploadHandlerRaw(body);
                webRequest.downloadHandler = new DownloadHandlerBuffer();
                webRequest.SetRequestHeader("Content-Type", "application/json");
                if (attempt.Compressed)
                {
                    webRequest.SetRequestHeader(
                        ShardPilotRequestCompression.ContentEncodingHeaderName,
                        ShardPilotRequestCompression.GzipCoding);
                }

                webRequest.SetRequestHeader("Authorization", "Bearer " + request.ApiKey);
                ShardPilotSdkIdentity.Apply(webRequest);

                webRequest.timeout = Math.Max(1, (int)Math.Ceiling(request.Timeout.TotalSeconds));

                var previouslyUnsent = request.DefinitelyNotDelivered;
                request.DefinitelyNotDelivered = false;
                var operation = webRequest.SendWebRequest();
                while (!operation.isDone)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        webRequest.Abort();
                        attempt.Canceled = true;
                        yield break;
                    }

                    yield return null;
                }

                if (webRequest.result == UnityWebRequest.Result.ConnectionError ||
                    webRequest.result == UnityWebRequest.Result.DataProcessingError)
                {
                    attempt.Error = new ShardPilotTransportException("shardpilot ingest connection failed", true);
                    yield break;
                }

                if (webRequest.responseCode < 200 || webRequest.responseCode >= 300)
                {
                    if (previouslyUnsent && webRequest.responseCode >= 400 && webRequest.responseCode < 500)
                        request.DefinitelyNotDelivered = true;
                    var errorBody = webRequest.downloadHandler == null ? string.Empty : webRequest.downloadHandler.text;
                    attempt.Error = BuildStatusException(
                        (int)webRequest.responseCode,
                        webRequest.GetResponseHeader("Retry-After"),
                        errorBody);
                    yield break;
                }

                try
                {
                    var responseText = webRequest.downloadHandler == null ? string.Empty : webRequest.downloadHandler.text;
                    attempt.Result = ParseBatchResponse(responseText);
                }
                catch (Exception ex)
                {
                    attempt.Error = new Exception("decode shardpilot ingest response: " + ex.Message, ex);
                }
            }
        }

        /// <summary>
        /// One publish attempt's outcome, carried out of the coroutine.
        /// A coroutine cannot return a value, and the retry above needs to
        /// know THREE things about the attempt it just made: whether it was
        /// compressed, whether it was cancelled, and how it failed.
        /// </summary>
        private sealed class ShardPilotPublishAttempt
        {
            internal bool Compressed;
            internal bool Canceled;
            internal Exception Error;
            internal ShardPilotBatchResult Result;
        }

        /// <summary>
        /// Builds the status exception for a non-2xx response, carrying the
        /// server's <c>{ "error": { code, details } }</c> envelope and the
        /// whole-seconds <c>Retry-After</c> header (429 backpressure) so the
        /// client surfaces a real cause and honours the server's retry hint
        /// instead of a bare HTTP status. The token never reaches this code.
        /// </summary>
        internal static ShardPilotHttpStatusException BuildStatusException(int statusCode, string retryAfterHeader, string body)
        {
            var retryAfter = ParseRetryAfterSeconds(retryAfterHeader);
            string errorCode = null;
            string detailCodes = null;
            ParseErrorEnvelope(body, out errorCode, out detailCodes);
            return new ShardPilotHttpStatusException(statusCode, errorCode, detailCodes, retryAfter);
        }

        internal static ShardPilotBatchResult ParseBatchResponse(string responseText)
        {
            if (string.IsNullOrEmpty(responseText))
            {
                return new ShardPilotBatchResult();
            }

            Dictionary<string, object> root;
            if (!ShardPilotJsonParser.TryParseObject(responseText, out root))
            {
                throw new FormatException("response was not a json object");
            }

            var result = new ShardPilotBatchResult
            {
                Accepted = ReadInt(root, "accepted"),
                Rejected = ReadInt(root, "rejected"),
                Duplicates = ReadInt(root, "duplicates")
            };

            object eventsValue;
            var nonAccepted = new List<ShardPilotEventOutcome>();
            var events = root.TryGetValue("events", out eventsValue) ? eventsValue as List<object> : null;
            if (events != null)
            {
                foreach (var entry in events)
                {
                    var item = entry as Dictionary<string, object>;
                    if (item == null)
                    {
                        continue;
                    }

                    var status = ReadString(item, "status");
                    if (string.IsNullOrEmpty(status) || string.Equals(status, "accepted", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    nonAccepted.Add(new ShardPilotEventOutcome(
                        ReadString(item, "event_id"),
                        status,
                        ReadString(item, "code"),
                        ReadString(item, "message")));
                }
            }

            result.NonAcceptedOutcomes = nonAccepted;
            return result;
        }

        private static void ParseErrorEnvelope(string body, out string errorCode, out string detailCodes)
        {
            errorCode = null;
            detailCodes = null;
            Dictionary<string, object> root;
            if (string.IsNullOrEmpty(body) || !ShardPilotJsonParser.TryParseObject(body, out root))
            {
                return;
            }

            object errorValue;
            var error = root.TryGetValue("error", out errorValue) ? errorValue as Dictionary<string, object> : null;
            if (error == null)
            {
                return;
            }

            errorCode = ReadString(error, "code");

            object detailsValue;
            var details = error.TryGetValue("details", out detailsValue) ? detailsValue as List<object> : null;
            if (details != null)
            {
                var codes = new List<string>();
                foreach (var entry in details)
                {
                    var detail = entry as Dictionary<string, object>;
                    if (detail == null)
                    {
                        continue;
                    }

                    var code = ReadString(detail, "code");
                    var field = ReadString(detail, "field");
                    if (string.IsNullOrEmpty(code))
                    {
                        continue;
                    }

                    codes.Add(string.IsNullOrEmpty(field) ? code : field + ":" + code);
                }

                if (codes.Count > 0)
                {
                    detailCodes = string.Join(",", codes.ToArray());
                }
            }
        }

        /// <summary>
        /// Parses delta-seconds or an HTTP date, rounding up to the server's
        /// floor and capping at 24 hours. Absent, past or invalid hints yield zero.
        /// </summary>
        internal static int ParseRetryAfterSeconds(string headerValue, DateTime? nowUtc = null)
        {
            if (string.IsNullOrEmpty(headerValue))
            {
                return 0;
            }

            var value = headerValue.Trim();
            var seconds = 0;
            var digits = value.Length > 0;
            foreach (var ch in value)
            {
                if (ch < '0' || ch > '9') { digits = false; break; }
                seconds = Math.Min(86400, seconds * 10 + ch - '0');
            }
            if (digits) return seconds;

            var now = nowUtc ?? DateTime.UtcNow;
            DateTimeOffset date;
            const string obsolete = "dddd, dd-MMM-yy HH:mm:ss 'GMT'";
            var formats = new[] { "r", obsolete, "ddd MMM d HH:mm:ss yyyy", "ddd MMM  d HH:mm:ss yyyy" };
            // HTTP's obsolete two-digit year uses a rolling 50-year window.
            var culture = (System.Globalization.CultureInfo)System.Globalization.CultureInfo.InvariantCulture.Clone();
            culture.DateTimeFormat.Calendar.TwoDigitYearMax = Math.Min(9999, now.Year + 50);
            foreach (var format in formats)
            {
                if (!DateTimeOffset.TryParseExact(value, format, culture,
                    System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out date))
                    continue;
                // The calendar resolves the year, then the complete instant
                // resolves the boundary within that year. Four-digit dates
                // are absolute and must never inherit this reinterpretation.
                if (format == obsolete && date.UtcDateTime > now.AddYears(50))
                    date = date.AddYears(-100);
                return (int)Math.Min(86400, Math.Max(0, Math.Ceiling((date.UtcDateTime - now).TotalSeconds)));
            }
            return 0;
        }

        private static int ReadInt(Dictionary<string, object> map, string key)
        {
            object value;
            if (map.TryGetValue(key, out value))
            {
                if (value is long)
                {
                    return (int)(long)value;
                }

                if (value is double)
                {
                    return (int)(double)value;
                }
            }

            return 0;
        }

        private static string ReadString(Dictionary<string, object> map, string key)
        {
            object value;
            return map.TryGetValue(key, out value) ? value as string : null;
        }
    }

    /// <summary>
    /// Generic one-shot HTTP request used by the consent receipt and the
    /// remote-config fetch (neither speaks the events:batch protocol).
    /// </summary>
    internal sealed class ShardPilotHttpRequest
    {
        public string Method { get; set; }
        public string Url { get; set; }
        public string Bearer { get; set; }
        public string ContentType { get; set; }
        public string Body { get; set; }

        /// <summary>
        /// Optional RAW request body. When set it wins over <see cref="Body"/>:
        /// the bytes are handed to the transport verbatim, never re-encoded —
        /// the tombstone lane's multipart body embeds a binary protobuf part
        /// that a string round-trip would corrupt. Null for the JSON senders.
        /// </summary>
        public byte[] BodyBytes { get; set; }
        public bool RefuseRedirects { get; set; }

        /// <summary>
        /// Optional last-moment dispatch gate. The production sender only
        /// ENQUEUES its coroutine and returns, so the caller's synchronized
        /// recheck-then-hand-off can be followed by a consent denial or
        /// crash opt-out COMPLETING before the coroutine ever runs; a
        /// deferring sender therefore evaluates this immediately before the
        /// network request object is created. False — or a throw, which
        /// fails CLOSED — means nothing may leave: the request is not
        /// built, and the callback completes with the synthetic
        /// <see cref="ShardPilotUnityHttpRequestSender.RefusedBeforeSendResponse"/>
        /// (a retryable transport-shaped refusal, so a durable caller
        /// retries under the NEW consent world's own gates). Null = no gate
        /// (every non-dump-class caller). The tombstone lane sets it.
        /// </summary>
        public Func<bool> BeforeSend { get; set; }

        /// <summary>
        /// Consented remote-config URL, with optional targeting attributes.
        /// Its path stays two segments; OnUrlSelected installs the subject header.
        /// Url is the identifier-free fallback. Only a sender that checks
        /// ConsentedUrlGate immediately before dispatch may use this variant.
        /// </summary>
        public string ConsentedUrl { get; set; }
        public Func<bool> ConsentedUrlGate { get; set; }

        /// <summary>
        /// Binds the selected URL to its subject header, validator and cache scope.
        /// Called before headers are copied to the wire request.
        /// </summary>
        public Action<bool> OnUrlSelected { get; set; }

        /// <summary>Optional local completion after URL/scope selection and
        /// before constructing a network request. A non-null response completes
        /// locally; a null result permits dispatch.</summary>
        public Func<ShardPilotHttpResponse> BeforeDispatch { get; set; }

        public Dictionary<string, string> Headers { get; set; }
        public TimeSpan Timeout { get; set; }
    }

    internal sealed class ShardPilotHttpResponse
    {
        public bool TransportError { get; set; }
        public long StatusCode { get; set; }
        public string Body { get; set; }
        public string ETag { get; set; }
        public string EffectiveUrl { get; set; }
        public string Error { get; set; }

        /// <summary>
        /// The server's whole-seconds <c>Retry-After</c> hint (429/503), parsed from the
        /// response header; 0 when absent. Exposed here so the crash path — which only sees
        /// this response object, not the raw web request — can honour it.
        /// </summary>
        public int RetryAfterSeconds { get; set; }
    }

    internal interface IShardPilotHttpRequestSender
    {
        void Send(ShardPilotHttpRequest request, Action<ShardPilotHttpResponse> callback);
    }

    /// <summary>
    /// UnityWebRequest sender riding the main-thread dispatcher, mirroring
    /// ShardPilotUnityWebRequestTransport. Live-host callbacks run on the main thread;
    /// a late send to a retired host completes on its caller with a transport error.
    /// </summary>
    internal sealed class ShardPilotUnityHttpRequestSender : IShardPilotHttpRequestSender
    {
        private readonly ShardPilotDispatcher _dispatcher;

        public ShardPilotUnityHttpRequestSender()
            : this(ShardPilotDispatcher.Instance)
        {
        }

        internal ShardPilotUnityHttpRequestSender(ShardPilotDispatcher dispatcher)
        {
            _dispatcher = dispatcher;
        }

        public void Send(ShardPilotHttpRequest request, Action<ShardPilotHttpResponse> callback)
        {
            Action<ShardPilotHttpResponse> complete = response => { try { Interlocked.Exchange(ref callback, null)?.Invoke(response); } catch { /* Host callbacks cannot break retirement. */ } };
            _dispatcher.Run(SendRoutine(request, complete), () => complete(new ShardPilotHttpResponse { TransportError = true, Error = "host_retired" }));
        }

        /// <summary>
        /// Evaluates the request's optional <see cref="ShardPilotHttpRequest.BeforeSend"/>
        /// gate at the moment the network request is about to be created.
        /// A throw fails CLOSED (dump-class bytes must not leave on an
        /// unknowable consent world). Shared with the test doubles so the
        /// harness pins the exact production semantics.
        /// </summary>
        internal static bool BeforeSendGateAllows(ShardPilotHttpRequest request)
        {
            var gate = request == null ? null : request.BeforeSend;
            if (gate == null)
            {
                return true;
            }

            try
            {
                return gate();
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Selects the consented URL only while its live gate allows it.
        /// Missing or throwing gates use the identifier-free baseline. The
        /// selected cache headers are installed before the request is built.
        /// Shared by the production sender and its test doubles.
        /// </summary>
        internal static string UrlForSend(ShardPilotHttpRequest request)
        {
            if (request == null)
            {
                return null;
            }

            var consented = false;
            try
            {
                consented = !string.IsNullOrEmpty(request.ConsentedUrl) &&
                    request.ConsentedUrlGate != null && request.ConsentedUrlGate();
            }
            catch
            {
                // An unreadable consent state cannot authorize identifiers.
            }

            request.OnUrlSelected?.Invoke(consented);
            return consented ? request.ConsentedUrl : request.Url;
        }

        /// <summary>
        /// The synthetic completion for a request the
        /// <see cref="ShardPilotHttpRequest.BeforeSend"/> gate refused:
        /// transport-shaped and therefore RETRYABLE — the caller's record
        /// stays owed and retries under the new consent world's own gates
        /// (for the tombstone lane: the next launch's pass, which a standing
        /// revocation settles without a report) — and carrying no
        /// Retry-After, so nothing re-arms backpressure from a refusal.
        /// </summary>
        internal static ShardPilotHttpResponse RefusedBeforeSendResponse()
        {
            return new ShardPilotHttpResponse
            {
                TransportError = true,
                Error = "refused_before_send"
            };
        }

        private static IEnumerator SendRoutine(ShardPilotHttpRequest request, Action<ShardPilotHttpResponse> callback)
        {
            // The last-moment gate, BEFORE the web request exists: a consent
            // denial or crash opt-out that completed between the caller's
            // synchronized handoff (its Send call only enqueued this
            // routine) and this pump must stop the bytes here — there is no
            // later client-side point.
            if (!BeforeSendGateAllows(request))
            {
                if (callback != null)
                {
                    try
                    {
                        callback(RefusedBeforeSendResponse());
                    }
                    catch
                    {
                        // Callbacks must never throw back into the dispatcher.
                    }
                }

                yield break;
            }

            // The URL is resolved HERE, not at Send: a consent downgrade
            // landing between the caller's enqueue and this pump must strip
            // identifiers and attributes back to the identifier-free
            // baseline (see UrlForSend) — after this line there is no later
            // client-side point.
            var url = UrlForSend(request);
            var local = request.BeforeDispatch?.Invoke();
            if (local != null)
            {
                try { callback?.Invoke(local); }
                catch { /* A host callback cannot break the dispatcher. */ }
                yield break;
            }
            using (var webRequest = new UnityWebRequest(url, request.Method))
            {
                if (request.RefuseRedirects) webRequest.redirectLimit = 0;
                // Raw bytes win over the string body (see BodyBytes): the
                // multipart tombstone body is binary and must go out verbatim.
                var bodyBytes = request.BodyBytes;
                if (bodyBytes == null && !string.IsNullOrEmpty(request.Body))
                {
                    bodyBytes = Encoding.UTF8.GetBytes(request.Body);
                }

                if (bodyBytes != null && bodyBytes.Length > 0)
                {
                    webRequest.uploadHandler = new UploadHandlerRaw(bodyBytes);
                }

                webRequest.downloadHandler = new DownloadHandlerBuffer();
                if (!string.IsNullOrEmpty(request.ContentType))
                {
                    webRequest.SetRequestHeader("Content-Type", request.ContentType);
                }

                if (!string.IsNullOrEmpty(request.Bearer))
                {
                    webRequest.SetRequestHeader("Authorization", "Bearer " + request.Bearer);
                }

                if (request.Headers != null)
                {
                    foreach (var header in request.Headers)
                    {
                        webRequest.SetRequestHeader(header.Key, header.Value);
                    }
                }

                ShardPilotSdkIdentity.Apply(webRequest);

                webRequest.timeout = Math.Max(1, (int)Math.Ceiling(request.Timeout.TotalSeconds));

                var operation = webRequest.SendWebRequest();
                while (!operation.isDone)
                {
                    yield return null;
                }

                var response = new ShardPilotHttpResponse
                {
                    TransportError = webRequest.result == UnityWebRequest.Result.ConnectionError ||
                                     webRequest.result == UnityWebRequest.Result.DataProcessingError,
                    StatusCode = webRequest.responseCode,
                    Body = webRequest.downloadHandler == null ? string.Empty : webRequest.downloadHandler.text,
                    ETag = webRequest.GetResponseHeader("ETag"),
                    EffectiveUrl = webRequest.url,
                    Error = webRequest.error,
                    RetryAfterSeconds = ShardPilotUnityWebRequestTransport.ParseRetryAfterSeconds(webRequest.GetResponseHeader("Retry-After"))
                };

                if (callback != null)
                {
                    try
                    {
                        callback(response);
                    }
                    catch
                    {
                        // Callbacks must never throw back into the dispatcher.
                    }
                }
            }
        }
    }
}
