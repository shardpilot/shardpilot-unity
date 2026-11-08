using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Fields = System.Collections.Generic.Dictionary<string, object>;

namespace ShardPilot.Tests
{
    [NonParallelizable]
    public sealed class ShardPilotConformanceTests
    {
        private const string Root = "https://conformance.example";
        private const string Key = "sp_synthetic";
        private const string CorpusHash = "cd3affb4c1732e18f83e069e7ad3ee7de6b21790e79b9e8e7fda92dd24fa483d";
        private const string ManifestHash = "89d8b31640d9caab8fa849577c537bf9cd0106ce7ca339d7e030e97102dabab6";
        private const string AdvisoryHash = "74c47858182bc41bdd279a1382bd6c63a80e82183b19312fd58e3b64dd28a6ec";
        private const string AnonHeader = "X-ShardPilot-Anonymous-Id";
        private const string ReplyWitness = "the captured reply reached the SDK";
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);

        private static string Folder
        {
            get
            {
#if UNITY_EDITOR
                var root = UnityEditor.PackageManager.PackageInfo.FindForAssembly(typeof(ShardPilotSdk).Assembly).resolvedPath;
#else
                var root = AppContext.BaseDirectory;
#endif
                return Path.Combine(root, "Tests~", "Conformance", "v1");
            }
        }

        private static string Text(Fields map, string key) => map.TryGetValue(key, out var value) ? (string)value : null;
        private static Fields Parse(string json)
        {
            Assert.That(ShardPilotJsonParser.TryParseObject(json, out var map), Is.True, "Expected a JSON object");
            return map;
        }

        private static string Hash(byte[] bytes)
        {
            using (var sha = SHA256.Create())
                return BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", "").ToLowerInvariant();
        }

        private static byte[] Bytes(string path) => File.ReadAllBytes(Path.Combine(Folder, path));
        private static string Body(Fields side) => Text(side, "file") is string path ? Utf8.GetString(Bytes(path)) : "";
        private static IEnumerable<Fields> Cases()
        {
            var bytes = Bytes("manifest.json");
            Assert.That(Hash(bytes), Is.EqualTo(ManifestHash), "manifest.json");
            return ((List<object>)Parse(Utf8.GetString(bytes))["cases"]).Cast<Fields>();
        }

        public static IEnumerable<TestCaseData> CaseIds() =>
            Cases().Select(c => new TestCaseData(Text(c, "id")).SetName("WireConformance(" + Text(c, "id") + ")"));

        [Test]
        public void WireConformancePinsEveryBody()
        {
            var cases = Cases().ToArray();
            Assert.That(cases.Select(c => Text(c, "id")).Distinct().Count(), Is.EqualTo(45));
            var digests = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var side in cases.SelectMany(c => new[] { (Fields)c["request"], (Fields)c["response"] }))
            {
                var path = Text(side, "file");
                if (path == null) continue;
                var bytes = Bytes(path);
                var digest = Hash(bytes);
                Assert.That(bytes.Length, Is.EqualTo(side["bytes"]), path);
                Assert.That(digest, Is.EqualTo(side["sha256"]), path);
                digests.Add(digest + "  " + path + "\n");
            }
            Assert.That(Directory.GetFiles(Path.Combine(Folder, "bodies")).Length, Is.EqualTo(digests.Count));
            Assert.That(digests.Count, Is.EqualTo(76));
            Assert.That(Hash(Utf8.GetBytes(string.Concat(digests))), Is.EqualTo(CorpusHash));
            Assert.That(Hash(Bytes("bodies/resolver.advisory-admitted-200.response")), Is.EqualTo(AdvisoryHash));
        }

        [TestCaseSource(nameof(CaseIds))]
        public void Replay(string id)
        {
            TestContext.Progress.WriteLine("Wire conformance: " + id);
            ReplayCase(id);
        }

        private static string ReplayOrigin() => "https://" + Guid.NewGuid().ToString("N") + ".conformance.example";

        [Test]
        public void SharedRetryScopeBlocksLaterCapturedReplies()
        {
            var shared = ReplayOrigin();
            ReplayCase("rc.rate-limited-429-positive-retry", shared);
            foreach (var id in new[] { "rc.rate-limited-429-default-retry", "rc.scope-unavailable-503" })
            {
                Assert.That(new TestDelegate(() => ReplayCase(id, shared)), Throws.TypeOf<AssertionException>().With.Message.Contains(ReplyWitness), id);
                ReplayCase(id);
                TestContext.Progress.WriteLine("Shared-scope inverse refused " + id + ". Isolated replay passed.");
            }
        }

        private static void ReplayCase(string id, string remoteConfigRoot = null)
        {
            var scene = Cases().Single(c => Text(c, "id") == id);
            var request = (Fields)scene["request"];
            var reply = (Fields)scene["response"];
            var headers = reply.TryGetValue("headers", out var capturedHeaders) ? (Fields)capturedHeaders : new Fields();
            var response = new ShardPilotHttpResponse
            {
                StatusCode = (long)reply["status"], Body = Body(reply), ETag = Text(headers, "ETag"),
                RetryAfterSeconds = ShardPilotUnityWebRequestTransport.ParseRetryAfterSeconds(Text(headers, "Retry-After"))
            };
            switch (Text(request, "path"))
            {
                case "/v1/events:batch": Batch(id, request, response); break;
                case "/v1/consent":
                    if (Text(scene, "surface") == "non_objection") NonObjection(id, request, response);
                    else Consent(id, request, response);
                    break;
                case "/api/cp/v1/consent/policy": Resolve(id, request, response); break;
                case "/api/v1/crashes/ingest": Crash(id, request, response); break;
                default: RemoteConfig(request, response, headers, remoteConfigRoot ?? ReplayOrigin()); break;
            }
        }

        private static void RequestLimit(string reason) => TestContext.Progress.WriteLine("request comparison: " + reason);

        private static void Project(Fields actual, Fields expected, params string[] keys)
        {
            foreach (var key in keys)
            {
                Assert.That(actual.ContainsKey(key), Is.EqualTo(expected.ContainsKey(key)), key);
                if (expected.ContainsKey(key)) Assert.That(actual[key], Is.EqualTo(expected[key]), key);
            }
        }

        private static void Batch(string id, Fields request, ShardPilotHttpResponse reply)
        {
            var body = Parse(reply.Body);
            if (reply.StatusCode == 202)
            {
                var result = ShardPilotUnityWebRequestTransport.ParseBatchResponse(reply.Body);
                Assert.That(result.Accepted, Is.EqualTo(body["accepted"]));
                Assert.That(result.Rejected, Is.EqualTo(body["rejected"]));
                Assert.That(result.Duplicates, Is.EqualTo(body["duplicates"]));
                var statuses = ((List<object>)body["events"]).Cast<Fields>().Select(e => Text(e, "status"));
                Assert.That(result.NonAcceptedOutcomes.Select(e => e.Status), Is.EqualTo(statuses.Where(s => s != "accepted")));
            }
            else
            {
                var error = ShardPilotUnityWebRequestTransport.BuildStatusException((int)reply.StatusCode, null, reply.Body);
                Assert.That(error.StatusCode, Is.EqualTo(reply.StatusCode));
                Assert.That(error.ErrorCode, Is.EqualTo(((Fields)body["error"])["code"]));
            }
            if (id == "batch-malformed-json" || id == "batch-retired-scope-member")
            {
                RequestLimit(id == "batch-malformed-json" ? "SDK serializer emits valid JSON." : "SDK serializer has no app_id field.");
                return;
            }
            RequestLimit("Event projection. Receiver fixture includes ingest_ts and service authentication; Unity stamps platform and uses an SDK credential.");
            if (id == "batch-size-rejected-mixed") RequestLimit("The oversized event is serialized below client admission to replay the receiver verdict.");
            if (id == "batch-suppressed-no-consent") RequestLimit("Receiver-side suppression is replayed below the client consent gate.");
            var input = ((List<object>)Parse(Body(request))["events"]).Cast<Fields>().ToArray();
            var config = Config(input[0]);
            var events = input.Select(e => new ShardPilotEvent
            {
                Id = Text(e, "event_id"), Name = Text(e, "event_name"), TimestampUtc = Instant(Text(e, "event_ts")),
                UserId = Text(e, "user_id"), AnonymousId = Text(e, "anonymous_id"), SessionId = Text(e, "session_id"),
                SessionSequence = (long)e["session_sequence"], SchemaVersion = (int)(long)e["schema_version"],
                Props = (Fields)e["props"]
            }).ToArray();
            var actual = (List<object>)Parse(ShardPilotJson.SerializeBatch(config, events))["events"];
            Assert.That(actual.Count, Is.EqualTo(input.Length));
            for (var i = 0; i < actual.Count; i++)
                Project((Fields)actual[i], input[i], "event_id", "event_name", "schema_version", "user_id", "anonymous_id",
                    "session_id", "session_sequence", "props", "workspace_key", "app_key", "environment_key");
        }

        private static DateTime Instant(string value) => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);

        private static ShardPilotConfig Config(Fields fields) => new ShardPilotConfig
        {
            AppUrl = Root, IngestUrl = Root, PublishableKey = Key, SpoolEnabled = false, HttpTimeout = TimeSpan.FromSeconds(5),
            WorkspaceKey = Text(fields, "workspace_key"), AppKey = Text(fields, "app_key"), EnvironmentKey = Text(fields, "environment_key"),
            AnonymousId = Text(fields, "anonymous_id") ?? Text(fields, "actor_identifier") ?? "synthetic-actor"
        };

        private sealed class ReplaySender : IShardPilotHttpRequestSender, IShardPilotTransport
        {
            public ShardPilotHttpResponse Reply;
            public ShardPilotHttpRequest Request;
            public string Url;
            public int Sent;
            public void Send(ShardPilotHttpRequest request, Action<ShardPilotHttpResponse> callback)
            {
                Request = request;
                var allowed = ShardPilotUnityHttpRequestSender.BeforeSendGateAllows(request);
                Url = allowed ? ShardPilotUnityHttpRequestSender.UrlForSend(request) : request.Url;
                var local = allowed ? request.BeforeDispatch?.Invoke() : ShardPilotUnityHttpRequestSender.RefusedBeforeSendResponse();
                if (local == null) Sent++;
                callback(local ?? Reply);
            }

            public Task<ShardPilotBatchResult> PublishAsync(ShardPilotTransportRequest request, CancellationToken token) =>
                throw new InvalidOperationException("Consent replay must not publish analytics.");
        }

        private static void Route(ReplaySender sender, Fields request, string root = Root)
        {
            Assert.That(sender.Sent, Is.EqualTo(1), ReplyWitness);
            Assert.That(sender.Request.Method, Is.EqualTo(request["method"]));
            Assert.That(sender.Url, Is.EqualTo(root + Text(request, "path")));
        }

        private static void Consent(string id, Fields request, ShardPilotHttpResponse reply)
        {
            var input = Parse(Body(request));
            var config = Config(input);
            var sender = new ReplaySender { Reply = reply };
            using var client = new ShardPilotClient(config, sender, false, null, sender);
            var kind = Text(input, "kind");
            var forged = id == "identity-consent-kind-forgery";
            var notice = Text(input, "notice_version");
            var entry = new ShardPilotConsentOutboxEntry(notice == null ? null :
                new ShardPilotConsentNoticeSnapshot(notice, Text(input, "notice_locale"), Text(input, "policy_version")))
            {
                WorkspaceId = config.WorkspaceKey, AppId = config.AppKey, EnvironmentId = config.EnvironmentKey,
                ActorIdentifier = config.AnonymousId, AnonymousId = config.AnonymousId, Kind = forged ? "anon" : kind,
                DecidedAt = Text(input, "decided_at"), IdempotencyKey = Text(input, "idempotency_key"),
                AnalyticsGranted = (bool)((Fields)input["categories"])["analytics"]
            };
            RequestLimit(forged ? "Publishable receipts cannot assert a verified actor." :
                id == "identity-consent-subject-mismatch" ? "The captured refusal uses a token for a different subject." :
                id == "consent-retired-scope-member" ? "Receipt serializer has no app_id field." :
                id == "consent-grant-publishable-refused" ? "Publishable dispatch can only send denials." :
                "Receipt field projection at the sender seam; member order and service credential differ from the capture.");
            var usedPublishable = Text(request, "credential") == "publishable_key" && !entry.AnalyticsGranted;
            typeof(ShardPilotClient).GetMethod("SendConsentReceipt", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(client, new object[] { Key, entry, usedPublishable });
            Route(sender, request);
            var actual = Parse(sender.Request.Body);
            Assert.That(actual.ContainsKey("app_id"), Is.False);
            Project(actual, input, "workspace_key", "app_key", "environment_key", "actor_identifier", "categories",
                "decided_at", "idempotency_key", "notice_version", "notice_locale", "policy_version");
            Assert.That(actual["kind"], Is.EqualTo(forged ? "anon" : kind));
            Assert.That(client.Snapshot().ConsentRecorded, Is.EqualTo(reply.StatusCode == 200 ? 1UL : 0UL));
            Assert.That(client.Snapshot().ConsentFailed, Is.EqualTo(reply.StatusCode == 200 ? 0UL : 1UL));
        }

        private static void NonObjection(string id, Fields request, ShardPilotHttpResponse reply)
        {
            var input = Parse(Body(request));
            var config = Config(input);
            var verified = Text(input, "kind") == "user_verified";
            if (Text(request, "credential") == "client_jwt")
                config.TokenProvider = _ => Task.FromResult(new ShardPilotTokenResult("synthetic-jwt"));
            if (verified) config.UserId = Text(input, "actor_identifier");
            RequestLimit(id == "nonobjection-mixed-categories" ? "Recording has no categories member." :
                id == "nonobjection-retired-scope-member" ? "Recording has no app_id member." :
                id == "nonobjection-service-refused" ? "The client offers publishable or provider authentication, never a service credential." :
                "Recording generates its own timestamp and idempotency key; stable fields are compared.");
            var sender = new ReplaySender { Reply = reply };
            using var client = new ShardPilotClient(config, sender, false, null, sender);
            var completed = new TaskCompletionSource<ShardPilotResult>();
            client.RecordNonObjection(ShardPilotNonObjectionReceipt.Host(input), value => completed.TrySetResult(value));
            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (!completed.Task.IsCompleted && deadline.Elapsed < TimeSpan.FromSeconds(6))
            {
                ShardPilotDispatcher.Instance.DrainHostCalls();
                Thread.Sleep(1);
            }
            Assert.That(completed.Task.IsCompleted, Is.True, id);
            Route(sender, request);
            var actual = Parse(sender.Request.Body);
            Assert.That(actual.ContainsKey("categories") || actual.ContainsKey("app_id"), Is.False);
            Project(actual, input, "workspace_key", "app_key", "environment_key", "actor_identifier", "kind", "admission_basis",
                "purpose", "advisory", "age_evidence_class", "attestation", "notice_version", "notice_locale", "policy_version");
            var result = completed.Task.Result;
            Assert.That(result.Success, Is.EqualTo(id == "nonobjection-recorded"));
            if (!result.Success)
                Assert.That(result.Error, Is.EqualTo(reply.StatusCode == 200 ? "non_objection_response_invalid" : "http_" + reply.StatusCode));
            Assert.That(client.ConsentState, Is.EqualTo(ShardPilotConsentState.Unknown));
        }

        private static void Resolve(string id, Fields request, ShardPilotHttpResponse reply)
        {
            var input = Parse(Body(request));
            var body = Parse(reply.Body);
            var sender = new ReplaySender { Reply = reply };
            var retired = id == "resolver.refusal-retired-key-400";
            RequestLimit(retired ? "Public policy context has no workspace_id member." :
                "Policy field projection. The capture includes a null store_region member that Unity omits.");
            var context = new ShardPilotConsentPolicyContext
            {
                AppUrl = Root, WorkspaceKey = Text(input, "workspace_key"), AppKey = Text(input, "app_key"),
                EnvironmentKey = Text(input, "environment_key"), Locale = Text(input, "locale"), Platform = Text(input, "platform"),
                AppVersion = Text(input, "app_version"), Store = Text(input, "store"), Advisory = input.ContainsKey("advisory")
            };
            var previousSender = ShardPilotConsentPolicy.SenderFactoryForTesting;
            var previousClock = ShardPilotConsentPolicy.ClockForTesting;
            try
            {
                ShardPilotConsentPolicy.SenderFactoryForTesting = () => sender;
                ShardPilotConsentPolicy.ClockForTesting = () => new DateTimeOffset(Instant(Text(body, "expires_at"))).ToUnixTimeSeconds() - 60;
                ShardPilotConsentPolicyDecision result = null;
                ShardPilotSdk.PrepareConsentPolicy(context, value => result = value);
                if (retired)
                {
                    Assert.That(sender.Sent, Is.Zero);
                    Assert.That(result.Reason, Is.EqualTo("invalid_request"));
                    context.WorkspaceKey = "synthetic-workspace";
                    ShardPilotSdk.PrepareConsentPolicy(context, value => result = value);
                }
                Route(sender, request);
                var actual = Parse(sender.Request.Body);
                Assert.That(actual.ContainsKey("workspace_id") || actual.ContainsKey("store_region"), Is.False);
                Project(actual, input, "app_key", "environment_key", "app_version", "locale", "platform", "store", "advisory");
                if (!retired) Project(actual, input, "workspace_key");
                Assert.That(result.Reason, Is.EqualTo(Text(body, "reason") ?? "plan_unsigned"));
                Assert.That(result.Advisory != null, Is.EqualTo(body.ContainsKey("advisory")));
                if (result.Advisory != null)
                    Assert.That(result.Advisory.Jurisdiction, Is.EqualTo(((Fields)body["advisory"])["jurisdiction"]));
                Assert.That(result.PlanUsed, Is.False);
            }
            finally
            {
                ShardPilotConsentPolicy.SenderFactoryForTesting = previousSender;
                ShardPilotConsentPolicy.ClockForTesting = previousClock;
                ShardPilotConsentPolicy.Invalidate();
            }
        }

        private static void RemoteConfig(Fields request, ShardPilotHttpResponse reply, Fields responseHeaders, string root)
        {
            var headers = (Fields)request["headers"];
            var parts = Text(request, "path").Split('/');
            var tag = Text(headers, "If-None-Match");
            var anonymous = Text(headers, AnonHeader);
            var identified = anonymous != null;
            var sender = new ReplaySender();
            var config = new ShardPilotConfig
            {
                AppUrl = root, WorkspaceKey = parts[3], EnvironmentKey = parts[4], AnonymousId = anonymous,
                RemoteConfigEnabled = true, PublishableKey = Key
            };
            if (Text(request, "credential") == "none")
                RequestLimit("Missing-auth capture is receiver-authored. The SDK sends its configured credential.");
            var host = ShardPilotDispatcher.Instance;
            var client = new ShardPilotRemoteConfig(config, null, sender, () => identified ?
                ShardPilotConsentState.Granted : ShardPilotConsentState.Unknown, () => 1, () => new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            ShardPilotRemoteConfigResult result = null;
            void Fetch()
            {
                Assert.That(client.FetchRemoteConfig(value => result = value), Is.True);
                host.DrainHostCalls();
            }
            if (tag != null)
            {
                var prior = reply.StatusCode == 304
                    ? Utf8.GetString(Bytes("bodies/rc.two-key-200.response"))
                    : "{\"version\":0,\"values\":{}}";
                sender.Reply = new ShardPilotHttpResponse { StatusCode = 200, ETag = tag, Body = prior };
                Fetch();
                Assert.That(result.Success, Is.True, "seeded cache");
            }
            sender.Sent = 0;
            sender.Reply = reply;
            Fetch();
            Route(sender, request, root);
            Assert.That(sender.Request.Headers ?? new Dictionary<string, string>(), Is.EqualTo(headers.ToDictionary(p => p.Key, p => (string)p.Value)));
            Assert.That(result.Success, Is.EqualTo(reply.StatusCode == 200 || reply.StatusCode == 304));
            Assert.That(result.FromCache, Is.EqualTo(reply.StatusCode == 304));
            if (reply.StatusCode == 200)
                Assert.That(result.Values, Is.EqualTo(Parse(reply.Body)["values"]));
            if (reply.StatusCode == 304)
                Assert.That(result.Values, Is.EqualTo(Parse(Utf8.GetString(Bytes("bodies/rc.two-key-200.response")))["values"]));
            if (responseHeaders.TryGetValue("Retry-After", out var retry))
            {
                Assert.That(result.Error, Does.EndWith("status " + reply.StatusCode));
                Assert.That(reply.RetryAfterSeconds, Is.EqualTo(int.Parse((string)retry, CultureInfo.InvariantCulture)));
                var sent = sender.Sent;
                Fetch();
                Assert.That(sender.Sent, Is.EqualTo(sent), "Retry-After gates another dispatch");
            }
        }

        private static void Crash(string id, Fields request, ShardPilotHttpResponse reply)
        {
            var input = Parse(Body(request));
            var app = (Fields)input["app"];
            RequestLimit(id == "json_retired" ? "Crash serializer has no app.id member." :
                input.ContainsKey("user_id") ? "Crash sender never asserts user_id. It can send a consented anonymous ID." :
                "Crash projection. Unity synthesizes thread metadata and omits native module lists.");
            var config = new ShardPilotConfig { AppKey = Text(app, "key"), AnonymousId = Text(input, "anonymous_id") };
            var report = new ShardPilotCrashEvent
            {
                CrashId = Text(input, "crash_id"), OccurredAtUtc = Instant(Text(input, "occurred_at")), AppVersion = Text(app, "version"),
                Platform = Text(input, "platform"), CrashType = Text((Fields)input["exception"], "type")
            };
            var wire = Parse(ShardPilotJson.SerializeCrashIngest(config, report, config.AnonymousId != null));
            var wireApp = (Fields)wire["app"];
            Project(wireApp, app, "key", "version");
            Project(wire, input, "crash_id", "anonymous_id");
            Assert.That(wireApp.ContainsKey("id") || wire.ContainsKey("user_id"), Is.False);
            var result = (ShardPilotResult)typeof(ShardPilotClient).GetMethod("ClassifyCrashResponse", BindingFlags.Static | BindingFlags.NonPublic)
                .Invoke(null, new object[] { reply });
            Assert.That(result.Success, Is.EqualTo(reply.StatusCode == 202));
            Assert.That(result.Error, Is.EqualTo(reply.StatusCode == 202 ? null : "status " + reply.StatusCode));
            Assert.That(result.Retryable, Is.False);
        }
    }
}
