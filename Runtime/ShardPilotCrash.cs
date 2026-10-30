using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using UnityEngine;

namespace ShardPilot
{
    internal sealed class ShardPilotCrashHooks : IDisposable
    {
        private readonly ShardPilotClient _client;
        private readonly ShardPilotCrashRuntimeInfo _runtimeInfo;
        private readonly ShardPilotAnrLane _anrLane;
        private bool _disposed;
        private int _emitting;

        private ShardPilotCrashHooks(ShardPilotClient client)
        {
            _client = client;
            ShardPilotCrashThread.CaptureMainThread();
            _runtimeInfo = ShardPilotCrashRuntimeInfo.CaptureFromUnity();
            Application.logMessageReceivedThreaded += OnLogMessageReceivedThreaded;
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

            // The Android ANR lane: the next-launch
            // ApplicationExitInfo pass and the opt-in stall watchdog. Both
            // are individually flag-gated in static config (dark by
            // default), Android-only, and never throw into Install — a
            // broken ANR lane must not cost the managed-exception hooks
            // above.
            _anrLane = ShardPilotAnrLane.TryStart(client);
        }

        internal static ShardPilotCrashHooks Install(ShardPilotClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException("client");
            }

            return new ShardPilotCrashHooks(client);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Application.logMessageReceivedThreaded -= OnLogMessageReceivedThreaded;
            AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
            if (_anrLane != null)
            {
                _anrLane.Stop();
            }
        }

        private void OnLogMessageReceivedThreaded(string condition, string stackTrace, LogType type)
        {
            if (type != LogType.Exception)
            {
                return;
            }

            EmitOnce(_runtimeInfo.Apply(ShardPilotCrashEvent.FromUnityLog(condition, stackTrace, false)), false);
        }

        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs args)
        {
            var fatal = args.IsTerminating;
            var exception = args.ExceptionObject as Exception;
            if (exception == null)
            {
                EmitOnce(_runtimeInfo.Apply(ShardPilotCrashEvent.FromException(null, fatal)), fatal);
                return;
            }

            EmitOnce(_runtimeInfo.Apply(ShardPilotCrashEvent.FromException(exception, fatal)), fatal);
        }

        private void EmitOnce(ShardPilotCrashEvent crashEvent, bool waitForSend)
        {
            if (_disposed || _client == null)
            {
                return;
            }

            if (System.Threading.Interlocked.Exchange(ref _emitting, 1) == 1)
            {
                return;
            }

            try
            {
                // The crash ships on the dedicated crash endpoint, not
                // the analytics flush. For a FATAL crash captured off the main
                // thread, block briefly on the send completing so the report
                // leaves the process before it terminates (the dispatcher pumps
                // the request on the main thread); on the main thread we cannot
                // block the pump that drives the send.
                var sent = new System.Threading.ManualResetEventSlim(false);
                ShardPilotCrashCaptureDurability ignoredDurability;
                var dispatched = _client.CaptureCrash(crashEvent, _ => sent.Set(), out ignoredDurability);
                if (sent != null && dispatched && waitForSend && !ShardPilotCrashThread.IsMainThread())
                {
                    sent.Wait(TimeSpan.FromSeconds(2));
                }

                sent.Dispose();
            }
            catch
            {
                // Crash hooks must never throw back into Unity or the CLR
                // unhandled-exception path.
            }
            finally
            {
                System.Threading.Interlocked.Exchange(ref _emitting, 0);
            }
        }
    }

#nullable enable annotations
    public sealed class ShardPilotCrashEvent
    {
        public string? CrashId { get; set; }
        public string? AppVersion { get; set; }
        public string? BuildId { get; set; }
        public string? OSName { get; set; }
        public string? OSVersion { get; set; }
        public string? DeviceClass { get; set; }
        public List<ShardPilotCrashFrame>? StackFrames { get; set; } = new List<ShardPilotCrashFrame>();
        public List<ShardPilotBreadcrumb>? Breadcrumbs { get; set; } = new List<ShardPilotBreadcrumb>();
        public string? ThreadState { get; set; }
        public string? SessionId { get; set; }
        public DateTime OccurredAtUtc { get; set; }
        public string? CrashType { get; set; }

        /// <summary>
        /// Optional human-readable exception reason/message. Sanitized like
        /// every other free-text field before it reaches the wire
        /// (<c>exception.reason</c>).
        /// </summary>
        public string? ExceptionReason { get; set; }

        /// <summary>
        /// Optional per-event component slug. When empty the
        /// client's configured <see cref="ShardPilotConfig.CrashComponent"/> is
        /// stamped instead. A per-event value wins.
        /// </summary>
        public string? Source { get; set; }

        /// <summary>
        /// Optional platform override. Fresh reports fold it to the SDK's closed
        /// platform vocabulary; an unmappable value uses other. When empty the
        /// client's configured or detected platform is stamped instead.
        /// </summary>
        public string? Platform { get; set; }

        /// <summary>
        /// Optional free-form context and metadata maps attached to the crash
        /// (additionalProperties:string on the wire). Keys are validated and values
        /// are PII-scrubbed before send; entries with an invalid key or a value that
        /// scrubs to empty are dropped.
        /// </summary>
        public Dictionary<string, string?>? Context { get; set; }

        public Dictionary<string, string?>? Metadata { get; set; }

        public bool Fatal { get; set; }

        /// <summary>
        /// The 1-in-N rate that admitted this non-fatal report. Stamped by
        /// CaptureCrash after sanitization, never copied from an input event.
        /// Zero means unknown; the serializer omits rates the wire cannot represent.
        /// </summary>
        internal int NonFatalSampleOneIn { get; set; }

        /// <summary>
        /// Optional event kind for the crash kind enum. Empty (the
        /// default) means the base <c>crash</c> kind and is OMITTED on the
        /// wire; the ANR lane sets <c>"anr"</c>.
        /// Internal: only the SDK's own capture lanes
        /// stamp it, and the sanitizer drops anything outside the closed set.
        /// </summary>
        internal string Kind { get; set; }

        /// <summary>
        /// Optional deterministic idempotency key for ApplicationExitInfo-
        /// derived and watchdog ANR reports
        /// (<c>aei_record_key</c>): a stable SHA-256-derived UUID-form hash of
        /// the exit-record identity (or of the era + stall start for the
        /// in-session watchdog), so a replayed record collapses server-side
        /// on the partial unique index. Empty = omitted (ordinary crashes
        /// never carry it).
        /// </summary>
        internal string AeiRecordKey { get; set; }

        /// <summary>
        /// Optional raw stack/trace text (the ingest schema's top-level
        /// <c>raw_text</c>): the ANR lane ships the bounded
        /// ApplicationExitInfo trace here for server-side parsing. Free text:
        /// it rides the SERVER-side scrub/drop pipeline
        /// (the path/token/email rules), so the client
        /// bounds its size but deliberately does not apply the local
        /// drop-the-whole-string PII heuristics that would destroy an entire
        /// thread dump over one matching line.
        /// </summary>
        internal string RawText { get; set; }

        /// <summary>
        /// When true, actor attribution is FROZEN to the process era the
        /// event came from: the wire body carries
        /// <see cref="EraAnonymousId"/> instead of the CURRENT config's
        /// anonymous id, and no session id at all — a next-launch AEI report
        /// must never borrow the next launch's login or session. An empty
        /// <see cref="EraAnonymousId"/> means device-scoped anonymous (no
        /// actor fields on the wire).
        /// </summary>
        internal bool UseEraAttribution { get; set; }

        /// <summary>The era subject's anonymous id; see <see cref="UseEraAttribution"/>.</summary>
        internal string EraAnonymousId { get; set; }

        /// <summary>
        /// When true, the breadcrumb-ring fallback is suppressed: a
        /// next-launch AEI report describes a PREVIOUS process era, and the
        /// current session's breadcrumbs would attach the wrong era's (and
        /// possibly the wrong subject's) context to it.
        /// </summary>
        internal bool OmitBreadcrumbs { get; set; }

        public static ShardPilotCrashEvent FromException(Exception? exception, bool fatal)
        {
            var crashType = exception == null ? "exception" : exception.GetType().FullName;
            return new ShardPilotCrashEvent
            {
                CrashType = crashType,
                ExceptionReason = exception == null ? string.Empty : exception.Message,
                Fatal = fatal,
                OccurredAtUtc = DateTime.UtcNow,
                StackFrames = ShardPilotCrashFrames.Parse(exception == null ? string.Empty : exception.StackTrace),
                ThreadState = ShardPilotCrashThread.MainThreadState(),
            };
        }

        public static ShardPilotCrashEvent FromUnityLog(string? condition, string? stackTrace, bool fatal)
        {
            return new ShardPilotCrashEvent
            {
                CrashType = ShardPilotCrashSanitizer.SafeCrashType(condition),
                ExceptionReason = ShardPilotCrashSanitizer.ExceptionReasonFromCondition(condition),
                Fatal = fatal,
                OccurredAtUtc = DateTime.UtcNow,
                StackFrames = ShardPilotCrashFrames.Parse(stackTrace),
                ThreadState = ShardPilotCrashThread.MainThreadState(),
            };
        }

    }

#nullable restore annotations
    internal sealed class ShardPilotCrashRuntimeInfo
    {
        private readonly string _osName;
        private readonly string _osVersion;
        private readonly string _deviceClass;

        private ShardPilotCrashRuntimeInfo(string osName, string osVersion, string deviceClass)
        {
            _osName = osName;
            _osVersion = osVersion;
            _deviceClass = deviceClass;
        }

        public static ShardPilotCrashRuntimeInfo CaptureFromUnity()
        {
            return new ShardPilotCrashRuntimeInfo(
                SystemInfo.operatingSystemFamily.ToString().ToLowerInvariant(),
                SystemInfo.operatingSystem,
                ShardPilotCrashDevice.DeviceClass());
        }

        public ShardPilotCrashEvent Apply(ShardPilotCrashEvent crashEvent)
        {
            if (crashEvent == null)
            {
                return null;
            }

            if (string.IsNullOrEmpty(crashEvent.OSName))
            {
                crashEvent.OSName = _osName;
            }

            if (string.IsNullOrEmpty(crashEvent.OSVersion))
            {
                crashEvent.OSVersion = _osVersion;
            }

            if (string.IsNullOrEmpty(crashEvent.DeviceClass))
            {
                crashEvent.DeviceClass = _deviceClass;
            }

            return crashEvent;
        }
    }

#nullable enable annotations
    public sealed class ShardPilotCrashFrame
    {
        public string? Function { get; set; }
        public string? File { get; set; }
        public int Line { get; set; }
        public string? Module { get; set; }
    }

    public sealed class ShardPilotBreadcrumb
    {
        public string? Name { get; set; }
        public DateTime TimestampUtc { get; set; }
    }

#nullable restore annotations
    internal sealed class ShardPilotBreadcrumbRing
    {
        public const int MaxBreadcrumbs = 50;
        private readonly ShardPilotBreadcrumb[] _entries = new ShardPilotBreadcrumb[MaxBreadcrumbs];
        private readonly object _gate = new object();
        private int _next;
        private int _count;

        public void Record(string name)
        {
            string sanitized;
            if (!ShardPilotCrashSanitizer.TrySanitizeBreadcrumbName(name, out sanitized))
            {
                return;
            }

            lock (_gate)
            {
                _entries[_next] = new ShardPilotBreadcrumb
                {
                    Name = sanitized,
                    TimestampUtc = DateTime.UtcNow
                };
                _next = (_next + 1) % _entries.Length;
                if (_count < _entries.Length)
                {
                    _count++;
                }
            }
        }

        public void Clear()
        {
            lock (_gate)
            {
                Array.Clear(_entries, 0, _entries.Length);
                _next = 0;
                _count = 0;
            }
        }

        public IReadOnlyList<ShardPilotBreadcrumb> Snapshot()
        {
            lock (_gate)
            {
                var result = new List<ShardPilotBreadcrumb>(_count);
                if (_count == 0)
                {
                    return result;
                }

                var start = _count == _entries.Length ? _next : 0;
                for (var i = 0; i < _count; i++)
                {
                    var breadcrumb = _entries[(start + i) % _entries.Length];
                    result.Add(new ShardPilotBreadcrumb
                    {
                        Name = breadcrumb.Name,
                        TimestampUtc = breadcrumb.TimestampUtc
                    });
                }

                return result;
            }
        }
    }

    internal static class ShardPilotCrashSanitizer
    {
        private const int MaxStackFrames = 256;
        private static readonly Regex AnalyticsEventNamePattern = new Regex("^[A-Za-z][A-Za-z0-9_.:-]{0,127}$");
        private static readonly Regex Ipv4Pattern = new Regex("(?:^|[^0-9])((?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9]))(?:$|[^0-9])");
        private static readonly Regex Ipv6CandidatePattern = new Regex("[0-9A-Fa-f:.]{3,}");
        private static readonly Regex JwtPattern = new Regex("(?:^|[^A-Za-z0-9_-])([A-Za-z0-9_-]{4,}\\.[A-Za-z0-9_-]{4,}\\.[A-Za-z0-9_-]{4,})(?:$|[^A-Za-z0-9_-])");
        private static readonly string[] DisallowedPrefixes = {"player_", "user_", "customer_", "device_"};
        private static readonly Regex MapKeyPattern = new Regex("^[A-Za-z][A-Za-z0-9_.:-]{0,63}$");

        public static ShardPilotCrashEvent Prepare(ShardPilotCrashEvent ev, IReadOnlyList<ShardPilotBreadcrumb> fallbackBreadcrumbs)
        {
            var copy = Clone(ev);
            copy.CrashId = ShardPilotConfig.Trim(copy.CrashId);
            if (string.IsNullOrEmpty(copy.CrashId))
            {
                copy.CrashId = ShardPilotIds.NewUuidV7();
            }

            if (!ShardPilotIds.IsUuidV7(copy.CrashId))
            {
                copy.CrashId = ShardPilotIds.NewUuidV7();
            }

            copy.AppVersion = SanitizeString(copy.AppVersion);
            copy.BuildId = SanitizeString(copy.BuildId);
            copy.OSName = SanitizeString(copy.OSName);
            copy.OSVersion = SanitizeString(copy.OSVersion);
            copy.DeviceClass = SanitizeString(copy.DeviceClass);
            copy.ThreadState = SanitizeThreadState(copy.ThreadState);
            copy.CrashType = SafeCrashType(copy.CrashType);
            copy.ExceptionReason = SanitizeString(copy.ExceptionReason);
            // Source is resolved against the configured default in the client
            // (ResolveCrashSource), which must distinguish a bad caller override
            // from an absent one — so the RAW caller value is preserved here and
            // not pre-scrubbed (a pre-scrub would erase that distinction).
            copy.Platform = SanitizeString(copy.Platform);
            copy.Context = SanitizeStringMap(copy.Context);
            copy.Metadata = SanitizeStringMap(copy.Metadata);
            copy.SessionId = SanitizeSessionId(copy.SessionId);
            copy.OccurredAtUtc = copy.OccurredAtUtc == default(DateTime)
                ? DateTime.UtcNow
                : copy.OccurredAtUtc.ToUniversalTime();

            // ANR-lane fields — all additive and normally
            // empty. Kind is a CLOSED set ("anr" is the only non-default
            // value this SDK ships); anything else is dropped to the default
            // rather than reaching the wire. The record key must be the
            // canonical lowercase UUID form the key derivation produces. The
            // era anonymous id rides EXACTLY the live path's acceptance rule
            // for the config's anonymous id (see ClampEraAnonymousId) — same
            // subject, same acceptance on both lanes; a value outside the
            // clamp degrades to device-scoped anonymous, never to the
            // current login. RawText is size-bounded only — its PII
            // scrubbing is server-side by design (see the field's doc).
            copy.Kind = copy.Kind == ShardPilotAnrIds.AnrKind ? ShardPilotAnrIds.AnrKind : string.Empty;
            copy.AeiRecordKey = ShardPilotAnrIds.IsRecordKeyShape(copy.AeiRecordKey)
                ? copy.AeiRecordKey
                : string.Empty;
            copy.RawText = ShardPilotAnrIds.BoundRawText(copy.RawText);
            copy.EraAnonymousId = ClampEraAnonymousId(copy.EraAnonymousId);

            copy.StackFrames = SanitizeFrames(copy.StackFrames);
            copy.Breadcrumbs = copy.OmitBreadcrumbs
                ? new List<ShardPilotBreadcrumb>()
                : SanitizeBreadcrumbs(copy.Breadcrumbs != null && copy.Breadcrumbs.Count > 0
                    ? copy.Breadcrumbs
                    : fallbackBreadcrumbs);
            return copy;
        }

        public static string SafeCrashType(string value)
        {
            value = SanitizeString(value);
            if (string.IsNullOrEmpty(value))
            {
                return "exception";
            }

            var colon = value.IndexOf(':');
            if (colon > 0)
            {
                value = value.Substring(0, colon);
            }

            var newline = value.IndexOfAny(new[] {'\r', '\n'});
            if (newline >= 0)
            {
                value = value.Substring(0, newline);
            }

            if (value.Length > 128)
            {
                value = value.Substring(0, 128);
            }

            return value;
        }

        /// <summary>
        /// Extracts the human-readable reason from a Unity log
        /// <c>condition</c> string of the shape <c>Type: message</c> — the part
        /// after the first colon, sanitized and single-lined. Returns empty when
        /// there is no message segment.
        /// </summary>
        public static string ExceptionReasonFromCondition(string condition)
        {
            condition = ShardPilotConfig.Trim(condition);
            if (string.IsNullOrEmpty(condition))
            {
                return string.Empty;
            }

            var colon = condition.IndexOf(':');
            var reason = colon >= 0 && colon + 1 < condition.Length
                ? condition.Substring(colon + 1)
                : string.Empty;

            reason = ShardPilotConfig.Trim(reason);
            if (string.IsNullOrEmpty(reason))
            {
                return string.Empty;
            }

            var newline = reason.IndexOfAny(new[] {'\r', '\n'});
            if (newline >= 0)
            {
                reason = reason.Substring(0, newline);
            }

            return SanitizeString(reason);
        }

        public static bool TrySanitizeBreadcrumbName(string name, out string sanitized)
        {
            sanitized = ShardPilotConfig.Trim(name);
            if (string.IsNullOrEmpty(sanitized))
            {
                return false;
            }

            if (!AnalyticsEventNamePattern.IsMatch(sanitized))
            {
                return false;
            }

            if (ContainsDisallowedContent(sanitized))
            {
                return false;
            }

            return true;
        }

        private static ShardPilotCrashEvent Clone(ShardPilotCrashEvent ev)
        {
            return new ShardPilotCrashEvent
            {
                CrashId = ev.CrashId,
                AppVersion = ev.AppVersion,
                BuildId = ev.BuildId,
                OSName = ev.OSName,
                OSVersion = ev.OSVersion,
                DeviceClass = ev.DeviceClass,
                StackFrames = ev.StackFrames == null ? new List<ShardPilotCrashFrame>() : new List<ShardPilotCrashFrame>(ev.StackFrames),
                Breadcrumbs = ev.Breadcrumbs == null ? new List<ShardPilotBreadcrumb>() : new List<ShardPilotBreadcrumb>(ev.Breadcrumbs),
                ThreadState = ev.ThreadState,
                SessionId = ev.SessionId,
                OccurredAtUtc = ev.OccurredAtUtc,
                CrashType = ev.CrashType,
                ExceptionReason = ev.ExceptionReason,
                Source = ev.Source,
                Platform = ev.Platform,
                Context = ev.Context == null ? null : new Dictionary<string, string>(ev.Context),
                Metadata = ev.Metadata == null ? null : new Dictionary<string, string>(ev.Metadata),
                Fatal = ev.Fatal,
                Kind = ev.Kind,
                AeiRecordKey = ev.AeiRecordKey,
                RawText = ev.RawText,
                UseEraAttribution = ev.UseEraAttribution,
                EraAnonymousId = ev.EraAnonymousId,
                OmitBreadcrumbs = ev.OmitBreadcrumbs
            };
        }

        private static List<ShardPilotCrashFrame> SanitizeFrames(IList<ShardPilotCrashFrame> frames)
        {
            var result = new List<ShardPilotCrashFrame>();
            if (frames == null)
            {
                return result;
            }

            var count = Math.Min(frames.Count, MaxStackFrames);
            for (var i = 0; i < count; i++)
            {
                var frame = frames[i];
                if (frame == null)
                {
                    continue;
                }

                result.Add(new ShardPilotCrashFrame
                {
                    Function = SanitizeString(frame.Function),
                    File = SanitizeString(frame.File),
                    Line = frame.Line < 0 ? 0 : frame.Line,
                    Module = SanitizeString(frame.Module)
                });
            }

            return result;
        }

        private static List<ShardPilotBreadcrumb> SanitizeBreadcrumbs(IReadOnlyList<ShardPilotBreadcrumb> breadcrumbs)
        {
            var result = new List<ShardPilotBreadcrumb>();
            if (breadcrumbs == null)
            {
                return result;
            }

            var start = Math.Max(0, breadcrumbs.Count - ShardPilotBreadcrumbRing.MaxBreadcrumbs);
            for (var i = start; i < breadcrumbs.Count; i++)
            {
                if (breadcrumbs[i] == null)
                {
                    continue;
                }

                string name;
                if (!TrySanitizeBreadcrumbName(breadcrumbs[i].Name, out name))
                {
                    continue;
                }

                result.Add(new ShardPilotBreadcrumb
                {
                    Name = name,
                    TimestampUtc = breadcrumbs[i].TimestampUtc == default(DateTime)
                        ? DateTime.UtcNow
                        : breadcrumbs[i].TimestampUtc.ToUniversalTime()
                });
            }

            return result;
        }

        private static string SanitizeThreadState(string value)
        {
            value = SanitizeString(value);
            return value == "background" ? "background" : "main";
        }

        public static string SanitizeSessionId(string value)
        {
            value = ShardPilotConfig.Trim(value);
            return ContainsDisallowedContent(value) ? string.Empty : value;
        }

        /// <summary>
        /// The era-attributed anonymous id rides EXACTLY the
        /// acceptance rule the live path applies to
        /// <see cref="ShardPilotConfig.AnonymousId"/> — the
        /// identifier length clamp (non-empty, within 512 UTF-8 bytes) —
        /// and deliberately NOT the session-id shape heuristics: a live
        /// crash body carries the config's already-clamped anonymous id
        /// verbatim (it is hashed into the canonical actor key at ingest,
        /// never stored raw), so an '@'-containing or 'device_'-prefixed
        /// anonymous id an integration legitimately configured must
        /// attribute IDENTICALLY whether the report ships live or from an
        /// era snapshot. Same subject, same acceptance on both lanes. A
        /// value outside the clamp degrades to empty — device-scoped
        /// anonymous, never a substitute identity.
        /// </summary>
        private static string ClampEraAnonymousId(string value)
        {
            value = ShardPilotConfig.Trim(value);
            return ShardPilotConsentOutbox.IsValidIdentifier(value) ? value : string.Empty;
        }

        /// <summary>
        /// Resolves the component <c>component</c> stamped on a crash report,
        /// distinguishing "the caller gave a bad override" from "the caller gave
        /// nothing" WITHOUT throwing (the crash path must never throw):
        /// <list type="bullet">
        /// <item>An empty/absent caller source falls back to the configured
        /// <paramref name="configDefault"/> (already slug-validated at config
        /// time) — the ordinary bare-app vs configured-component choice.</item>
        /// <item>A NON-EMPTY caller override that PII-scrubs to empty or is not
        /// a valid slug is OMITTED (empty result, bare app). It is deliberately
        /// NOT replaced with the configured default: a caller who supplied an
        /// explicit-but-bad override should not be silently re-attributed to a
        /// different component. Omitting also keeps a malformed value from ever
        /// reaching the wire raw (which could 400 the report and drop even a
        /// FATAL crash).</item>
        /// </list>
        /// </summary>
        public static string ResolveCrashSource(string callerSource, string configDefault)
        {
            // Distinguish absent from present BEFORE scrubbing: a non-empty
            // caller value that scrubs away is a bad override (omit), not an
            // absent one (fall back to the default).
            var callerGaveSource = !string.IsNullOrEmpty(ShardPilotConfig.Trim(callerSource));
            var sanitized = SanitizeString(callerSource);
            if (!string.IsNullOrEmpty(sanitized) && ShardPilotConfig.IsCrashSourceSlug(sanitized))
            {
                return sanitized;
            }

            if (callerGaveSource)
            {
                // Explicit-but-bad override: omit (bare app), never the default.
                return string.Empty;
            }

            // Absent/empty caller source: stamp the configured default.
            return configDefault ?? string.Empty;
        }

        public static string SanitizeOptionalString(string value)
        {
            return SanitizeString(value);
        }

        // Scrubs a free-form string map for the wire: drops an entry whose key is not a
        // valid map key or carries disallowed content, and whose value scrubs to empty.
        // Returns null when nothing survives, so the field is omitted entirely.
        private static Dictionary<string, string> SanitizeStringMap(IDictionary<string, string> map)
        {
            if (map == null || map.Count == 0)
            {
                return null;
            }

            var result = new Dictionary<string, string>();
            foreach (var entry in map)
            {
                var key = ShardPilotConfig.Trim(entry.Key);
                if (!MapKeyPattern.IsMatch(key) || ContainsDisallowedContent(key))
                {
                    continue;
                }

                var value = SanitizeString(entry.Value);
                if (string.IsNullOrEmpty(value))
                {
                    continue;
                }

                result[key] = value;
            }

            return result.Count == 0 ? null : result;
        }

        private static string SanitizeString(string value)
        {
            value = ShardPilotConfig.Trim(value);
            if (string.IsNullOrEmpty(value) || ContainsDisallowedContent(value))
            {
                return string.Empty;
            }

            return value;
        }

        private static bool ContainsDisallowedContent(string value)
        {
            value = ShardPilotConfig.Trim(value);
            if (string.IsNullOrEmpty(value))
            {
                return false;
            }

            if (value.IndexOf('@') >= 0)
            {
                return true;
            }

            var lowered = value.ToLowerInvariant();
            for (var i = 0; i < DisallowedPrefixes.Length; i++)
            {
                if (lowered.StartsWith(DisallowedPrefixes[i], StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return Ipv4Pattern.IsMatch(value) || ContainsIpv6(value) || JwtPattern.IsMatch(value);
        }

        private static bool ContainsIpv6(string value)
        {
            var matches = Ipv6CandidatePattern.Matches(value);
            for (var i = 0; i < matches.Count; i++)
            {
                var candidate = matches[i].Value.Trim('[', ']', '(', ')', '{', '}', '<', '>', ',', ';');
                if (candidate.IndexOf(':') < 0)
                {
                    continue;
                }

                IPAddress parsed;
                if (IPAddress.TryParse(candidate, out parsed))
                {
                    return true;
                }
            }

            return false;
        }
    }

    internal static class ShardPilotCrashFrames
    {
        public static List<ShardPilotCrashFrame> Parse(string stackTrace)
        {
            var result = new List<ShardPilotCrashFrame>();
            if (string.IsNullOrEmpty(stackTrace))
            {
                return result;
            }

            var lines = stackTrace.Split(new[] {'\r', '\n'}, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < lines.Length && result.Count < 256; i++)
            {
                var frame = ParseLine(lines[i]);
                if (frame != null)
                {
                    result.Add(frame);
                }
            }

            return result;
        }

        private static ShardPilotCrashFrame ParseLine(string line)
        {
            line = ShardPilotConfig.Trim(line);
            if (string.IsNullOrEmpty(line))
            {
                return null;
            }

            if (line.StartsWith("at ", StringComparison.Ordinal))
            {
                line = line.Substring(3);
            }

            var frame = new ShardPilotCrashFrame
            {
                Function = line,
                File = string.Empty,
                Line = 0,
                Module = string.Empty
            };

            var unityAt = line.IndexOf(" (at ", StringComparison.Ordinal);
            if (unityAt >= 0 && line.EndsWith(")", StringComparison.Ordinal))
            {
                frame.Function = line.Substring(0, unityAt);
                ApplyFileLine(frame, line.Substring(unityAt + 5, line.Length - unityAt - 6));
                return frame;
            }

            var inIndex = line.IndexOf(" in ", StringComparison.Ordinal);
            if (inIndex >= 0)
            {
                frame.Function = line.Substring(0, inIndex);
                ApplyFileLine(frame, line.Substring(inIndex + 4));
            }

            return frame;
        }

        private static void ApplyFileLine(ShardPilotCrashFrame frame, string fileLine)
        {
            fileLine = ShardPilotConfig.Trim(fileLine);
            var lineIndex = fileLine.LastIndexOf(":line ", StringComparison.Ordinal);
            if (lineIndex >= 0)
            {
                frame.File = SafeFileName(fileLine.Substring(0, lineIndex));
                int parsed;
                if (int.TryParse(fileLine.Substring(lineIndex + 6), out parsed))
                {
                    frame.Line = parsed;
                }
                return;
            }

            var colon = fileLine.LastIndexOf(':');
            if (colon >= 0)
            {
                frame.File = SafeFileName(fileLine.Substring(0, colon));
                int parsed;
                if (int.TryParse(fileLine.Substring(colon + 1), out parsed))
                {
                    frame.Line = parsed;
                }
                return;
            }

            frame.File = SafeFileName(fileLine);
        }

        private static string SafeFileName(string path)
        {
            path = ShardPilotConfig.Trim(path);
            if (string.IsNullOrEmpty(path))
            {
                return string.Empty;
            }

            return Path.GetFileName(path.Replace('\\', '/'));
        }
    }

    internal static class ShardPilotCrashThread
    {
        private static int _mainThreadId;

        public static void CaptureMainThread()
        {
            System.Threading.Interlocked.CompareExchange(
                ref _mainThreadId,
                System.Threading.Thread.CurrentThread.ManagedThreadId,
                0);
        }

        public static string MainThreadState()
        {
            return IsMainThread() ? "main" : "background";
        }

        public static bool IsMainThread()
        {
            return System.Threading.Volatile.Read(ref _mainThreadId) == System.Threading.Thread.CurrentThread.ManagedThreadId;
        }
    }

    internal static class ShardPilotCrashDevice
    {
        public static string DeviceClass()
        {
            switch (SystemInfo.deviceType)
            {
                case DeviceType.Handheld:
                    return Application.platform == RuntimePlatform.IPhonePlayer || Application.platform == RuntimePlatform.Android
                        ? "phone"
                        : "tablet";
                case DeviceType.Console:
                    return "console";
                case DeviceType.Desktop:
                    return "desktop";
                default:
                    return string.Empty;
            }
        }
    }
}
