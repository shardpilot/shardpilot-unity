using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;

namespace ShardPilot
{
    internal static class ShardPilotJson
    {
        internal static string CaptureMap(IDictionary<string, object> value)
        {
            if (value == null || value.Count == 0) return string.Empty;
            var writer = new JsonWriter();
            writer.WriteValue(value);
            return writer.ToString();
        }

        // Mirrors ingest/event_size.go:124-126 and experiment_fact_version.go:15-16:
        // retired schema-1 experiment facts belong to the version refusal, not size.
        internal static bool EventExceedsSizeLimit(string envelope, out int bytes)
        {
            var raw = envelope.Trim();
            bytes = Encoding.UTF8.GetByteCount(raw);
            if (bytes <= 2048) return false;
            Dictionary<string, object> fields;
            object version, name;
            if (ShardPilotJsonParser.TryParseObject(raw, out fields) &&
                fields.TryGetValue("schema_version", out version) && version is long && (long)version == 1 &&
                fields.TryGetValue("event_name", out name) && name is string)
            {
                var trimmed = ((string)name).Trim();
                if (trimmed == "experiment_exposure" || trimmed == "experiment_outcome") return false;
            }
            return true;
        }

        public static string SerializeBatch(ShardPilotConfig config, IList<ShardPilotEvent> events)
        {
            var writer = new JsonWriter();
            writer.BeginObject();
            writer.Name("events");
            writer.BeginArray();
            for (var i = 0; i < events.Count; i++)
            {
                if (i > 0)
                {
                    writer.Comma();
                }

                // ONE winning envelope per event (see CanonicalEnvelope):
                // the transport sends these bytes, and a later offline-spool
                // append of the same event reuses them verbatim — even when
                // two serializers race a caller's concurrent mutation of a
                // shared nested props map. Restored events already carry
                // theirs.
                writer.Raw(CanonicalEnvelope(config, events[i]));
            }

            writer.EndArray();
            writer.EndObject();
            return writer.ToString();
        }

        /// <summary>
        /// Serializes ONE event's envelope — the exact JSON object that ships
        /// inside the batch body's <c>events</c> array. Returns the stamped
        /// envelope verbatim when the event already carries one.
        /// </summary>
        public static string SerializeEnvelope(ShardPilotConfig config, ShardPilotEvent ev)
        {
            if (!string.IsNullOrEmpty(ev.SerializedEnvelope)) return ev.SerializedEnvelope;
            if (ev.AdmittedEnvelope != null) return ev.AdmittedEnvelope;
            if (ev.SealedFact != null) return ShardPilotExperimentApply.Envelope(config, ev);
            var writer = new JsonWriter();
            WriteEnvelope(writer, config, ev);
            return writer.ToString();
        }

        /// <summary>
        /// Returns THE ONE canonical serialized envelope for an event. The
        /// first serializer to produce one wins a compare-exchange stamp;
        /// every racing or later caller — the transport batch build, the
        /// offline-spool appends — then USES that winner and discards its own
        /// losing snapshot. Two serializers racing a caller's concurrent
        /// mutation of a shared nested props map can therefore never put one
        /// snapshot on the wire and a different one in the spool.
        /// </summary>
        public static string CanonicalEnvelope(ShardPilotConfig config, ShardPilotEvent ev)
        {
            var existing = ev.SerializedEnvelope;
            if (!string.IsNullOrEmpty(existing))
            {
                return existing;
            }

            var produced = SerializeEnvelope(config, ev);
            var winner = Interlocked.CompareExchange(ref ev.SerializedEnvelope, produced, null);
            return winner ?? produced;
        }

        /// <summary>What <see cref="RepairSpooledPlatform"/> was able to do.</summary>
        internal enum PlatformRepair
        {
            /// <summary>Nothing to change: no platform, or one already canonical.</summary>
            Unchanged,

            /// <summary>The platform was folded or removed; <c>repaired</c> is the new envelope.</summary>
            Repaired,

            /// <summary>A change was needed and could NOT be made safely.</summary>
            Unsafe,
        }

        /// <summary>
        /// Folds the <c>platform</c> of an envelope that was serialized by an
        /// EARLIER version of this SDK and is sitting in the offline spool.
        /// </summary>
        /// <remarks>
        /// <para>
        /// FOLDING AT THE WRITE DOES NOT FOLD WHAT IS ALREADY WRITTEN. A restored
        /// event carries its original serialized envelope and
        /// <see cref="WriteEnvelope"/> emits it VERBATIM, so a spooled envelope
        /// never passes through the fold above. Without this, an upgrade would
        /// leave old platform spellings on the resend path. Existing envelopes
        /// without this optional field remain unchanged.
        /// </para>
        /// <para>
        /// THE EDIT IS STRUCTURAL, NOT TEXTUAL. The current value comes from the
        /// PARSER, never from a pattern match; the member to replace is then
        /// rebuilt with the same <see cref="JsonWriter"/> that produced the file,
        /// so its escaping is the writer's rather than a second implementation of
        /// it. The result must occur EXACTLY ONCE, and the rewritten envelope is
        /// re-parsed and re-checked before it is accepted. Every one of those is a
        /// refusal: a repair that cannot be proven is reported as
        /// <see cref="PlatformRepair.Unsafe"/> rather than guessed at, because the
        /// alternative is inventing bytes for a wire we are trying to make
        /// trustworthy.
        /// </para>
        /// </remarks>
        internal static PlatformRepair RepairSpooledPlatform(string json, out string repaired)
        {
            repaired = json;
            if (string.IsNullOrEmpty(json))
            {
                return PlatformRepair.Unchanged;
            }

            Dictionary<string, object> envelope;
            if (!ShardPilotJsonParser.TryParseObject(json, out envelope) || envelope == null)
            {
                // Unreadable: it cannot be proven to need a repair, and it cannot
                // be repaired. Unchanged, not Unsafe -- Unsafe means "a change is
                // owed and I could not make it".
                return PlatformRepair.Unchanged;
            }

            object present;
            if (!envelope.TryGetValue("platform", out present))
            {
                return PlatformRepair.Unchanged;
            }

            var raw = present as string;
            if (raw == null)
            {
                // OptionalProperty only ever writes strings, so a non-string here
                // did not come from this writer.
                return PlatformRepair.Unsafe;
            }

            var folded = ShardPilotEnvelopePlatform.Normalize(raw);
            if (string.Equals(folded, raw, StringComparison.Ordinal))
            {
                return PlatformRepair.Unchanged;
            }

            var existing = "," + Member("platform", raw);
            var first = json.IndexOf(existing, StringComparison.Ordinal);
            if (first < 0 || first != json.LastIndexOf(existing, StringComparison.Ordinal))
            {
                // Absent (not written by this writer) or ambiguous (the same bytes
                // appear inside a nested value). Either way the span to edit is not
                // established, so nothing is edited.
                return PlatformRepair.Unsafe;
            }

            var replacement = folded == null ? string.Empty : "," + Member("platform", folded);
            var candidate = json.Substring(0, first) + replacement +
                json.Substring(first + existing.Length);

            // PROVE IT, do not assume it. The rewritten envelope must still parse,
            // and must now say what the fold decided.
            Dictionary<string, object> check;
            if (!ShardPilotJsonParser.TryParseObject(candidate, out check) || check == null)
            {
                return PlatformRepair.Unsafe;
            }

            object after;
            var has = check.TryGetValue("platform", out after);
            if (folded == null)
            {
                if (has)
                {
                    return PlatformRepair.Unsafe;
                }
            }
            else if (!has || !string.Equals(after as string, folded, StringComparison.Ordinal))
            {
                return PlatformRepair.Unsafe;
            }

            repaired = candidate;
            return PlatformRepair.Repaired;
        }

        /// <summary>
        /// The exact bytes this writer emits for one string member, so a rewrite
        /// searches for what was written rather than for what a second escaping
        /// implementation believes was written.
        /// </summary>
        private static string Member(string name, string value)
        {
            var writer = new JsonWriter();
            writer.BeginObject();
            writer.Property(name, value);
            writer.EndObject();
            var text = writer.ToString();
            return text.Substring(1, text.Length - 2);
        }

        private static void WriteEnvelope(JsonWriter writer, ShardPilotConfig config, ShardPilotEvent ev)
        {
            // An event restored from the offline spool carries its original
            // serialized envelope: emit it verbatim so the re-send matches the
            // first attempt byte for byte (the stable event_id then lets the
            // ingest service de-duplicate).
            if (!string.IsNullOrEmpty(ev.SerializedEnvelope))
            {
                writer.Raw(ev.SerializedEnvelope);
                return;
            }

            writer.BeginObject();
            writer.Property("event_id", ev.Id);
            writer.Property("schema_version", ev.SchemaVersion ?? 1);
            writer.Property("event_name", ev.Name);
            writer.Property("source", config.SourceValue);
            writer.Property("event_ts", FormatTimestamp(ev.TimestampUtc));
            writer.Property("workspace_key", config.WorkspaceKey);
            writer.Property("app_key", config.AppKey);
            writer.Property("environment_key", config.EnvironmentKey);
            writer.OptionalProperty("user_id", ev.UserId);
            writer.OptionalProperty("anonymous_id", ev.AnonymousId);
            writer.OptionalProperty("session_id", ev.SessionId);
            if (ev.SessionSequence > 0)
            {
                writer.Property("session_sequence", ev.SessionSequence);
            }

            // Analytics always uses a canonical value, including the fallback.
            // Fresh crash writers share this platform vocabulary.
            writer.OptionalProperty(
                "platform",
                ShardPilotEnvelopePlatform.Normalize(FirstNonEmpty(ev.Platform, config.Platform)));
            writer.OptionalProperty("os_name", ev.OSName);
            writer.OptionalProperty("os_version", ev.OSVersion);
            writer.OptionalProperty("device_model", ev.DeviceModel);
            writer.OptionalProperty("locale", ev.Locale);
            writer.OptionalProperty("country", ev.Country);
            writer.OptionalProperty("app_version", FirstNonEmpty(ev.AppVersion, config.AppVersion));
            writer.OptionalProperty("app_build", FirstNonEmpty(ev.AppBuild, config.AppBuild));
            writer.OptionalProperty("release_version", ev.ReleaseVersion);
            writer.OptionalMap("context", ev.Context, ev.CapturedContext);
            writer.OptionalMap("props", ev.Props, ev.CapturedProps);
            writer.EndObject();
        }

        /// <summary>
        /// Serializes a sanitized crash event into the crash report JSON
        /// wire shape, the body of
        /// <c>POST {crashBase}/api/v1/crashes/ingest</c>. This is a DISTINCT
        /// schema from the analytics batch envelope: top-level <c>crash_id</c>,
        /// <c>occurred_at</c>, <c>app{...}</c>, the component <c>component</c>
        /// slug, <c>platform</c>, <c>os{...}</c>, <c>device</c>,
        /// <c>exception{...}</c>, pre-symbolicated <c>threads[]/frames[]</c>
        /// (function/file/line, NO native modules), <c>breadcrumbs[]</c>, and
        /// the hashed-at-ingest <c>anonymous_id</c>/<c>session_id</c>.
        /// </summary>
        private static void WriteStringMap(JsonWriter writer, string name, IDictionary<string, string> map)
        {
            if (map == null || map.Count == 0)
            {
                return;
            }

            writer.Name(name);
            writer.BeginObject();
            foreach (var entry in map)
            {
                writer.OptionalProperty(entry.Key, entry.Value);
            }
            writer.EndObject();
        }

        public static string SerializeCrashIngest(ShardPilotConfig config, ShardPilotCrashEvent crash)
        {
            var writer = new JsonWriter();
            writer.BeginObject();

            writer.Property("crash_id", crash.CrashId);
            writer.Property("occurred_at", FormatTimestamp(crash.OccurredAtUtc));
            writer.Property("fatal", crash.Fatal);
            // The ingest contract accepts only 1..1,000,000 on known non-fatals.
            // Leave an unrepresentable rate unknown without changing admission.
            if (!crash.Fatal && crash.NonFatalSampleOneIn >= 1 && crash.NonFatalSampleOneIn <= 1000000)
            {
                writer.Property("non_fatal_sample_one_in", crash.NonFatalSampleOneIn);
            }

            // app{key,version,build_id}. The key cross-checks the credential's app scope.
            writer.Name("app");
            writer.BeginObject();
            writer.Property("key", config.AppKey);
            writer.OptionalProperty("version", crash.AppVersion);
            writer.OptionalProperty("build_id", crash.BuildId);
            writer.EndObject();

            // Component slug, already resolved by the client: a valid per-event
            // override, else the configured default, else empty when the caller
            // gave a bad override or there is no configured default. Empty is
            // omitted (bare app).
            writer.OptionalProperty("component", crash.Source);

            writer.Property("platform", ShardPilotEnvelopePlatform.Normalize(FirstNonEmpty(crash.Platform, config.Platform)));

            // os{name,version} — only when at least one is present.
            if (!string.IsNullOrEmpty(crash.OSName) || !string.IsNullOrEmpty(crash.OSVersion))
            {
                writer.Name("os");
                writer.BeginObject();
                writer.OptionalProperty("name", crash.OSName);
                writer.OptionalProperty("version", crash.OSVersion);
                writer.EndObject();
            }

            // device — additionalProperties:string. class + thread_state.
            if (!string.IsNullOrEmpty(crash.DeviceClass) || !string.IsNullOrEmpty(crash.ThreadState))
            {
                writer.Name("device");
                writer.BeginObject();
                writer.OptionalProperty("class", crash.DeviceClass);
                writer.OptionalProperty("thread_state", crash.ThreadState);
                writer.EndObject();
            }

            // context / metadata — free-form additionalProperties:string maps. Keys are
            // already validated and values scrubbed by the sanitizer; both are non-empty.
            WriteStringMap(writer, "context", crash.Context);
            WriteStringMap(writer, "metadata", crash.Metadata);

            // exception{type,reason,crashed_thread_id}. type is required.
            writer.Name("exception");
            writer.BeginObject();
            writer.Property("type", string.IsNullOrEmpty(crash.CrashType) ? "exception" : crash.CrashType);
            writer.OptionalProperty("reason", crash.ExceptionReason);
            // A Unity-captured crash is single-threaded from the SDK's view; the
            // crashed thread is the synthesized "0" thread below.
            writer.Property("crashed_thread_id", "0");
            writer.EndObject();

            // threads[]/frames[] — a single PRE-SYMBOLICATED thread. The frames
            // carry function/file/line and NO native module/address, so the
            // crash endpoint accepts them without a modules[] map. modules
            // is intentionally omitted (empty allowed).
            writer.Name("threads");
            writer.BeginArray();
            writer.BeginObject();
            writer.Property("id", "0");
            writer.Property("crashed", true);
            writer.Name("frames");
            writer.BeginArray();
            var frames = crash.StackFrames;
            var wroteFrame = false;
            if (frames != null)
            {
                for (var i = 0; i < frames.Count; i++)
                {
                    var frame = frames[i];
                    if (frame == null || string.IsNullOrEmpty(frame.Function))
                    {
                        // A frame with no function is not addressable in the
                        // pre-symbolicated shape (no address either) and would be
                        // rejected; skip it.
                        continue;
                    }

                    if (wroteFrame)
                    {
                        writer.Comma();
                    }

                    writer.BeginObject();
                    writer.Property("index", i);
                    writer.Property("function", frame.Function);
                    writer.OptionalProperty("file", frame.File);
                    if (frame.Line > 0)
                    {
                        writer.Property("line", frame.Line);
                    }
                    writer.EndObject();
                    wroteFrame = true;
                }
            }

            // The server requires at least one frame OR raw_text. When no
            // structured frame survived, emit a synthetic frame naming the
            // exception type so the report still groups.
            if (!wroteFrame)
            {
                writer.BeginObject();
                writer.Property("index", 0);
                writer.Property("function", string.IsNullOrEmpty(crash.CrashType) ? "exception" : crash.CrashType);
                writer.EndObject();
            }

            writer.EndArray();
            writer.EndObject();
            writer.EndArray();

            // breadcrumbs[] — each {name, timestamp}.
            var breadcrumbs = crash.Breadcrumbs;
            if (breadcrumbs != null && breadcrumbs.Count > 0)
            {
                writer.Name("breadcrumbs");
                writer.BeginArray();
                for (var i = 0; i < breadcrumbs.Count; i++)
                {
                    if (breadcrumbs[i] == null)
                    {
                        continue;
                    }

                    if (i > 0)
                    {
                        writer.Comma();
                    }

                    writer.BeginObject();
                    writer.Property("name", breadcrumbs[i].Name);
                    writer.Property("timestamp", FormatTimestamp(breadcrumbs[i].TimestampUtc));
                    writer.EndObject();
                }

                writer.EndArray();
            }

            // anonymous_id is hashed at ingest into the canonical actor key (the
            // per-subject key for consent + GDPR erasure); never stored raw.
            // user_id is intentionally NOT sent: a self-asserted
            // user_id never becomes the canonical actor under API-key auth and
            // is stripped at ingest. session_id is hashed at ingest.
            if (crash.UseEraAttribution)
            {
                // An ApplicationExitInfo-derived report is
                // reported one-or-more launches AFTER the process era in
                // which the exit occurred, so its attribution is FROZEN to
                // that era's snapshot — never the config's CURRENT anonymous
                // id (that would pin the crash to whoever is logged in at
                // the next launch). Empty means the record is device-scoped
                // anonymous: no actor field at all, borrowing no login. No
                // session_id in either case: the era's session is unknowable
                // at era start and the current session must not be borrowed.
                writer.OptionalProperty("anonymous_id", crash.EraAnonymousId);
            }
            else
            {
                writer.OptionalProperty("anonymous_id", config.AnonymousId);
                writer.OptionalProperty("session_id", crash.SessionId);
            }

            // Additive fields, all omitted when empty so every
            // pre-ANR crash body stays byte-identical: the event kind
            // (only "anr" is ever stamped client-side today; absent = the
            // base "crash" kind), the aei_record_key field,
            // and the raw trace text the server-side ANR parse consumes.
            writer.OptionalProperty("kind", crash.Kind);
            writer.OptionalProperty("aei_record_key", crash.AeiRecordKey);
            writer.OptionalProperty("raw_text", crash.RawText);

            writer.EndObject();
            return writer.ToString();
        }

        /// <summary>
        /// Serializes the metadata envelope PART of a tombstone upload
        /// (<c>POST {crashBase}/api/v1/crashes/ingest-tombstone</c>):
        /// the SAME envelope fields the JSON crash lane
        /// sends — <c>crash_id</c>, <c>occurred_at</c>, <c>app{...}</c>,
        /// <c>component</c>, <c>platform</c>, the optional os/device/context/
        /// metadata blocks, <c>exception{type,reason}</c>, the era-frozen
        /// actor attribution, and <c>aei_record_key</c> — MINUS everything
        /// the tombstone lane's server refuses or derives itself:
        /// NO <c>threads[]</c>, NO <c>raw_text</c>, NO <c>modules[]</c>
        /// (frames/modules/threads come from the server-parsed tombstone,
        /// never from client JSON), NO breadcrumbs (wrong era), NO
        /// <c>exception.crashed_thread_id</c> (the server resolves the
        /// crashing thread's synthetic ordinal into the metadata sidecar
        /// itself), and NO <c>kind</c> (native crash is the base
        /// <c>crash</c> kind — absent field — by construction).
        /// </summary>
        public static string SerializeTombstoneMetadata(ShardPilotConfig config, ShardPilotCrashEvent crash)
        {
            var writer = new JsonWriter();
            writer.BeginObject();

            writer.Property("crash_id", crash.CrashId);
            writer.Property("occurred_at", FormatTimestamp(crash.OccurredAtUtc));
            writer.Property("fatal", true); // A native tombstone describes a terminated process.

            // app{key,version,build_id}. The key cross-checks the credential's app
            // scope; version/build are the CRASHED era's (possibly empty —
            // unknown ships as absent, never as the reporting launch's).
            writer.Name("app");
            writer.BeginObject();
            writer.Property("key", config.AppKey);
            writer.OptionalProperty("version", crash.AppVersion);
            writer.OptionalProperty("build_id", crash.BuildId);
            writer.EndObject();

            writer.OptionalProperty("component", crash.Source);

            // The tombstone lane is Android-only; the builder stamps the
            // closed "android" slug and the server refuses anything else.
            writer.Property("platform", ShardPilotEnvelopePlatform.Normalize(FirstNonEmpty(crash.Platform, config.Platform)));

            if (!string.IsNullOrEmpty(crash.OSName) || !string.IsNullOrEmpty(crash.OSVersion))
            {
                writer.Name("os");
                writer.BeginObject();
                writer.OptionalProperty("name", crash.OSName);
                writer.OptionalProperty("version", crash.OSVersion);
                writer.EndObject();
            }

            if (!string.IsNullOrEmpty(crash.DeviceClass) || !string.IsNullOrEmpty(crash.ThreadState))
            {
                writer.Name("device");
                writer.BeginObject();
                writer.OptionalProperty("class", crash.DeviceClass);
                writer.OptionalProperty("thread_state", crash.ThreadState);
                writer.EndObject();
            }

            WriteStringMap(writer, "context", crash.Context);
            WriteStringMap(writer, "metadata", crash.Metadata);

            // exception{type,reason}: the exit record's metadata — the
            // authoritative frames come from the server-parsed tombstone.
            writer.Name("exception");
            writer.BeginObject();
            writer.Property("type", string.IsNullOrEmpty(crash.CrashType) ? "exception" : crash.CrashType);
            writer.OptionalProperty("reason", crash.ExceptionReason);
            writer.EndObject();

            // Era-frozen attribution, exactly the AEI JSON rule:
            // the era subject's anonymous id or NOTHING (device-scoped
            // anonymous) — never the reporting launch's login, never a
            // session id.
            if (crash.UseEraAttribution)
            {
                writer.OptionalProperty("anonymous_id", crash.EraAnonymousId);
            }
            else
            {
                writer.OptionalProperty("anonymous_id", config.AnonymousId);
                writer.OptionalProperty("session_id", crash.SessionId);
            }

            // Carry the supplied OS-record key when present so the report
            // retains its record identity on replay. This does not infer
            // server deduplication or deployment readiness.
            writer.OptionalProperty("aei_record_key", crash.AeiRecordKey);

            writer.EndObject();
            return writer.ToString();
        }

        public static string FormatTimestamp(DateTime timestamp)
        {
            var utc = timestamp.Kind == DateTimeKind.Utc ? timestamp : timestamp.ToUniversalTime();
            var value = utc.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            value = value.TrimEnd('0').TrimEnd('.');
            return value + "Z";
        }

        private static string FirstNonEmpty(params string[] values)
        {
            for (var i = 0; i < values.Length; i++)
            {
                if (!string.IsNullOrEmpty(values[i]))
                {
                    return values[i];
                }
            }

            return string.Empty;
        }

        public static void AppendQuoted(StringBuilder builder, string value)
        {
            builder.Append('"');
            value = value ?? string.Empty;
            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                switch (ch)
                {
                    case '"':
                        builder.Append("\\\"");
                        break;
                    case '\\':
                        builder.Append("\\\\");
                        break;
                    case '\b':
                        builder.Append("\\b");
                        break;
                    case '\f':
                        builder.Append("\\f");
                        break;
                    case '\n':
                        builder.Append("\\n");
                        break;
                    case '\r':
                        builder.Append("\\r");
                        break;
                    case '\t':
                        builder.Append("\\t");
                        break;
                    default:
                        if (ch < 0x20)
                        {
                            builder.Append("\\u");
                            builder.Append(((int)ch).ToString("x4", CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            builder.Append(ch);
                        }
                        break;
                }
            }

            builder.Append('"');
        }

        private sealed class JsonWriter
        {
            private readonly StringBuilder _builder = new StringBuilder();
            private readonly Stack<bool> _needsComma = new Stack<bool>();

            public void BeginObject()
            {
                _builder.Append('{');
                _needsComma.Push(false);
            }

            public void EndObject()
            {
                _builder.Append('}');
                _needsComma.Pop();
                MarkValueWritten();
            }

            public void BeginArray()
            {
                _builder.Append('[');
                _needsComma.Push(false);
            }

            public void EndArray()
            {
                _builder.Append(']');
                _needsComma.Pop();
                MarkValueWritten();
            }

            public void Comma()
            {
                _builder.Append(',');
            }

            /// <summary>
            /// Appends pre-serialized JSON verbatim as one value. Mirrors the
            /// stack effect of a BeginObject/EndObject pair so surrounding
            /// comma bookkeeping stays correct.
            /// </summary>
            public void Raw(string json)
            {
                _builder.Append(json);
                MarkValueWritten();
            }

            public void Name(string name)
            {
                WriteCommaIfNeeded();
                WriteString(name);
                _builder.Append(':');
            }

            public void Property(string name, string value)
            {
                Name(name);
                WriteString(value ?? string.Empty);
                MarkValueWritten();
            }

            public void Property(string name, int value)
            {
                Name(name);
                _builder.Append(value.ToString(CultureInfo.InvariantCulture));
                MarkValueWritten();
            }

            public void Property(string name, long value)
            {
                Name(name);
                _builder.Append(value.ToString(CultureInfo.InvariantCulture));
                MarkValueWritten();
            }

            public void Property(string name, bool value)
            {
                Name(name);
                _builder.Append(value ? "true" : "false");
                MarkValueWritten();
            }

            public void OptionalProperty(string name, string value)
            {
                if (!string.IsNullOrEmpty(value))
                {
                    Property(name, value);
                }
            }

            public void OptionalMap(string name, IDictionary<string, object> value, string captured = null)
            {
                if (captured != null)
                {
                    if (captured.Length != 0) { Name(name); Raw(captured); }
                    return;
                }
                if (value == null || value.Count == 0)
                {
                    return;
                }

                Name(name);
                WriteValue(value);
                MarkValueWritten();
            }

            public void WriteValue(object value)
            {
                if (value == null)
                {
                    _builder.Append("null");
                    return;
                }

                var text = value as string;
                if (text != null)
                {
                    WriteString(text);
                    return;
                }

                if (value is bool)
                {
                    _builder.Append((bool)value ? "true" : "false");
                    return;
                }

                if (value is int || value is long || value is short || value is byte ||
                    value is uint || value is ulong || value is ushort || value is sbyte)
                {
                    _builder.Append(Convert.ToString(value, CultureInfo.InvariantCulture));
                    return;
                }

                if (value is float || value is double || value is decimal)
                {
                    var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                    if (double.IsNaN(number) || double.IsInfinity(number))
                    {
                        _builder.Append("null");
                    }
                    else
                    {
                        _builder.Append(Convert.ToString(value, CultureInfo.InvariantCulture));
                    }
                    return;
                }

                var dateTime = value as DateTime?;
                if (dateTime.HasValue)
                {
                    WriteString(FormatTimestamp(dateTime.Value));
                    return;
                }

                var map = value as IDictionary<string, object>;
                if (map != null)
                {
                    WriteMap(map);
                    return;
                }

                var enumerable = value as IEnumerable;
                if (enumerable != null)
                {
                    WriteEnumerable(enumerable);
                    return;
                }

                WriteString(Convert.ToString(value, CultureInfo.InvariantCulture));
            }

            private void WriteMap(IDictionary<string, object> map)
            {
                BeginObject();
                var first = true;
                var keys = new List<string>(map.Keys);
                keys.Sort(StringComparer.Ordinal);
                foreach (var key in keys)
                {
                    if (string.IsNullOrEmpty(key))
                    {
                        continue;
                    }

                    if (!first)
                    {
                        Comma();
                    }

                    first = false;
                    WriteString(key);
                    _builder.Append(':');
                    WriteValue(map[key]);
                }

                EndObject();
            }

            private void WriteEnumerable(IEnumerable values)
            {
                BeginArray();
                var first = true;
                foreach (var value in values)
                {
                    if (!first)
                    {
                        Comma();
                    }

                    first = false;
                    WriteValue(value);
                }

                EndArray();
            }

            private void WriteCommaIfNeeded()
            {
                if (_needsComma.Count == 0)
                {
                    return;
                }

                var needs = _needsComma.Pop();
                if (needs)
                {
                    _builder.Append(',');
                }

                _needsComma.Push(needs);
            }

            private void MarkValueWritten()
            {
                if (_needsComma.Count == 0)
                {
                    return;
                }

                _needsComma.Pop();
                _needsComma.Push(true);
            }

            private void WriteString(string value)
            {
                AppendQuoted(_builder, value);
            }

            public override string ToString()
            {
                return _builder.ToString();
            }
        }
    }
}
