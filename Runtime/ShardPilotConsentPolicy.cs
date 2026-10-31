using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace ShardPilot
{
    /// <summary>Identifier-free host context for preparing a consent screen.</summary>
    public sealed class ShardPilotConsentPolicyContext
    {
        public string AppUrl, WorkspaceKey, AppKey, EnvironmentKey, AppVersion, Locale, Platform;
        public string Store, StoreRegion;
        public ShardPilotConsentPolicyAgeBand AgeBand;
        public bool Advisory;
    }

    public sealed class ShardPilotConsentPolicyAgeBand
    {
        public string Vocabulary, Band;
    }

#nullable enable annotations
    /// <summary>Version of the detached advisory reference data.</summary>
    public sealed class ShardPilotConsentPolicyAdvisoryMatrix
    {
        public string DocsCommit { get; }
        public string FileSha256 { get; }
        public string Date { get; }
        internal ShardPilotConsentPolicyAdvisoryMatrix(string docsCommit, string fileSha256, string date)
        { DocsCommit = docsCommit; FileSha256 = fileSha256; Date = date; }
    }

    /// <summary>Detached reference data. It never grants consent or changes the local decision.</summary>
    public sealed class ShardPilotConsentPolicyAdvisory
    {
        public string Jurisdiction { get; }
        public string? Estimate { get; }
        public string RowId { get; }
        public string RowStatus { get; }
        public string RowBasis { get; }
        public string AdvisoryBasis { get; }
        public ShardPilotConsentPolicyAdvisoryMatrix Matrix { get; }
        public string ResolvedBy { get; }
        internal ShardPilotConsentPolicyAdvisory(string jurisdiction, string? estimate, string rowId,
            string rowStatus, string rowBasis, string advisoryBasis, ShardPilotConsentPolicyAdvisoryMatrix matrix, string resolvedBy)
        {
            Jurisdiction = jurisdiction; Estimate = estimate; RowId = rowId; RowStatus = rowStatus;
            RowBasis = rowBasis; AdvisoryBasis = advisoryBasis; Matrix = matrix; ResolvedBy = resolvedBy;
        }
    }
#nullable restore annotations

    /// <summary>A local strict decision. It never grants analytics or enables capture.</summary>
    public sealed class ShardPilotConsentPolicyDecision
    {
        public string Regime => "STRICT_OPT_IN";
        public string CrashProfile => "off";
        public string ServerAnalytics => "denied";
        public string ChildRules => "minimised";
        public string AnalyticsChoiceDefault => "off";
        public bool ExplicitGrantRequired => true;
        public bool PlanUsed => false;
        public string[] OperationBlocks => null;
        public string OperationBlocksSource => "none";
        public string Reason { get; }
#nullable enable annotations
        public ShardPilotConsentPolicyAdvisory? Advisory { get; }
#nullable restore annotations
        internal ShardPilotConsentPolicyDecision(string reason, ShardPilotConsentPolicyAdvisory advisory = null)
        { Reason = reason; Advisory = advisory; }
    }

    internal static class ShardPilotConsentPolicy
    {
        internal static Func<IShardPilotHttpRequestSender> SenderFactoryForTesting;
        internal static Func<double> ClockForTesting;
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);
        private static readonly object OrderGate = new object();
        private static string _context;
        private static long _epoch, _dispatch, _latest;

        internal static void Invalidate()
        {
            lock (OrderGate) { _epoch++; _latest = 0; }
        }

        internal static void Prepare(ShardPilotConsentPolicyContext context, Action<ShardPilotConsentPolicyDecision> callback)
        {
            if (callback == null) throw new ArgumentNullException(nameof(callback));
            void settle(string reason, ShardPilotConsentPolicyAdvisory advisory = null)
            {
                var host = Interlocked.Exchange(ref callback, null);
                try { host?.Invoke(new ShardPilotConsentPolicyDecision(reason, advisory)); }
                catch { /* A host callback cannot break transport completion. */ }
            }
            string root, body;
            try
            {
                if (!Request(context, out root, out body)) { settle("invalid_request"); return; }
            }
            catch { settle("invalid_request"); return; }

            var clock = ClockForTesting ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0);
            var started = ReadClock(clock);
            long epoch, dispatch = 0;
            lock (OrderGate)
            {
                var key = root + "\n" + body;
                if (_context != key) { _context = key; _epoch++; _latest = 0; }
                epoch = _epoch;
                if (!double.IsNaN(started)) _latest = dispatch = ++_dispatch;
            }
            if (double.IsNaN(started)) { settle("clock_unavailable"); return; }
            var request = new ShardPilotHttpRequest
            {
                Method = "POST", Url = root + "/api/cp/v1/consent/policy",
                ContentType = "application/json", Body = body,
                Timeout = TimeSpan.FromSeconds(2), RefuseRedirects = true
            };
            request.BeforeSend = () =>
            {
                var now = ReadClock(clock);
                var reason = PendingReason(now, started, epoch, dispatch, false);
                if (reason != null) { settle(reason); return false; }
                request.Timeout = TimeSpan.FromSeconds(Math.Max(0.001, started + 2 - now));
                return true;
            };
            try
            {
                var sender = SenderFactoryForTesting == null
                    ? new ShardPilotUnityHttpRequestSender() : SenderFactoryForTesting();
                if (sender == null) { settle("transport_unavailable"); return; }
                sender.Send(request, response =>
                {
                    var arrived = ReadClock(clock);
                    var reason = PendingReason(arrived, started, epoch, dispatch, true);
                    ShardPilotConsentPolicyAdvisory advisory = null;
                    if (reason == null) reason = ResponseReason(response, body, arrived, out advisory);
                    settle(reason, advisory);
                });
            }
            catch { settle("transport_error"); }
        }

        private static double ReadClock(Func<double> clock)
        {
            try { var value = clock(); return double.IsInfinity(value) ? double.NaN : value; }
            catch { return double.NaN; }
        }

        private static string ClockRefusal(double now, double started)
        {
            if (double.IsNaN(now)) return "clock_unavailable";
            if (now < started) return "clock_regressed";
            return now > started + 2 ? "deadline_exceeded" : null;
        }

        private static string PendingReason(double now, double started, long epoch, long dispatch, bool complete)
        {
            var clock = ClockRefusal(now, started);
            if (clock == "clock_unavailable" || clock == "clock_regressed") return clock;
            lock (OrderGate)
            {
                if (epoch != _epoch) return "invalidated";
                if (dispatch != _latest) return "superseded";
                if (complete) _latest = 0;
            }
            return clock;
        }

        private static HashSet<string> Names(string value) => new HashSet<string>(
            value.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries), StringComparer.Ordinal);
        private static Dictionary<string, object> Map(object value) => value as Dictionary<string, object>;
        private static object Member(Dictionary<string, object> map, string key) => map != null && map.TryGetValue(key, out var value) ? value : null;
        private static bool OneOf(object value, params string[] choices) => value is string text && System.Array.IndexOf(choices, text) >= 0;
        private static bool Shape(Dictionary<string, object> map, string required, string optional = "", string nullable = "")
        {
            if (map == null) return false;
            var needed = Names(required); var allowed = Names(required + " " + optional); var nulls = Names(nullable);
            return needed.All(map.ContainsKey) && map.All(pair => allowed.Contains(pair.Key) && (pair.Value != null || nulls.Contains(pair.Key)));
        }

        private static string ResponseReason(ShardPilotHttpResponse response, string requestBody, double now, out ShardPilotConsentPolicyAdvisory advisory)
        {
            advisory = null;
            if (response == null || response.TransportError) return "transport_error";
            try
            {
                Dictionary<string, object> plan, request; Dictionary<string, string> raw;
                if (!Bounded(response.Body, 16 * 1024) || !ShardPilotJsonParser.TryParseUniqueObject(response.Body, out plan, out raw)) return "invalid_response";
                var reason = Member(plan, "reason");
                var refusal = response.StatusCode < 200 || response.StatusCode >= 300 || reason != null;
                if ((reason != null && !(reason is string)) || !ShardPilotJsonParser.TryParseObject(requestBody, out request) ||
                    !ValidPlan(plan, request, now, refusal)) return "invalid_response";
                if (refusal)
                    return OneOf(reason, "invalid_request", "invalid_scope", "store_region_not_accepted", "unsupported_app_version", "policy_unavailable")
                        ? (string)reason : "policy_unavailable";
                advisory = CopyAdvisory(Map(Member(plan, "advisory")));
                return "plan_unsigned";
            }
            catch { return "invalid_response"; }
        }

        private static ShardPilotConsentPolicyAdvisory CopyAdvisory(Dictionary<string, object> advisory)
        {
            if (advisory == null) return null;
            var matrix = Map(advisory["matrix"]);
            return new ShardPilotConsentPolicyAdvisory((string)advisory["jurisdiction"], (string)advisory["estimate"],
                (string)advisory["row_id"], (string)advisory["row_status"], (string)advisory["row_basis"], (string)advisory["advisory_basis"],
                new ShardPilotConsentPolicyAdvisoryMatrix((string)matrix["docs_commit"], (string)matrix["file_sha256"], (string)matrix["date"]),
                (string)advisory["resolved_by"]);
        }

        private static bool ValidPlan(Dictionary<string, object> plan, Dictionary<string, object> request, double now, bool refusal)
        {
            if (!Shape(plan, "regime flags policy_version consent_text_version presented_language scope signals_used band_vocabulary band_vocabulary_version expires_at max_age_seconds signature basis", "reason advisory", "signature")) return false;
            if (!OneOf(plan["regime"], "STRICT_OPT_IN", "SOFT_OPT_OUT", "UNKNOWN") || plan["signature"] != null) return false;
            var flags = Map(plan["flags"]); var scope = Map(plan["scope"]); var basis = Map(plan["basis"]);
            if (!Shape(flags, "crash_profile server_analytics child_rules operation_blocks") ||
                !Shape(scope, "workspace_key app_key environment_key") || !Shape(basis, "character table_provenance notice")) return false;
            if (!OneOf(flags["crash_profile"], "off", "minimal_diagnostics_for_minors") ||
                !OneOf(flags["server_analytics"], "denied") || !OneOf(flags["child_rules"], "minimised")) return false;
            var blocks = flags["operation_blocks"] as List<object>;
            if (blocks == null || blocks.Count > 64 || !blocks.All(item => Bounded(item as string, 64))) return false;
            if (!Version(plan["policy_version"] as string) || !Version(plan["consent_text_version"] as string) ||
                !Bounded(plan["presented_language"] as string, 35) || !Bounded(plan["band_vocabulary"] as string, 32) ||
                !Bounded(plan["band_vocabulary_version"] as string, 32)) return false;
            if (!OneOf(basis["character"], "informational_reference") || !OneOf(basis["table_provenance"], "ai_draft", "owner_accepted") ||
                !Bounded(basis["notice"] as string, 2048)) return false;
            // A refusal asserts no caller scope; its string fields may be empty.
            var scopeValid = refusal ? scope.Values.All(value => value is string text && Utf8.GetByteCount(text) <= 64) :
                Names("workspace_key app_key environment_key").All(key => Equals(scope[key], request[key]));
            if (!scopeValid) return false;
            var signals = plan["signals_used"] as List<object>;
            if (signals == null || signals.Count > 16) return false;
            foreach (var item in signals)
            {
                var signal = Map(item);
                if (!Shape(signal, "name available", "reason") || !Bounded(signal["name"] as string, 64) || !(signal["available"] is bool)) return false;
                if ((bool)signal["available"] ? signal.ContainsKey("reason") :
                    !OneOf(Member(signal, "reason"), "source_not_permitted", "source_unavailable", "not_enabled_in_release")) return false;
            }
            if (!Bounded(plan["expires_at"] as string, 64) || !Timestamp(plan["expires_at"] as string, out var expiry) || expiry <= now) return false;
            var age = plan["max_age_seconds"];
            if (!(age is long whole && whole >= 0) && !(age is double number && number >= 0 && !double.IsInfinity(number) && Math.Floor(number) == number)) return false;
            return !plan.ContainsKey("advisory") || !refusal && Equals(Member(request, "advisory"), true) && ValidAdvisory(Map(plan["advisory"]));
        }

        private static bool ValidAdvisory(Dictionary<string, object> advisory)
        {
            if (!Shape(advisory, "jurisdiction estimate row_id row_status row_basis advisory_basis matrix resolved_by", "", "estimate")) return false;
            var jurisdiction = advisory["jurisdiction"] as string;
            if (jurisdiction == null || !Regex.IsMatch(jurisdiction, @"\A(?:[A-Z]{2}|OTHER)\z") || !Equals(advisory["row_id"], jurisdiction)) return false;
            if (advisory["estimate"] != null && !OneOf(advisory["estimate"], "STRICT_OPT_IN", "SOFT_OPT_OUT")) return false;
            if (jurisdiction == "OTHER" && advisory["estimate"] != null ||
                !OneOf(advisory["resolved_by"], "server_country", "unknown") || Equals(advisory["resolved_by"], "unknown") && jurisdiction != "OTHER") return false;
            if (!OneOf(advisory["row_status"], "COUNSEL_PENDING") || !OneOf(advisory["row_basis"], "ai_draft")) return false;
            var text = advisory["advisory_basis"] as string;
            if (!Bounded(text, 2048) || text.Any(c => c < 32 || c >= 127 && c <= 159)) return false;
            var matrix = Map(advisory["matrix"]);
            if (!Shape(matrix, "docs_commit file_sha256 date") ||
                !Regex.IsMatch(matrix["docs_commit"] as string ?? "", @"\A[0-9a-f]{40}\z") ||
                !Regex.IsMatch(matrix["file_sha256"] as string ?? "", @"\A[0-9a-f]{64}\z")) return false;
            var date = matrix["date"] as string;
            return date != null && date.Length == 10 && Timestamp(date + "T00:00:00Z", out _);
        }

        internal static bool CalendarDate(string value) => value != null && value.Length == 10 &&
            Timestamp(value + "T00:00:00Z", out _);

        // Keep the timestamp grammar's calendar and offset rules without date normalization.
        private static bool Timestamp(string value, out double timestamp)
        {
            timestamp = 0;
            if (value == null) return false;
            var match = Regex.Match(value, @"\A([0-9]{4})-([0-9]{2})-([0-9]{2})(?:T|t)([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})\z");
            if (!match.Success) return false;
            var parts = Enumerable.Range(1, 6).Select(i => int.Parse(match.Groups[i].Value, CultureInfo.InvariantCulture)).ToArray();
            var year = parts[0]; var month = parts[1]; var day = parts[2];
            var leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            var days = new[] { 31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
            if (month < 1 || month > 12 || day < 1 || day > days[month - 1] || parts[3] > 23 || parts[4] > 59 || parts[5] > 59) return false;
            var zone = match.Groups[7].Value; var offset = 0;
            if (zone.Length > 1)
            {
                var hours = int.Parse(zone.Substring(1, 2), CultureInfo.InvariantCulture); var minutes = int.Parse(zone.Substring(4, 2), CultureInfo.InvariantCulture);
                if (hours > 23 || minutes > 59 || zone == "-00:00") return false;
                offset = (hours * 3600 + minutes * 60) * (zone[0] == '-' ? -1 : 1);
            }
            year -= month <= 2 ? 1 : 0;
            var era = (year >= 0 ? year : year - 399) / 400; var yoe = year - era * 400;
            var doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
            var doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            timestamp = (era * 146097L + doe - 719468) * 86400 + parts[3] * 3600 + parts[4] * 60 + parts[5] - offset;
            return true;
        }

        private static bool Bounded(string value, int maximum)
        {
            return !string.IsNullOrEmpty(value) && Utf8.GetByteCount(value) <= maximum;
        }

        private static bool Version(string value)
        {
            if (!Bounded(value, 64)) return false;
            foreach (var c in value)
                if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
                    !(c >= '0' && c <= '9') && c != '.' && c != '_' && c != '+' && c != '/' && c != '-') return false;
            return true;
        }

        private static bool Request(ShardPilotConsentPolicyContext context, out string root, out string body)
        {
            root = body = null;
            if (context == null || !Bounded(context.AppUrl?.Trim(), 256) ||
                !Bounded(context.WorkspaceKey, 64) || !Bounded(context.AppKey, 64) ||
                !Bounded(context.EnvironmentKey, 64) || !Version(context.AppVersion) ||
                !Bounded(context.Locale, 35) || string.IsNullOrEmpty(context.Platform) || context.StoreRegion != null) return false;
            if (context.Store != null && context.Store != "steam" && context.Store != "apple" &&
                context.Store != "google_play" && context.Store != "standalone") return false;
            var age = context.AgeBand;
            if (age != null && (!Bounded(age.Vocabulary, 32) || !Bounded(age.Band, 32))) return false;
            root = ShardPilotConfig.NormalizePolicyEndpoint(context.AppUrl);
            var fields = new Dictionary<string, object>
            {
                { "workspace_key", context.WorkspaceKey }, { "app_key", context.AppKey },
                { "environment_key", context.EnvironmentKey }, { "app_version", context.AppVersion },
                { "locale", context.Locale }, { "platform", ShardPilotEnvelopePlatform.Normalize(context.Platform) }
            };
            if (context.Store != null) fields.Add("store", context.Store);
            if (age != null) fields.Add("age_band", new Dictionary<string, object>
                { { "vocabulary", age.Vocabulary }, { "band", age.Band } });
            if (context.Advisory) fields.Add("advisory", true);
            body = ShardPilotJson.CaptureMap(fields);
            return true;
        }
    }
}
