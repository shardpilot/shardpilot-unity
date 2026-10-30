using System;
using System.Collections.Generic;
using System.Text;
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
        internal ShardPilotConsentPolicyDecision(string reason) { Reason = reason; }
    }

    internal static class ShardPilotConsentPolicy
    {
        internal static Func<IShardPilotHttpRequestSender> SenderFactoryForTesting;
        internal static Func<double> ClockForTesting;
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);

        internal static void Prepare(ShardPilotConsentPolicyContext context, Action<ShardPilotConsentPolicyDecision> callback)
        {
            if (callback == null) throw new ArgumentNullException(nameof(callback));
            Action<string> settle = reason =>
            {
                var host = Interlocked.Exchange(ref callback, null);
                try { host?.Invoke(new ShardPilotConsentPolicyDecision(reason)); }
                catch { /* A host callback cannot break transport completion. */ }
            };
            string root, body;
            try
            {
                if (!Request(context, out root, out body)) { settle("invalid_request"); return; }
            }
            catch { settle("invalid_request"); return; }

            var clock = ClockForTesting ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0);
            var started = ReadClock(clock);
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
                var reason = ClockRefusal(now, started);
                if (reason != null) { settle(reason); return false; }
                request.Timeout = TimeSpan.FromSeconds(Math.Max(0.001, started + 2 - now));
                return true;
            };
            try
            {
                var sender = SenderFactoryForTesting == null
                    ? new ShardPilotUnityHttpRequestSender() : SenderFactoryForTesting();
                if (sender == null) { settle("transport_unavailable"); return; }
                sender.Send(request, response => settle(ClockRefusal(ReadClock(clock), started) ??
                    (response == null || response.TransportError ? "transport_error" : "policy_unavailable")));
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
