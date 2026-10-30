using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace ShardPilot
{
    // A captured application; the owner supplies the same snapshot for build and decode.
    internal sealed class ShardPilotExperimentApplication
    {
        public string ExperimentKey, VariantKey, SubjectKey, ServedAt, AppliedAt, OccurrenceId;
        public string OccurredAt, OutcomeKey;
        public long ExperimentVersion, ServedRevision, OutcomeValue;
        public bool ServedKillGate;
        public List<KeyValuePair<string, string>> Attributes;
        public bool IsOutcome { get { return OutcomeKey != null; } }
    }

    internal sealed class ShardPilotSealedExperimentFact
    {
        internal readonly string Raw, Seal, EventId, EventTimestamp, SubjectFactKey;
        internal readonly string WorkspaceKey, AppKey, EnvironmentKey, EventName;
        internal readonly string ExperimentKey, VariantKey, Attestation, OutcomeKey;
        internal readonly long ExperimentVersion, OutcomeValue;
        internal readonly Dictionary<string, object> Props;
        internal readonly bool IsEnvelope;

        internal ShardPilotSealedExperimentFact(string raw, string seal,
            Dictionary<string, object> values, Dictionary<string, object> props, bool isEnvelope = false)
        {
            Props = new Dictionary<string, object>(props, StringComparer.Ordinal);
            Raw = raw;
            IsEnvelope = isEnvelope;
            Seal = seal;
            EventId = (string)values["event_id"];
            EventTimestamp = (string)values["event_ts"];
            WorkspaceKey = (string)values["workspace_key"];
            AppKey = (string)values["app_key"];
            EnvironmentKey = (string)values["environment_key"];
            EventName = (string)values["event_name"];
            SubjectFactKey = (string)props["assignment_key"];
            ExperimentKey = (string)props["experiment_key"];
            VariantKey = (string)props["variant_key"];
            Attestation = (string)props["attestation"];
            ExperimentVersion = (long)props["experiment_version"];
            if (EventName == "experiment_outcome")
            {
                OutcomeKey = (string)props["outcome_key"];
                OutcomeValue = (long)props["outcome_value"];
            }
        }
    }

    // Protocol seam only. The experiment owner supplies lifecycle/consent gates,
    // sends the request, classifies HTTP outcomes, and admits the returned fact.
    internal static class ShardPilotExperimentApply
    {
        internal const int MaxSealedBytes = 65536;
        internal const string EnvelopeTooLarge = "event_too_large";
        private static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false, true);
        private static readonly HashSet<string> FactNames = new HashSet<string>(StringComparer.Ordinal)
            { "workspace_key", "app_key", "environment_key", "event_id", "event_name", "event_ts", "props" };

        private static readonly string[] EnvelopeNames = { "schema_version", "source", "attestation_seal",
            "anonymous_id", "session_id", "session_sequence", "platform", "app_version", "app_build" };

        // These identities are absent from the sealed client contract, not
        // future extensions. Include wire and recognized field spellings.
        private static readonly HashSet<string> OmittedIdentityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "user_id", "UserID", "client_id", "ClientID", "subject_key", "SubjectKey",
            "workspace_id", "WorkspaceID", "app_id", "AppID", "environment_id", "EnvironmentID",
            "project_id", "ProjectID", "game_id", "GameID", "env",
            "user_id_verified", "UserIDVerified", "user_unverified_hash", "UserUnverifiedHash",
            "anon_alias_hash", "AnonAliasHash"
        };

        internal static ShardPilotHttpRequest BuildRequest(ShardPilotConfig config, ShardPilotExperimentApplication application)
        {
            var body = new StringBuilder("{");
            Append(body, "app_key", config.AppKey);
            Append(body, "environment_key", config.EnvironmentKey);
            Append(body, "experiment_key", application.ExperimentKey);
            Append(body, "experiment_version", application.ExperimentVersion);
            Append(body, "variant_key", application.VariantKey);
            Append(body, "subject_key", application.SubjectKey);
            Append(body, "served_revision", application.ServedRevision);
            Append(body, "served_kill_gate", application.ServedKillGate);
            Append(body, "served_at", application.ServedAt);
            Append(body, "applied_at", application.AppliedAt);
            Append(body, application.IsOutcome ? "outcome_id" : "exposure_id", application.OccurrenceId);
            if (application.IsOutcome)
            {
                Append(body, "occurred_at", application.OccurredAt);
                Append(body, "outcome_key", application.OutcomeKey);
                Append(body, "outcome_value", application.OutcomeValue);
            }
            body.Append(",\"attributes\":");
            var attributes = new StringBuilder("{");
            if (application.Attributes != null)
                foreach (var pair in application.Attributes) Append(attributes, pair.Key, pair.Value);
            body.Append(attributes).Append("}}");
            return new ShardPilotHttpRequest { Method = "POST", ContentType = "application/json",
                Url = config.AppUrl.TrimEnd('/') + "/api/cp/v1/runtime/experiments/" +
                    (application.IsOutcome ? "outcomes" : "exposures"),
                Bearer = config.PublishableKey, Timeout = config.HttpTimeout, Body = body.ToString() };
        }

        private static void Append(StringBuilder builder, string name, object value)
        {
            if (builder.Length > 1) builder.Append(',');
            ShardPilotJson.AppendQuoted(builder, name);
            builder.Append(':');
            if (value is long) builder.Append(((long)value).ToString(CultureInfo.InvariantCulture));
            else if (value is bool) builder.Append((bool)value ? "true" : "false");
            else ShardPilotJson.AppendQuoted(builder, (string)value);
        }

        internal static bool TryDecodeResponse(ShardPilotConfig config, ShardPilotExperimentApplication application,
            string body, out ShardPilotSealedExperimentFact fact)
        {
            fact = null;
            Dictionary<string, object> values;
            Dictionary<string, string> raw;
            if (!WithinBudget(body) || !ShardPilotJsonParser.TryParseUniqueObject(body, out values, out raw) ||
                !raw.ContainsKey("fact")) return false;
            ShardPilotSealedExperimentFact candidate;
            if (!TryDecodeFact(raw["fact"], Text(values, "seal"), out candidate)) return false;
            var eventName = application.IsOutcome ? "experiment_outcome" : "experiment_exposure";
            if (candidate.WorkspaceKey != config.WorkspaceKey || candidate.AppKey != config.AppKey ||
                candidate.EnvironmentKey != config.EnvironmentKey || candidate.EventName != eventName ||
                candidate.ExperimentKey != application.ExperimentKey || candidate.VariantKey != application.VariantKey ||
                candidate.ExperimentVersion != application.ExperimentVersion ||
                candidate.EventId != EventId(config.WorkspaceKey, candidate.Attestation, eventName,
                    application.ExperimentKey, application.ExperimentVersion, candidate.SubjectFactKey, application.OccurrenceId) ||
                !TimeMatches(application.IsOutcome ? application.OccurredAt : application.AppliedAt, candidate.EventTimestamp) ||
                (application.IsOutcome && (candidate.OutcomeKey != application.OutcomeKey || candidate.OutcomeValue != application.OutcomeValue)))
                return false;
            fact = candidate;
            return true;
        }

        // Validate required fields, allowing additive signed members. Attestation
        // is opaque; analytics verifies seal authenticity, never the SDK.
        internal static bool TryDecodeFact(string raw, string seal, out ShardPilotSealedExperimentFact fact)
        {
            return TryDecodeFact(raw, seal, false, out fact);
        }

        private static bool TryDecodeFact(string raw, string seal, bool isEnvelope, out ShardPilotSealedExperimentFact fact)
        {
            fact = null;
            Dictionary<string, object> values;
            Dictionary<string, string> tokens;
            if (!IsSeal(seal) || !WithinBudget(raw) ||
                !ShardPilotJsonParser.TryParseUniqueObject(raw, out values, out tokens)) return false;
            foreach (var name in FactNames) if (!values.ContainsKey(name)) return false;
            foreach (var name in values.Keys) if (OmittedIdentityNames.Contains(name)) return false;
            // The client appends these metadata fields. A signed root cannot
            // claim one and create a duplicate in the assembled envelope.
            if (!isEnvelope)
                foreach (var name in EnvelopeNames) if (values.ContainsKey(name)) return false;
            var props = values["props"] as Dictionary<string, object>;
            if (props == null) return false;
            foreach (var name in new[] { "workspace_key", "app_key", "environment_key", "event_name" })
                if (string.IsNullOrEmpty(Text(values, name))) return false;
            if (!ValidTime(Text(values, "event_ts")) || !IsFactKey(Text(values, "event_id"), "cpx1_") ||
                !IsFactKey(Text(props, "assignment_key"), "sfk1_") || Text(props, "assignment_unit") != "client_id" ||
                !ShardPilotExperimentsLogic.IsVocabularyToken(Text(props, "attestation")) ||
                string.IsNullOrEmpty(Text(props, "experiment_key")) || string.IsNullOrEmpty(Text(props, "variant_key"))) return false;
            // Validate required members by type, independently of future fields.
            object version;
            if (!props.TryGetValue("experiment_version", out version) || !(version is long) || (long)version < 1) return false;
            if (Text(values, "event_name") == "experiment_outcome")
            {
                object amount;
                if (string.IsNullOrEmpty(Text(props, "outcome_key")) ||
                    !props.TryGetValue("outcome_value", out amount) || !(amount is long)) return false;
            }
            else if (Text(values, "event_name") != "experiment_exposure") return false;
            fact = new ShardPilotSealedExperimentFact(raw, seal, values, props, isEnvelope);
            return true;
        }

        internal static bool WithinBudget(string text)
        {
            if (string.IsNullOrEmpty(text) || text.Length > MaxSealedBytes) return false;
            try { return StrictUtf8.GetByteCount(text) <= MaxSealedBytes; }
            catch (EncoderFallbackException) { return false; }
        }

        private static string Text(Dictionary<string, object> values, string name)
        {
            object value;
            return values.TryGetValue(name, out value) ? value as string : null;
        }

        private static bool IsFactKey(string value, string prefix)
        {
            if (value == null || value.Length != 69 || !value.StartsWith(prefix, StringComparison.Ordinal)) return false;
            for (var i = 5; i < value.Length; i++)
                if (!(value[i] >= '0' && value[i] <= '9') && !(value[i] >= 'a' && value[i] <= 'f')) return false;
            return true;
        }

        private static bool IsSeal(string seal)
        {
            return seal != null && seal.Length <= 1024 && Regex.IsMatch(seal, @"\Asps1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\z");
        }

        internal static string EventId(string workspace, string attestation, string eventName,
            string experiment, long version, string factKey, string occurrence)
        {
            // Correlation only: use the returned fact key, which can legitimately
            // rotate with the server salt. Never derive a subject key client-side.
            var input = string.Join("|", workspace, attestation, eventName, experiment,
                version.ToString(CultureInfo.InvariantCulture), factKey, occurrence);
            using (var sha = SHA256.Create())
            {
                var digest = sha.ComputeHash(StrictUtf8.GetBytes(input));
                var builder = new StringBuilder("cpx1_");
                foreach (var octet in digest) builder.Append(octet.ToString("x2", CultureInfo.InvariantCulture));
                return builder.ToString();
            }
        }

        private static bool ValidTime(string stamp)
        {
            if (stamp == null || !Regex.IsMatch(stamp, @"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z\z")) return false;
            DateTime second;
            return DateTime.TryParseExact(stamp.Substring(0, 19), "yyyy-MM-dd'T'HH:mm:ss",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out second);
        }

        internal static string Envelope(ShardPilotConfig config, ShardPilotEvent ev)
        {
            var fact = ev.SealedFact;
            if (fact.IsEnvelope) return fact.Raw;
            var end = fact.Raw.LastIndexOf('}');
            var builder = new StringBuilder(fact.Raw.Substring(0, end));
            Append(builder, "schema_version", 2L);
            Append(builder, "source", "client");
            Append(builder, "attestation_seal", fact.Seal);
            Append(builder, "anonymous_id", ev.AnonymousId);
            Append(builder, "session_id", ev.SessionId);
            if (ev.SessionSequence > 0) Append(builder, "session_sequence", ev.SessionSequence);
            var platform = ShardPilotEnvelopePlatform.Normalize(ev.Platform ?? config.Platform);
            if (!string.IsNullOrEmpty(platform)) Append(builder, "platform", platform);
            if (!string.IsNullOrEmpty(ev.AppVersion ?? config.AppVersion)) Append(builder, "app_version", ev.AppVersion ?? config.AppVersion);
            if (!string.IsNullOrEmpty(ev.AppBuild ?? config.AppBuild)) Append(builder, "app_build", ev.AppBuild ?? config.AppBuild);
            return builder.Append(fact.Raw.Substring(end)).ToString();
        }

        internal static bool AdmitEnvelope(string raw, string id, out ShardPilotSealedExperimentFact fact)
        {
            fact = null;
            Dictionary<string, object> values; Dictionary<string, string> tokens;
            // Classify the whole envelope BEFORE applying the sealed size budget.
            // The ordinary spool has its own configurable byte limit.
            if (!ShardPilotJsonParser.TryParseUniqueObject(raw, out values, out tokens)) return false;
            object schema, name, member;
            var ordinary = !values.TryGetValue("schema_version", out schema) ||
                schema is long && (long)schema >= 1 && (long)schema <= 65535;
            // Scope keys are shared by ordinary and sealed envelopes.
            var sealedMembers = values.ContainsKey("attestation_seal");
            if (ordinary && !sealedMembers)
                return ShardPilotIds.IsUuidV7(id) && values.TryGetValue("event_id", out member) && member as string == id &&
                    (!values.TryGetValue("event_name", out name) || name as string != "experiment_exposure" && name as string != "experiment_outcome");
            if (!WithinBudget(raw) || !(schema is long) || (long)schema != 2) return false;
            var required = new[] { "source", "attestation_seal", "anonymous_id", "session_id" };
            foreach (var key in required)
                if (!values.TryGetValue(key, out member) || !(member is string) || ((string)member).Length == 0) return false;
            if ((string)values["source"] != "client") return false;
            foreach (var pair in values)
            {
                if (FactNames.Contains(pair.Key) || pair.Key == "schema_version") continue;
                if (Array.IndexOf(required, pair.Key) >= 0) continue;
                if (pair.Key == "session_sequence") { if (!(pair.Value is long)) return false; }
                else if (pair.Key == "platform" || pair.Key == "app_version" || pair.Key == "app_build")
                { if (!(pair.Value is string)) return false; }
            }
            // Validate the assembled value instead of reconstructing a selected
            // subset of signed fields. The spool and replay retain these bytes.
            if (!TryDecodeFact(raw, (string)values["attestation_seal"], true, out fact)) return false;
            if (fact.EventId == id) return true;
            fact = null;
            return false;
        }

        internal static bool TimeMatches(string expected, string returned)
        {
            return ValidTime(expected) && ValidTime(returned) && CanonicalTime(expected) == CanonicalTime(returned);
        }

        private static string CanonicalTime(string stamp)
        {
            if (stamp.Length == 20) return stamp;
            return stamp.Substring(0, stamp.Length - 1).TrimEnd('0').TrimEnd('.') + "Z";
        }
    }
}
