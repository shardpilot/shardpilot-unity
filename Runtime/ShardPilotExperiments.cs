using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using WithdrawalFrame = ShardPilot.ShardPilotWithdrawalJournal.Frame;
using WithdrawalKind = ShardPilot.ShardPilotWithdrawalForm.Kind;

namespace ShardPilot
{
    /// <summary>The game's declared age band for experiment eligibility.</summary>
    public enum ShardPilotExperimentAgeBand
    {
        Unknown = 0,
        UnderThreshold = 1,
        Adult = 2
    }

    /// <summary>Builds fetch attributes from a game-supplied age declaration.</summary>
#nullable enable annotations
    public static class ShardPilotExperimentAttributes
    {
        /// <summary>
        /// Copies optional targeting attributes and sets <c>age_band</c>.
        /// Only a game-declared Adult can be eligible for client-id experiments;
        /// omitting a declaration is ineligible. This does not change consent
        /// or override a conflicting <c>custom_attribute_age_band</c> declaration.
        /// </summary>
        public static Dictionary<string, string?> WithAgeBand(
            ShardPilotExperimentAgeBand ageBand, IDictionary<string, string?>? attributes = null)
        {
            string value;
            switch (ageBand)
            {
                case ShardPilotExperimentAgeBand.Unknown: value = "unknown"; break;
                case ShardPilotExperimentAgeBand.UnderThreshold: value = "under_threshold"; break;
                case ShardPilotExperimentAgeBand.Adult: value = "adult"; break;
                default: throw new ArgumentOutOfRangeException(nameof(ageBand));
            }
            var result = attributes == null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(attributes, StringComparer.Ordinal);
            result["age_band"] = value;
            return result;
        }
    }

    /// <summary>
    /// Result of an experiment-assignment fetch.
    /// <list type="bullet">
    /// <item><c>Ok=true, FromCache=false</c> — a fresh server decision: either
    /// <c>Assigned=true</c> with the variant, or <c>Assigned=false</c> with
    /// <c>Reason</c> (null = the deterministic traffic gate,
    /// "kill_switch" = an operator kill, "targeting_unmatched", or
    /// "age_ineligible" = no eligible game-declared age band, or "unknown"
    /// = a future reason or assignment unit).</item>
    /// <item><c>Ok=true, FromCache=true</c> — a transient failure served the
    /// cached assignment; <c>Error</c> carries why the network could not
    /// refresh it. Only assigned entries are ever cached, so a cache serve
    /// always carries a variant.</item>
    /// <item><c>Ok=false</c> — nothing usable: <c>Error</c> is the reason
    /// ("unauthorized" fails closed, "consent_unknown"/"consent_denied" is
    /// the granted-only plane refusing, permanent HTTP errors, or a
    /// transient failure with no cache).</item>
    /// </list>
    /// </summary>
    public sealed class ShardPilotExperimentResult
    {
        public bool Ok { get; internal set; }
        public bool FromCache { get; internal set; }
        public string? Error { get; internal set; }
        public bool Assigned { get; internal set; }
        public string? Reason { get; internal set; }
        public string? VariantKey { get; internal set; }
        public IReadOnlyDictionary<string, object?>? VariantPayload { get; internal set; }
        public long? Version { get; internal set; }
        public IReadOnlyDictionary<string, object?>? Boundary { get; internal set; }

        internal static ShardPilotExperimentResult Failure(string error)
        {
            return new ShardPilotExperimentResult
            {
                Ok = false,
                FromCache = false,
                Error = error
            };
        }
    }

    /// <summary>
    /// One cached assignment: the fields the getters serve and the fact
    /// producers stamp, plus the normalized attribute set the fetch that
    /// installed it sent (re-sent verbatim by revalidation) and the freshness
    /// stamp that orders records for a scope.
    /// </summary>
#nullable restore annotations
    internal sealed class ShardPilotExperimentEntry
    {
        public string AssignmentKey { get; set; }
        public string VariantKey { get; set; }
        public Dictionary<string, object> VariantPayload { get; set; }
        public long Version { get; set; }
        public string AssignmentUnit { get; set; }
        public string SubjectFactKey { get; set; }
        public string SubjectKey { get; set; }
        public List<KeyValuePair<string, string>> Attributes { get; set; }
        public DateTime FetchedAtUtc { get; set; }
        public long ServedRevision { get; set; }
        public bool ServedKillGate { get; set; }
        public string ServedAt { get; set; }
    }

    /// <summary>
    /// The directives one classified fetch outcome hands the stateful
    /// install, mirroring the pure/stateful split of the remote-config
    /// client. Exactly one of the shape flags describes each outcome.
    /// </summary>
    internal sealed class ShardPilotExperimentOutcome
    {
        public ShardPilotExperimentResult Result { get; set; }

        /// <summary>Settles the per-(scope, experiment) fence: any fresh
        /// 200, 401/403, 404, 400, and other permanent errors. Transient and
        /// cache-served outcomes never settle.</summary>
        public bool Authoritative { get; set; }

        /// <summary>Cache this assignment (exists exactly for 200 assigned).</summary>
        public ShardPilotExperimentEntry NewEntry { get; set; }

        /// <summary>Drop the experiment's cached assignment: every
        /// not-assigned 200 shape, 404, and non-grammar 400.</summary>
        public bool DropEntry { get; set; }

        /// <summary>The <c>age_ineligible</c> refusal: besides dropping the
        /// entry, the install WITHDRAWS every still-owed application of this
        /// experiment for the refused subject (owed exposures and
        /// accepted-but-undelivered facts). Every other
        /// not-assigned reason keeps the ordinary drop, whose owed
        /// exposures survive as facts.</summary>
        public bool WithdrawOwedApplications { get; set; }

        /// <summary>The refusal body's <c>subject_fact_key</c> when it
        /// passes the fact-key grammar (null otherwise): the refused
        /// subject's fact identity, used with the dropped entry's own key to
        /// match the facts the withdrawal removes.</summary>
        public string RefusedSubjectFactKey { get; set; }

        /// <summary>Drop the whole durable record (the real-subjects kill
        /// sentinel 403).</summary>
        public bool DropAll { get; set; }

        /// <summary>401/403: stop serving, halt revalidation, fail closed.</summary>
        public bool AuthBlocked { get; set; }

        /// <summary>The subject-grammar 400 sentinel: the caller may re-mint
        /// the subject id once per process and retry.</summary>
        public bool Remint { get; set; }

        /// <summary>Transient: serve-cache outcome that paces the
        /// revalidation backoff.</summary>
        public bool Transient { get; set; }
        public bool TerminalAssignment { get; set; }

        /// <summary>Server Retry-After (seconds) from a 429/5xx, when sent.</summary>
        public int RetryAfterSeconds { get; set; }
    }

    /// <summary>
    /// Pure experiment-assignment decision logic,
    /// separated from transport and disk IO so every branch is testable
    /// headlessly. Wire contract:
    /// <c>GET {AppUrl}/api/cp/v1/runtime/experiments/assignment
    /// ?app_key=&amp;environment_key=&amp;experiment_key=&amp;subject_key=&amp;&lt;attributes&gt;</c>
    /// with the publishable <c>PublishableKey</c> as Bearer — the same credential and
    /// host as the remote-config fetch, path-swapped; no new endpoint
    /// configuration exists.
    /// </summary>
    internal static class ShardPilotExperimentsLogic
    {
        public const string AssignmentRoute = "/api/cp/v1/runtime/experiments/assignment";

        private const char ScopeSeparator = '\u001f';

        /// <summary>
        /// Revalidation cadence (the SDK's contribution to the kill-switch
        /// reach): re-issue the assignment GET for every cached entry,
        /// batched per tick, every 300 seconds with ±10% uniform jitter. The
        /// endpoint has no conditional requests (no ETag), so revalidation
        /// is a plain re-fetch; an offline client keeps its variant
        /// indefinitely — the documented kill-latency caveat.
        /// </summary>
        public const int RevalidateIntervalSeconds = 300;
        public const double RevalidateJitter = 0.1;

        /// <summary>Transient-failure backoff for the revalidation cadence
        /// (transport parity): full jitter in [base, ceiling], ceiling
        /// doubling per consecutive failure up to the cap; a server
        /// Retry-After (clamped to one day) overrides the computed wait.</summary>
        public const int BackoffBaseSeconds = 1;
        public const int BackoffCapSeconds = 60;
        public const int MaxDeferSeconds = 86400;

        /// <summary>Server error sentinels reacted to by exact body-text
        /// equality (the error contract distinguishes same-status outcomes
        /// only by the body's <c>error</c> string).</summary>
        public const string SentinelRealSubjectsDisabled = "experiment real-subject assignment is disabled";
        public const string SentinelSubjectGrammar = "experiment metadata must use synthetic local-safe identifiers only";

        public const int MaxAttributeValueBytes = 512;
        public const int MaxAttributes = 64;
        private const string CustomAttributePrefix = "custom_attribute_";

        private static readonly HashSet<string> AllowedAttributes = new HashSet<string>(StringComparer.Ordinal)
        {
            "geo", "app_version", "device_type", "install_date", "user_segment"
        };

        private static readonly HashSet<string> KnownNotAssignedReasons = new HashSet<string>(StringComparer.Ordinal)
        {
            "kill_switch", "targeting_unmatched", "age_ineligible"
        };

        /// <summary>The not-assigned reason that withdraws the subject's
        /// owed applications instead of preserving them (see
        /// <see cref="ShardPilotExperimentOutcome.WithdrawOwedApplications"/>).</summary>
        public const string ReasonAgeIneligible = "age_ineligible";

        private static readonly HashSet<string> KnownAssignmentUnits = new HashSet<string>(StringComparer.Ordinal)
        {
            "synthetic_subject_key", "client_id"
        };

        /// <summary>
        /// The wire grammar for a client subject id:
        /// <c>^spcid_[A-Za-z0-9_-]{20,64}$</c>. Accepting the full grammar on
        /// load (not just the SDK's own 32-hex body) keeps a stored id sticky
        /// across SDK upgrades: re-minting re-buckets, so an id that is still
        /// wire-valid is never discarded.
        /// </summary>
        public static bool ValidSubjectId(string value)
        {
            if (value == null || value.Length < 26 || value.Length > 70)
            {
                return false;
            }

            if (!value.StartsWith("spcid_", StringComparison.Ordinal))
            {
                return false;
            }

            for (var i = 6; i < value.Length; i++)
            {
                var ch = value[i];
                var ok = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                         (ch >= '0' && ch <= '9') || ch == '_' || ch == '-';
                if (!ok)
                {
                    return false;
                }
            }

            return true;
        }

        // Vocabulary extensions are nonblank strings; preserve the original
        // bytes rather than trimming or inventing an alias.
        internal static bool IsVocabularyToken(string value)
        {
            return !string.IsNullOrWhiteSpace(value);
        }

        /// <summary>
        /// Mint a fresh subject id: "spcid_" + the SDK's UUIDv7 with dashes
        /// removed (32 lowercase hex chars, 38 chars total — inside the wire
        /// grammar's 20-64 body bound and charset). SDK-managed: there is no
        /// host override path — no public setter and no config field is read
        /// for it.
        /// </summary>
        public static string MintSubjectId()
        {
            return "spcid_" + ShardPilotIds.NewUuidV7().Replace("-", string.Empty);
        }

        /// <summary>
        /// The derived analytics fact subject for client_id-unit
        /// assignments: <c>^sfk1_[0-9a-f]{64}$</c>. This — never the raw
        /// subject id — is the only value permitted as the
        /// <c>assignment_key</c> prop on experiment facts, so it is grammar
        /// checked before assignment admission. A present malformed client
        /// echo refuses the response; synthetic assignments ignore the echo.
        /// </summary>
        public static bool ValidSubjectFactKey(string value)
        {
            if (value == null || value.Length != 69 ||
                !value.StartsWith("sfk1_", StringComparison.Ordinal))
            {
                return false;
            }

            for (var i = 5; i < value.Length; i++)
            {
                var ch = value[i];
                if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// A short, non-secret fingerprint of the publishable key, used as a
        /// cache-scope component: an in-place key swap (a new tenant on the
        /// same install) must scope-miss the previous key's cached
        /// assignments, never serve them. Twelve hex chars of SHA-256 — not
        /// reversible, never sent anywhere.
        /// </summary>
        public static string CredentialFingerprint(string apiKey)
        {
            if (string.IsNullOrEmpty(apiKey))
            {
                return string.Empty;
            }

            using (var sha = SHA256.Create())
            {
                var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(apiKey));
                var builder = new StringBuilder(12);
                for (var i = 0; i < 6; i++)
                {
                    builder.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }

                return builder.ToString();
            }
        }

        /// <summary>
        /// Builds the assignment URL from ordered (name, value) query pairs;
        /// both sides are escaped (injective percent-escaping, the shared
        /// remote-config discipline), so an identifier or attribute value
        /// containing "&amp;", "=", or "#" cannot restructure the query.
        /// </summary>
        public static string BuildUrl(string baseUrl, List<KeyValuePair<string, string>> query)
        {
            var builder = new StringBuilder(baseUrl.TrimEnd('/'));
            builder.Append(AssignmentRoute);
            for (var i = 0; i < query.Count; i++)
            {
                builder.Append(i == 0 ? '?' : '&');
                builder.Append(Uri.EscapeDataString(query[i].Key));
                builder.Append('=');
                builder.Append(Uri.EscapeDataString(query[i].Value));
            }

            return builder.ToString();
        }

        /// <summary>
        /// Cache-scope discipline shared with the remote-config client:
        /// components are escaped and joined with a separator no escaped
        /// component can contain, so two distinct (workspace, environment,
        /// subject, credential, url) tuples can never collide into one scope
        /// string. The experiment key is deliberately NOT part of the base
        /// scope — entries are keyed by it inside the record, so the full
        /// cache key is the (scope, experiment_key) pair. The app id and
        /// credential fingerprint are both swappable in place via
        /// re-initialization, and the previous app's/tenant's assignments
        /// (and their fact keys) must scope-miss rather than serve.
        /// </summary>
        public static string BuildScope(
            string workspaceId,
            string appId,
            string environmentId,
            string subjectId,
            string credentialFingerprint,
            string baseUrl)
        {
            return Uri.EscapeDataString(workspaceId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(appId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(environmentId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(subjectId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(credentialFingerprint ?? string.Empty) + ScopeSeparator +
                   ShardPilotConfig.Trim(baseUrl).TrimEnd('/');
        }

        /// <summary>True when two cache scopes belong to the same TENANT —
        /// every <see cref="BuildScope"/> component equal except the subject
        /// id. Malformed scopes
        /// fall back to exact equality (fail closed on the tenant claim).</summary>
        public static bool ScopesShareTenant(string a, string b)
        {
            if (string.Equals(a, b, StringComparison.Ordinal))
            {
                return true;
            }

            if (a == null || b == null)
            {
                return false;
            }

            var partsA = a.Split(ScopeSeparator);
            var partsB = b.Split(ScopeSeparator);
            if (partsA.Length != 6 || partsB.Length != 6)
            {
                return false;
            }

            for (var i = 0; i < 6; i++)
            {
                if (i == 3)
                {
                    // The subject component — the one a re-mint rotates.
                    continue;
                }

                if (!string.Equals(partsA[i], partsB[i], StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        private static bool ValidAttributeName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return false;
            }

            if (AllowedAttributes.Contains(name))
            {
                return true;
            }

            if (name.StartsWith(CustomAttributePrefix, StringComparison.Ordinal))
            {
                var suffix = name.Length - CustomAttributePrefix.Length;
                return suffix >= 1 && suffix <= 64;
            }

            return false;
        }

        /// <summary>
        /// Normalize host-supplied targeting attributes into the ordered
        /// (name, value) pairs a fetch sends: names outside the server's
        /// vocabulary (the fixed allowlist plus <c>custom_attribute_&lt;1-64
        /// chars&gt;</c>) and unusable values are DROPPED (counted for
        /// diagnostics), never sent — an invented name would be ignored
        /// server-side at best, and an oversized value would fail the whole
        /// fetch whenever the experiment carries a targeting condition.
        /// Dropping fails toward targeting_unmatched, the safe direction.
        /// Values are trimmed and bounded to 512 UTF-8 bytes; the surviving
        /// pairs are sorted by name (ordinal) and capped at 64, mirroring the
        /// server's own sorted-key consideration order.
        /// </summary>
        public static List<KeyValuePair<string, string>> NormalizeAttributes(
            IDictionary<string, string> attributes,
            out int dropped)
        {
            dropped = 0;
            var pairs = new List<KeyValuePair<string, string>>();
            if (attributes == null || attributes.Count == 0)
            {
                return pairs;
            }

            var names = new List<string>(attributes.Keys);
            names.Sort(StringComparer.Ordinal);
            for (var i = 0; i < names.Count; i++)
            {
                var name = names[i];
                var value = attributes[name];
                var text = value == null ? null : value.Trim();
                if (!ValidAttributeName(name) || string.IsNullOrEmpty(text) ||
                    Encoding.UTF8.GetByteCount(text) > MaxAttributeValueBytes)
                {
                    dropped++;
                }
                else if (pairs.Count >= MaxAttributes)
                {
                    dropped++;
                }
                else
                {
                    pairs.Add(new KeyValuePair<string, string>(name, text));
                }
            }

            return pairs;
        }

        // Mandatory age declarations precede optional targeting normalization.
        // Reserve both spellings within the existing cap: dropping a refusing
        // alias or trimming it into "adult" would change the server's verdict.
        // Remote config retains its separate, unchanged targeting vocabulary.
        public static List<KeyValuePair<string, string>> NormalizeExperimentAttributes(
            IDictionary<string, string> attributes, out int dropped)
        {
            var targeting = attributes == null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(attributes, StringComparer.Ordinal);
            var pairs = new List<KeyValuePair<string, string>>();
            var invalidBands = 0;
            foreach (var key in new[] { "age_band", "custom_attribute_age_band" })
            {
                string value;
                if (!targeting.TryGetValue(key, out value)) continue;
                targeting.Remove(key);
                if (value != "adult" && value != "unknown" && value != "under_threshold")
                {
                    value = "unknown";
                    invalidBands++;
                }
                pairs.Add(new KeyValuePair<string, string>(key, value));
            }
            var normalized = NormalizeAttributes(targeting, out dropped);
            dropped += invalidBands;
            foreach (var pair in normalized)
            {
                if (pairs.Count < MaxAttributes) pairs.Add(pair);
                else dropped++;
            }
            pairs.Sort((left, right) => string.CompareOrdinal(left.Key, right.Key));
            return pairs;
        }

        /// <summary>
        /// Whether stored assignment attributes carry an age declaration under
        /// either spelling, whatever its value: the server decided admission on
        /// that declaration when it stored the entry.
        /// </summary>
        internal static bool HasAgeDeclaration(List<KeyValuePair<string, string>> attributes)
        {
            if (attributes == null) return false;
            foreach (var pair in attributes)
            {
                if (pair.Key == "age_band" || pair.Key == "custom_attribute_age_band") return true;
            }
            return false;
        }

        /// <summary>
        /// Whether a host fetch's attributes declare an age other than adult
        /// under either spelling. Only an exact "adult" admits (the fetch
        /// normalizer sends anything else as "unknown"), so an unknown,
        /// under-threshold, empty or oversized value is non-adult. An absent
        /// declaration is not one: it declares nothing.
        /// </summary>
        internal static bool DeclaresNonAdultAge(IDictionary<string, string> attributes)
        {
            if (attributes == null) return false;
            foreach (var pair in attributes)
            {
                if (IsAgeDeclarationName(pair.Key) && !string.Equals(pair.Value, "adult", StringComparison.Ordinal))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Whether a host fetch's attributes declare adult under every age
        /// spelling they carry, and carry at least one.
        /// </summary>
        internal static bool DeclaresAdultAge(IDictionary<string, string> attributes)
        {
            if (attributes == null) return false;
            var declared = false;
            foreach (var pair in attributes)
            {
                if (!IsAgeDeclarationName(pair.Key)) continue;
                if (!string.Equals(pair.Value, "adult", StringComparison.Ordinal)) return false;
                declared = true;
            }
            return declared;
        }

        private static bool IsAgeDeclarationName(string name)
        {
            return string.Equals(name, "age_band", StringComparison.Ordinal) ||
                   string.Equals(name, "custom_attribute_age_band", StringComparison.Ordinal);
        }

        internal static string ResponseErrorText(ShardPilotHttpResponse response)
        {
            Dictionary<string, object> decoded;
            if (response == null || string.IsNullOrEmpty(response.Body) ||
                !ShardPilotJsonParser.TryParseObject(response.Body, out decoded))
            {
                return null;
            }

            object value;
            return decoded.TryGetValue("error", out value) ? value as string : null;
        }

        /// <summary>
        /// Parses an assignment 200 body into a result plus (for the
        /// assigned shape) the cache entry. Strict on the fields the
        /// contract pins — a body this cannot interpret is MALFORMED (a
        /// transient outcome), so a garbled payload can neither overwrite a
        /// good cache nor feed the producers:
        /// <list type="bullet">
        /// <item><c>assigned</c> must be a boolean and the body must echo
        /// the REQUESTED <c>experiment_key</c> — a wrong-experiment echo
        /// would misattribute another experiment's decision (and its
        /// exposures) to this scope;</item>
        /// <item>an assigned decision requires non-empty
        /// <c>assignment_key</c> + <c>variant_key</c>, a numeric integral
        /// <c>version</c>, and a <c>boundary.assignment_unit</c>;
        /// <c>variant_payload</c>, when the member is present, must be a
        /// JSON object — an array, string, number, or null payload is
        /// malformed, never coerced to "no payload";</item>
        /// <item>only the three published refusal reasons exist
        /// ("kill_switch", "targeting_unmatched", "age_ineligible") — absence is the fourth
        /// (deterministic traffic-gate) shape, and any other value is a
        /// body this build cannot interpret.</item>
        /// </list>
        /// Returns null when the body is malformed.
        /// </summary>
        /// <summary>True when the body carries <paramref name="member"/> and
        /// it is anything other than the exact expected string — a present
        /// non-string (JSON null included) or a disagreeing value. An ABSENT
        /// member accepts: the echoes are integrity checks, not required
        /// fields.</summary>
        private static bool EchoDisagrees(
            Dictionary<string, object> decoded, string member, string expected)
        {
            object value;
            if (!decoded.TryGetValue(member, out value))
            {
                return false;
            }

            var text = value as string;
            return text == null || !string.Equals(text, expected ?? string.Empty, StringComparison.Ordinal);
        }

        public static ShardPilotExperimentOutcome ParseAssignment(
            string body,
            string experimentKey,
            string appKey,
            string environmentKey,
            DateTime nowUtc)
        {
            Dictionary<string, object> decoded;
            Dictionary<string, string> tokens;
            if (!ShardPilotJsonParser.TryParseUniqueObject(body, out decoded, out tokens))
            {
                return null;
            }

            object assignedValue;
            if (!decoded.TryGetValue("assigned", out assignedValue) || !(assignedValue is bool))
            {
                return null;
            }

            // Scope-field echoes: a body naming ANOTHER experiment, app, or
            // environment would misattribute a foreign decision (and its
            // exposures) to this scope — present-and-disagreeing or
            // present-non-string is malformed BEFORE any install; an absent
            // echo accepts.
            if (EchoDisagrees(decoded, "experiment_key", experimentKey) ||
                EchoDisagrees(decoded, "app_key", appKey) ||
                EchoDisagrees(decoded, "environment_key", environmentKey))
            {
                return null;
            }

            object boundaryValue;
            var boundary = decoded.TryGetValue("boundary", out boundaryValue)
                ? boundaryValue as Dictionary<string, object>
                : null;

            // Unknown string units are authoritative non-assignments. Wrong
            // types remain malformed; no future unit can mint client facts.
            string unit = null;
            if (boundary != null)
            {
                object unitValue;
                if (boundary.TryGetValue("assignment_unit", out unitValue))
                {
                    unit = unitValue as string;
                    if (!IsVocabularyToken(unit))
                    {
                        return null;
                    }
                }
            }

            long version;
            var hasVersion = TryReadIntegralVersion(decoded, out version);

            // Presence-aware version decode: a PRESENT member must be an
            // integral number on EVERY shape — a JSON null, a string, or a
            // fractional value is a body this build cannot interpret
            // (malformed, cache retained), never read as the absent
            // tolerated form. Present-null is not absent.
            if (decoded.ContainsKey("version") && (!hasVersion || version <= 0))
            {
                return null;
            }

            var unknownUnit = unit != null && !KnownAssignmentUnits.Contains(unit);
            if ((bool)assignedValue && (!hasVersion || version <= 0)) return null;
            if ((bool)assignedValue && !unknownUnit)
            {
                object assignmentKeyValue;
                var assignmentKey = decoded.TryGetValue("assignment_key", out assignmentKeyValue)
                    ? assignmentKeyValue as string
                    : null;
                object variantKeyValue;
                var variantKey = decoded.TryGetValue("variant_key", out variantKeyValue)
                    ? variantKeyValue as string
                    : null;
                // The assigned version must be a POSITIVE integer — the
                // same grammar the record restore enforces (version <= 0
                // reads as corruption there, because the SDK never writes
                // one). Installing a non-positive version would mint a
                // cache this same SDK refuses to reload AND emit it in
                // exposure/outcome props; it is a body this build cannot
                // interpret — malformed, serve-stale, nothing installs.
                if (string.IsNullOrEmpty(assignmentKey) || string.IsNullOrEmpty(variantKey) ||
                    !hasVersion || version <= 0 || string.IsNullOrEmpty(unit))
                {
                    return null;
                }

                Dictionary<string, object> payload = null;
                object payloadValue;
                if (decoded.TryGetValue("variant_payload", out payloadValue))
                {
                    // The member is PRESENT: it must be a JSON object. The
                    // parser preserves the null/array/object distinction, so
                    // a null-bearing or non-object member reads as malformed
                    // here rather than silently becoming "no payload".
                    payload = payloadValue as Dictionary<string, object>;
                    if (payload == null)
                    {
                        return null;
                    }
                }

                object factKeyValue;
                var factKey = decoded.TryGetValue("subject_fact_key", out factKeyValue)
                    ? factKeyValue as string
                    : null;
                if (unit == "client_id" && decoded.ContainsKey("subject_fact_key") && !ValidSubjectFactKey(factKey))
                    return null;
                // Synthetic assignments never consume a fact-key echo.
                if (unit != "client_id") factKey = null;

                var entry = new ShardPilotExperimentEntry
                {
                    AssignmentKey = assignmentKey,
                    VariantKey = variantKey,
                    VariantPayload = payload,
                    Version = version,
                    AssignmentUnit = unit,
                    SubjectFactKey = factKey,
                    FetchedAtUtc = nowUtc
                };
                if (!ReadServingPins(decoded, entry) || entry.ServedKillGate) return null;
                return new ShardPilotExperimentOutcome
                {
                    Result = new ShardPilotExperimentResult
                    {
                        Ok = true,
                        FromCache = false,
                        Assigned = true,
                        VariantKey = variantKey,
                        VariantPayload = CopyPayload(payload),
                        Version = version,
                        Boundary = CopyPayload(boundary)
                    },
                    Authoritative = true,
                    NewEntry = entry
                };
            }

            string reason = null;
            object reasonValue;
            if (decoded.TryGetValue("reason", out reasonValue))
            {
                reason = reasonValue as string;
                if (!IsVocabularyToken(reason)) return null;
                if (!KnownNotAssignedReasons.Contains(reason)) reason = "unknown";
            }
            if (unknownUnit) reason = "unknown";

            // An age refusal names the refused subject by its fact key; a
            // malformed echo degrades to "unknown" (the dropped entry's own
            // key still identifies the subject), never to a foreign match.
            var ageIneligible = string.Equals(reason, ReasonAgeIneligible, StringComparison.Ordinal);
            string refusedFactKey = null;
            if (ageIneligible)
            {
                object refusedFactKeyValue;
                refusedFactKey = decoded.TryGetValue("subject_fact_key", out refusedFactKeyValue)
                    ? refusedFactKeyValue as string
                    : null;
                if (!ValidSubjectFactKey(refusedFactKey))
                {
                    refusedFactKey = null;
                }
            }

            return new ShardPilotExperimentOutcome
            {
                Result = new ShardPilotExperimentResult
                {
                    Ok = true,
                    FromCache = false,
                    Assigned = false,
                    Reason = reason,
                    Version = hasVersion ? version : (long?)null,
                    Boundary = CopyPayload(boundary)
                },
                Authoritative = true,
                DropEntry = true,
                WithdrawOwedApplications = ageIneligible,
                RefusedSubjectFactKey = refusedFactKey
            };
        }

        private static bool TryReadIntegralVersion(Dictionary<string, object> decoded, out long version)
        {
            version = 0;
            object value;
            if (!decoded.TryGetValue("version", out value))
            {
                return false;
            }

            if (value is long)
            {
                version = (long)value;
                return true;
            }

            return false;
        }

        internal static Dictionary<string, object> CopyPayload(Dictionary<string, object> value)
        {
            return CopyPayloadValue(value, 0) as Dictionary<string, object>;
        }

        /// <summary>Depth-bounded copy of a decoded JSON value, so a map
        /// handed to game code can be mutated freely without corrupting the
        /// cached entry later reads serve.</summary>
        private static object CopyPayloadValue(object value, int depth)
        {
            if (depth >= 16)
            {
                return null;
            }

            var map = value as Dictionary<string, object>;
            if (map != null)
            {
                var copy = new Dictionary<string, object>(map.Count, StringComparer.Ordinal);
                foreach (var pair in map)
                {
                    copy[pair.Key] = CopyPayloadValue(pair.Value, depth + 1);
                }

                return copy;
            }

            var list = value as List<object>;
            if (list != null)
            {
                var copy = new List<object>(list.Count);
                for (var i = 0; i < list.Count; i++)
                {
                    copy.Add(CopyPayloadValue(list[i], depth + 1));
                }

                return copy;
            }

            return value;
        }

        /// <summary>
        /// Serve the cached entry for a transient failure, or fail when none
        /// exists. A served entry is still a SUCCESS (<c>Ok=true</c>) — the
        /// game has a usable assignment — with <c>FromCache=true</c> and
        /// <c>Error</c> carrying why the network could not refresh it.
        /// </summary>
        public static ShardPilotExperimentOutcome ServeEntryOrFail(
            ShardPilotExperimentEntry entry,
            string error,
            int retryAfterSeconds)
        {
            var outcome = new ShardPilotExperimentOutcome
            {
                Transient = true,
                RetryAfterSeconds = retryAfterSeconds
            };
            if (entry != null)
            {
                outcome.Result = new ShardPilotExperimentResult
                {
                    Ok = true,
                    FromCache = true,
                    Assigned = true,
                    VariantKey = entry.VariantKey,
                    VariantPayload = CopyPayload(entry.VariantPayload),
                    Version = entry.Version,
                    Error = error
                };
            }
            else
            {
                outcome.Result = ShardPilotExperimentResult.Failure(error);
            }

            return outcome;
        }

        /// <summary>
        /// Decide one fetch outcome from the transport response and the
        /// cached entry. Pure (no IO, no state) so tests can drive every
        /// branch. Classification is status-first: an authoritative 401/403
        /// (and every other received non-200 status) is classified by its
        /// HTTP status even when the transport layer reports an error for
        /// the same response (a stalled or truncated body must not soften a
        /// received auth refusal into a cache-serving transient — the
        /// stalled-body strictness this SDK also applies to the remote-config
        /// classifier). A 200 whose transport errored has an untrustworthy
        /// body and stays transient.
        /// </summary>
        public static ShardPilotExperimentOutcome Apply(
            ShardPilotExperimentEntry entry,
            ShardPilotHttpResponse response,
            DateTime nowUtc,
            string experimentKey,
            string appKey,
            string environmentKey)
        {
            var status = response == null ? 0 : response.StatusCode;

            if (status == 200 && response != null && !response.TransportError)
            {
                var parsed = ParseAssignment(response.Body, experimentKey, appKey, environmentKey, nowUtc);
                if (parsed != null)
                {
                    return parsed;
                }

                return ServeEntryOrFail(entry, "malformed_response", 0);
            }

            // Unauthorized / forbidden fail CLOSED (a dark server, a revoked
            // or unscoped key, a suspended tenant — indistinguishable here,
            // all must stop serving): the cached record is never served for
            // this outcome and revalidation halts. The durable record is
            // kept EXCEPT under the real-subjects kill sentinel, which drops
            // it — the assignment and its subject-fact key must not outlive
            // the platform flipping that flag off. A truncated sentinel body
            // fails the exact-equality parse and reads as a generic 403 (no
            // drop) — the safe direction.
            if (status == 401 || status == 403)
            {
                var outcome = new ShardPilotExperimentOutcome
                {
                    Result = ShardPilotExperimentResult.Failure("unauthorized"),
                    Authoritative = true,
                    AuthBlocked = true
                };
                if (status == 403 &&
                    string.Equals(ResponseErrorText(response), SentinelRealSubjectsDisabled, StringComparison.Ordinal))
                {
                    outcome.DropAll = true;
                }

                return outcome;
            }

            // Unknown experiment key or nothing published in scope:
            // permanent for this key. Treated as a first-class not-assigned
            // answer (the host's actionable outcome is identical: no
            // variant); the cached assignment is dropped, never served
            // stale, and the drop stops revalidation from re-asking.
            if (status == 404)
            {
                return new ShardPilotExperimentOutcome
                {
                    Result = new ShardPilotExperimentResult
                    {
                        Ok = true,
                        FromCache = false,
                        Assigned = false,
                        Error = "not_found"
                    },
                    Authoritative = true,
                    DropEntry = true
                };
            }

            // Bad inputs are permanent for this input set — retrying
            // unchanged cannot help. The one self-healing case: the
            // subject-grammar sentinel with an SDK-minted id means the
            // persisted id went bad; the caller re-mints once (fresh-install
            // semantics) and retries — deliberately NOT a drop: the
            // assignment itself was never rejected, the subject id was.
            // Every OTHER 400 fails closed for the experiment: a cached
            // assignment whose revalidation is permanently rejected must not
            // keep serving stale forever while the cadence re-sends the same
            // invalid input set, so the entry drops (durably, with the usual
            // decision-time stamp raise).
            if (status == 400)
            {
                if (string.Equals(ResponseErrorText(response), SentinelSubjectGrammar, StringComparison.Ordinal))
                {
                    return new ShardPilotExperimentOutcome
                    {
                        Result = ShardPilotExperimentResult.Failure("bad_request"),
                        Authoritative = true,
                        Remint = true
                    };
                }

                return new ShardPilotExperimentOutcome
                {
                    Result = ShardPilotExperimentResult.Failure("bad_request"),
                    Authoritative = true,
                    DropEntry = true
                };
            }

            // Transient bucket: no connection, a transport-errored response
            // (a stalled 200 included), timeout, backpressure, or any server
            // error — including the 503 the endpoint answers when its
            // kill-state read fails (an explicit serve-stale-and-retry
            // case). Retry-After is honored on 429 and 5xx alike.
            if (response == null || (response.TransportError && status == 200) || status == 0 || status == 408 ||
                status == 429 || status >= 500)
            {
                var retryAfter = 0;
                if (response != null && (status == 429 || status >= 500))
                {
                    retryAfter = response.RetryAfterSeconds;
                }

                string reason;
                if (response == null || (response.TransportError && status <= 0))
                {
                    reason = "http_0";
                }
                else if (response.TransportError)
                {
                    reason = "transport_" + status.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    reason = status == 408 || status == 429 || status >= 500
                        ? "transient_" + status.ToString(CultureInfo.InvariantCulture)
                        : "http_" + status.ToString(CultureInfo.InvariantCulture);
                }

                return ServeEntryOrFail(entry, reason, retryAfter);
            }

            // Received non-retryable statuses fail closed for this session.
            // Keep the durable cache, but do not serve it or poll this key again
            // until a new session or an explicit successful refetch.
            return new ShardPilotExperimentOutcome
            {
                Result = ShardPilotExperimentResult.Failure("http_" + status.ToString(CultureInfo.InvariantCulture)),
                Authoritative = true,
                TerminalAssignment = true
            };
        }

        /// <summary>True when two normalized attribute sets are the same
        /// ordered (name, value) pairs; null counts as empty. Serve-stale
        /// consults it: a cached assignment fetched under one targeting
        /// context must not serve a transient failure in ANOTHER context —
        /// the server's verdict could legitimately differ (geo=US cached
        /// must not answer a geo=CA request during a 503).</summary>
        public static bool AttributeSetsMatch(
            List<KeyValuePair<string, string>> a,
            List<KeyValuePair<string, string>> b)
        {
            var countA = a == null ? 0 : a.Count;
            var countB = b == null ? 0 : b.Count;
            if (countA != countB)
            {
                return false;
            }

            for (var i = 0; i < countA; i++)
            {
                if (!string.Equals(a[i].Key, b[i].Key, StringComparison.Ordinal) ||
                    !string.Equals(a[i].Value, b[i].Value, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// The exposure de-dupe tuple: (experiment, version, subject) —
        /// exactly the documented once-per-session contract, nothing more.
        /// The assignment key is deliberately NOT part of it: it is normally
        /// deterministic for this tuple anyway, and a regenerated key for
        /// the same tuple must not over-count exposures.
        /// </summary>
        public static string ExposureTuple(string experimentKey, ShardPilotExperimentEntry entry)
        {
            return experimentKey + "\u001f" +
                   entry.Version.ToString(CultureInfo.InvariantCulture) + "\u001f" +
                   (entry.SubjectKey ?? string.Empty);
        }

        // ── durable record ───────────────────────────────────────────────────

        /// <summary>Refuse a record whose serialized form approaches the
        /// remote-config body clamp's neighborhood so one oversized payload
        /// cannot wedge the whole record.</summary>
        public const int MaxRecordBytes = 393216;

        public static string SerializeRecord(string scope, Dictionary<string, ShardPilotExperimentEntry> entries)
        {
            return SerializeRecord(scope, entries, null);
        }

        /// <summary>Serializes the record with the optional record-level
        /// "fact_key_history" section as well: experiment key → the
        /// subject-fact keys of this record's subject that no entry carries
        /// any more (a republish rotated them out, or a drop deleted the
        /// entry) while facts built under them may still be queued, spooled
        /// or owed. It sits at record level so it survives the entry's
        /// delete; an age refusal's withdrawal matches those facts through
        /// it after a relaunch. Keys and experiment keys are written sorted,
        /// so equal histories serialize byte-identically. Readers that
        /// predate the section ignore it.</summary>
        public static string SerializeRecord(
            string scope,
            Dictionary<string, ShardPilotExperimentEntry> entries,
            Dictionary<string, List<string>> factKeyHistory)
        {
            return SerializeRecord(scope, entries, factKeyHistory, null);
        }

        /// <summary>The record with the optional record-level
        /// "withdrawal_intents" section as well: the age withdrawals whose
        /// withdrawn facts' pending-purge marker is not durable yet (see
        /// <see cref="ShardPilotWithdrawalIntent"/>), each as
        /// <c>{"experiment_key", "fact_keys", "as_of_ticks"}</c>, sorted.
        /// Written only when there is one, so a record without an intent is
        /// byte-identical to what an earlier release writes. Readers that
        /// predate the section ignore it.</summary>
        public static string SerializeRecord(
            string scope,
            Dictionary<string, ShardPilotExperimentEntry> entries,
            Dictionary<string, List<string>> factKeyHistory,
            List<ShardPilotWithdrawalIntent> withdrawalIntents)
        {
            var builder = new StringBuilder(512);
            builder.Append("{\"scope\":");
            ShardPilotJson.AppendQuoted(builder, scope ?? string.Empty);
            builder.Append(",\"entries\":{");
            var first = true;
            var keys = new List<string>(entries.Keys);
            keys.Sort(StringComparer.Ordinal);
            for (var i = 0; i < keys.Count; i++)
            {
                var entry = entries[keys[i]];
                if (entry == null)
                {
                    continue;
                }

                if (!first)
                {
                    builder.Append(',');
                }

                first = false;
                ShardPilotJson.AppendQuoted(builder, keys[i]);
                builder.Append(':');
                AppendEntryObject(builder, entry);
            }

            builder.Append('}');
            AppendFactKeyHistory(builder, factKeyHistory);
            AppendWithdrawalIntents(builder, withdrawalIntents, scope);
            builder.Append('}');
            return builder.ToString();
        }

        private static void AppendWithdrawalIntents(
            StringBuilder builder, List<ShardPilotWithdrawalIntent> intents, string recordScope)
        {
            if (intents == null || intents.Count == 0)
            {
                return;
            }

            var sorted = new List<ShardPilotWithdrawalIntent>(intents);
            sorted.Sort((left, right) =>
            {
                var byScope = string.CompareOrdinal(left.Scope ?? recordScope, right.Scope ?? recordScope);
                if (byScope != 0) return byScope;
                var byKey = string.CompareOrdinal(left.ExperimentKey, right.ExperimentKey);
                if (byKey != 0) return byKey;
                var byTime = left.AsOfUtcTicks.CompareTo(right.AsOfUtcTicks);
                return byTime != 0 ? byTime : string.CompareOrdinal(left.Id, right.Id);
            });
            builder.Append(",\"withdrawal_intents\":[");
            for (var i = 0; i < sorted.Count; i++)
            {
                var keys = new List<string>(sorted[i].FactKeys ?? new List<string>());
                keys.Sort(StringComparer.Ordinal);
                builder.Append(i > 0 ? ",{\"experiment_key\":" : "{\"experiment_key\":");
                ShardPilotJson.AppendQuoted(builder, sorted[i].ExperimentKey ?? string.Empty);
                if (sorted[i].Scope != null && sorted[i].Scope != recordScope)
                {
                    builder.Append(",\"scope\":");
                    ShardPilotJson.AppendQuoted(builder, sorted[i].Scope);
                }
                builder.Append(",\"fact_keys\":[");
                for (var j = 0; j < keys.Count; j++)
                {
                    if (j > 0)
                    {
                        builder.Append(',');
                    }

                    ShardPilotJson.AppendQuoted(builder, keys[j]);
                }

                builder.Append("],\"as_of_ticks\":");
                builder.Append(sorted[i].AsOfUtcTicks.ToString(CultureInfo.InvariantCulture));
                if (sorted[i].Id != null)
                {
                    builder.Append(",\"withdrawal_id\":");
                    ShardPilotJson.AppendQuoted(builder, sorted[i].Id);
                }
                if (sorted[i].WholeKeyPopulation) builder.Append(",\"whole_key\":true");
                if (sorted[i].SparedEventIds != null && sorted[i].SparedEventIds.Count > 0)
                {
                    var spared = new List<string>(sorted[i].SparedEventIds);
                    spared.Sort(StringComparer.Ordinal);
                    builder.Append(",\"spared_event_ids\":[");
                    for (var j = 0; j < spared.Count; j++)
                    {
                        if (j > 0) builder.Append(',');
                        ShardPilotJson.AppendQuoted(builder, spared[j]);
                    }
                    builder.Append(']');
                }
                builder.Append('}');
            }

            builder.Append(']');
        }

        private static void AppendFactKeyHistory(StringBuilder builder, Dictionary<string, List<string>> history)
        {
            if (history == null || history.Count == 0)
            {
                return;
            }

            var experimentKeys = new List<string>(history.Keys);
            experimentKeys.Sort(StringComparer.Ordinal);
            var opened = false;
            for (var i = 0; i < experimentKeys.Count; i++)
            {
                var keys = history[experimentKeys[i]];
                if (string.IsNullOrEmpty(experimentKeys[i]) || keys == null || keys.Count == 0)
                {
                    continue;
                }

                var sorted = new List<string>(keys);
                sorted.Sort(StringComparer.Ordinal);
                builder.Append(opened ? "," : ",\"fact_key_history\":{");
                opened = true;
                ShardPilotJson.AppendQuoted(builder, experimentKeys[i]);
                builder.Append(":[");
                for (var j = 0; j < sorted.Count; j++)
                {
                    if (j > 0)
                    {
                        builder.Append(',');
                    }

                    ShardPilotJson.AppendQuoted(builder, sorted[j]);
                }

                builder.Append(']');
            }

            if (opened)
            {
                builder.Append('}');
            }
        }

        private static bool ReadServingPins(Dictionary<string, object> values, ShardPilotExperimentEntry entry)
        {
            object revision, kill, stamp;
            if (values.TryGetValue("served_revision", out revision) && revision is long && (long)revision > 0 &&
                values.TryGetValue("served_kill_gate", out kill) && kill is bool &&
                values.TryGetValue("served_at", out stamp) && stamp is string &&
                ShardPilotExperimentApply.TimeMatches((string)stamp, (string)stamp))
            {
                entry.ServedRevision = (long)revision;
                entry.ServedKillGate = (bool)kill;
                entry.ServedAt = (string)stamp;
                return true;
            }
            return false;
        }

        private static void AppendEntryObject(StringBuilder builder, ShardPilotExperimentEntry entry)
        {
            builder.Append("{\"assignment_key\":");
            ShardPilotJson.AppendQuoted(builder, entry.AssignmentKey ?? string.Empty);
            builder.Append(",\"variant_key\":");
            ShardPilotJson.AppendQuoted(builder, entry.VariantKey ?? string.Empty);
            builder.Append(",\"version\":");
            builder.Append(entry.Version.ToString(CultureInfo.InvariantCulture));
            builder.Append(",\"assignment_unit\":");
            ShardPilotJson.AppendQuoted(builder, entry.AssignmentUnit ?? string.Empty);
            if (!string.IsNullOrEmpty(entry.SubjectFactKey))
            {
                builder.Append(",\"subject_fact_key\":");
                ShardPilotJson.AppendQuoted(builder, entry.SubjectFactKey);
            }

            if (!string.IsNullOrEmpty(entry.SubjectKey))
            {
                builder.Append(",\"subject_key\":");
                ShardPilotJson.AppendQuoted(builder, entry.SubjectKey);
            }

            if (entry.ServedAt != null)
            {
                builder.Append(",\"served_revision\":").Append(entry.ServedRevision.ToString(CultureInfo.InvariantCulture));
                builder.Append(",\"served_kill_gate\":").Append(entry.ServedKillGate ? "true" : "false");
                builder.Append(",\"served_at\":");
                ShardPilotJson.AppendQuoted(builder, entry.ServedAt);
            }
            builder.Append(",\"fetched_at\":");
            ShardPilotJson.AppendQuoted(builder, ShardPilotJson.FormatTimestamp(entry.FetchedAtUtc));
            builder.Append(",\"fetched_at_ticks\":");
            builder.Append(entry.FetchedAtUtc.Ticks.ToString(CultureInfo.InvariantCulture));
            if (entry.VariantPayload != null)
            {
                builder.Append(",\"variant_payload\":");
                AppendValue(builder, entry.VariantPayload, 0);
            }

            if (entry.Attributes != null && entry.Attributes.Count > 0)
            {
                builder.Append(",\"attributes\":[");
                for (var a = 0; a < entry.Attributes.Count; a++)
                {
                    if (a > 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append("{\"name\":");
                    ShardPilotJson.AppendQuoted(builder, entry.Attributes[a].Key);
                    builder.Append(",\"value\":");
                    ShardPilotJson.AppendQuoted(builder, entry.Attributes[a].Value);
                    builder.Append('}');
                }

                builder.Append(']');
            }

            builder.Append('}');
        }

        private static void AppendValue(StringBuilder builder, object value, int depth)
        {
            if (value == null || depth >= 16)
            {
                builder.Append("null");
                return;
            }

            var text = value as string;
            if (text != null)
            {
                ShardPilotJson.AppendQuoted(builder, text);
                return;
            }

            if (value is bool)
            {
                builder.Append((bool)value ? "true" : "false");
                return;
            }

            if (value is long)
            {
                builder.Append(((long)value).ToString(CultureInfo.InvariantCulture));
                return;
            }

            if (value is int)
            {
                builder.Append(((int)value).ToString(CultureInfo.InvariantCulture));
                return;
            }

            if (value is double)
            {
                var d = (double)value;
                if (double.IsNaN(d) || double.IsInfinity(d))
                {
                    builder.Append("null");
                    return;
                }

                builder.Append(d.ToString("R", CultureInfo.InvariantCulture));
                return;
            }

            var map = value as Dictionary<string, object>;
            if (map != null)
            {
                builder.Append('{');
                var first = true;
                foreach (var pair in map)
                {
                    if (!first)
                    {
                        builder.Append(',');
                    }

                    first = false;
                    ShardPilotJson.AppendQuoted(builder, pair.Key);
                    builder.Append(':');
                    AppendValue(builder, pair.Value, depth + 1);
                }

                builder.Append('}');
                return;
            }

            var list = value as List<object>;
            if (list != null)
            {
                builder.Append('[');
                for (var i = 0; i < list.Count; i++)
                {
                    if (i > 0)
                    {
                        builder.Append(',');
                    }

                    AppendValue(builder, list[i], depth + 1);
                }

                builder.Append(']');
                return;
            }

            builder.Append("null");
        }

        /// <summary>
        /// Parses a durable record; corrupt = miss. Keeps only entries that
        /// are complete, well-formed assignment records, copied down to the
        /// known fields — anything else (a corrupt file, a truncated entry,
        /// a garbled field) is dropped rather than served or crashed on.
        /// </summary>
        public static bool TryParseRecord(
            string text,
            out string scope,
            out Dictionary<string, ShardPilotExperimentEntry> entries)
        {
            Dictionary<string, List<string>> ignored;
            return TryParseRecord(text, out scope, out entries, out ignored);
        }

        /// <summary>The full parse, with the record-level
        /// "fact_key_history" section (see
        /// <see cref="SerializeRecord(string, Dictionary{string, ShardPilotExperimentEntry}, Dictionary{string, List{string}})"/>).
        /// A record without the section — every record an earlier release
        /// wrote — parses with an empty history. The section is parsed per
        /// ITEM: a key that is not sfk1_-shaped, or
        /// a member that is not a list, is skipped, and never costs the
        /// record or any entry.</summary>
        public static bool TryParseRecord(
            string text,
            out string scope,
            out Dictionary<string, ShardPilotExperimentEntry> entries,
            out Dictionary<string, List<string>> factKeyHistory)
        {
            List<ShardPilotWithdrawalIntent> ignored;
            return TryParseRecord(text, out scope, out entries, out factKeyHistory, out ignored);
        }

        /// <summary>The full parse with the "withdrawal_intents" section
        /// too. Parsed per item: an intent without an experiment key, a
        /// positive tick count or a valid fact key is skipped, and never
        /// costs the record. A record without the section parses with
        /// none.</summary>
        public static bool TryParseRecord(
            string text,
            out string scope,
            out Dictionary<string, ShardPilotExperimentEntry> entries,
            out Dictionary<string, List<string>> factKeyHistory,
            out List<ShardPilotWithdrawalIntent> withdrawalIntents)
        {
            withdrawalIntents = new List<ShardPilotWithdrawalIntent>();
            factKeyHistory = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            scope = null;
            entries = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            Dictionary<string, object> decoded;
            if (!ShardPilotJsonParser.TryParseObject(text, out decoded))
            {
                return false;
            }

            object scopeValue;
            scope = decoded.TryGetValue("scope", out scopeValue) ? scopeValue as string : null;
            if (string.IsNullOrEmpty(scope))
            {
                return false;
            }

            object entriesValue;
            var entriesMap = decoded.TryGetValue("entries", out entriesValue)
                ? entriesValue as Dictionary<string, object>
                : null;
            if (entriesMap == null)
            {
                return false;
            }

            foreach (var pair in entriesMap)
            {
                var entry = ParseEntry(pair.Value as Dictionary<string, object>);
                if (!string.IsNullOrEmpty(pair.Key) && entry != null)
                {
                    entries[pair.Key] = entry;
                }
            }

            object historyValue;
            var historyMap = decoded.TryGetValue("fact_key_history", out historyValue)
                ? historyValue as Dictionary<string, object>
                : null;
            if (historyMap != null)
            {
                foreach (var pair in historyMap)
                {
                    var items = pair.Value as List<object>;
                    if (string.IsNullOrEmpty(pair.Key) || items == null)
                    {
                        continue;
                    }

                    List<string> keys = null;
                    for (var i = 0; i < items.Count; i++)
                    {
                        var factKey = items[i] as string;
                        if (!ValidSubjectFactKey(factKey) || (keys != null && keys.Contains(factKey)))
                        {
                            continue;
                        }

                        if (keys == null)
                        {
                            keys = new List<string>();
                        }

                        keys.Add(factKey);
                    }

                    if (keys != null)
                    {
                        factKeyHistory[pair.Key] = keys;
                    }
                }
            }

            object intentsValue;
            var intentItems = decoded.TryGetValue("withdrawal_intents", out intentsValue)
                ? intentsValue as List<object>
                : null;
            if (intentItems != null)
            {
                for (var i = 0; i < intentItems.Count; i++)
                {
                    var map = intentItems[i] as Dictionary<string, object>;
                    object value;
                    var experimentKey = map != null && map.TryGetValue("experiment_key", out value)
                        ? value as string
                        : null;
                    var ticks = map != null && map.TryGetValue("as_of_ticks", out value) && value is long
                        ? (long)value
                        : 0L;
                    var factKeyItems = map != null && map.TryGetValue("fact_keys", out value)
                        ? value as List<object>
                        : null;
                    if (string.IsNullOrEmpty(experimentKey) || ticks <= 0 || factKeyItems == null)
                    {
                        continue;
                    }

                    var factKeys = new List<string>();
                    for (var j = 0; j < factKeyItems.Count; j++)
                    {
                        var factKey = factKeyItems[j] as string;
                        if (ValidSubjectFactKey(factKey) && !factKeys.Contains(factKey))
                        {
                            factKeys.Add(factKey);
                        }
                    }

                    if (factKeys.Count > 0)
                    {
                        var intentScope = scope;
                        if (map.TryGetValue("scope", out value))
                        {
                            intentScope = value as string;
                            if (string.IsNullOrEmpty(intentScope)) continue;
                        }
                        var id = map.TryGetValue("withdrawal_id", out value) ? value as string : null;
                        var wholeKey = map.TryGetValue("whole_key", out value) && value is bool && (bool)value;
                        var spared = new List<string>();
                        var sparedItems = map.TryGetValue("spared_event_ids", out value) ? value as List<object> : null;
                        if (sparedItems != null)
                            for (var j = 0; j < sparedItems.Count; j++)
                            {
                                var eventId = sparedItems[j] as string;
                                if (!string.IsNullOrEmpty(eventId) && !spared.Contains(eventId)) spared.Add(eventId);
                            }
                        withdrawalIntents.Add(new ShardPilotWithdrawalIntent
                        {
                            Id = id,
                            WholeKeyPopulation = wholeKey,
                            SparedEventIds = spared,
                            Scope = intentScope,
                            ExperimentKey = experimentKey,
                            FactKeys = factKeys,
                            AsOfUtcTicks = ticks
                        });
                    }
                }
            }

            return true;
        }

        private static ShardPilotExperimentEntry ParseEntry(Dictionary<string, object> map)
        {
            if (map == null)
            {
                return null;
            }

            object value;
            var assignmentKey = map.TryGetValue("assignment_key", out value) ? value as string : null;
            var variantKey = map.TryGetValue("variant_key", out value) ? value as string : null;
            var unit = map.TryGetValue("assignment_unit", out value) ? value as string : null;
            long version;
            if (string.IsNullOrEmpty(assignmentKey) || string.IsNullOrEmpty(variantKey) ||
                string.IsNullOrEmpty(unit) || !TryReadIntegralVersion(map, out version) ||
                version <= 0)
            {
                // Version sanity is part of the restore sanitizer: the SDK
                // never wrote a non-positive (or fractional) version, so
                // one in the record means corruption — the entry is a miss
                // (clean re-fetch), exactly like a malformed fact key.
                return null;
            }

            if (!KnownAssignmentUnits.Contains(unit))
            {
                return null;
            }

            var entry = new ShardPilotExperimentEntry
            {
                AssignmentKey = assignmentKey,
                VariantKey = variantKey,
                Version = version,
                AssignmentUnit = unit
            };
            ReadServingPins(map, entry);
            if (map.TryGetValue("subject_fact_key", out value))
            {
                // This record was written by the SDK itself, so a fact key that
                // is not sfk1_-shaped means corruption — the whole entry is
                // a miss (clean re-fetch), and an unsafe subject can never
                // be emitted.
                var factKey = value as string;
                if (!ValidSubjectFactKey(factKey))
                {
                    return null;
                }

                entry.SubjectFactKey = factKey;
            }

            if (map.TryGetValue("subject_key", out value))
            {
                entry.SubjectKey = value as string;
            }

            if (map.TryGetValue("variant_payload", out value))
            {
                var payload = value as Dictionary<string, object>;
                if (payload == null)
                {
                    return null;
                }

                entry.VariantPayload = payload;
            }

            if (map.TryGetValue("fetched_at_ticks", out value) && value is long)
            {
                var ticks = (long)value;
                if (ticks >= DateTime.MaxValue.Ticks)
                {
                    // Timestamp sanity is part of the restore sanitizer too:
                    // the SDK stamps records from the wall clock, so a
                    // MaxValue (or beyond) stamp means corruption — and the
                    // install/condemnation freshness raises put AddTicks(1)
                    // above restored stamps, so a MaxValue stamp would throw
                    // inside the assignment callback/worker on the next
                    // successful fetch. Corrupt record, dropped entry (a
                    // clean re-fetch), per the sanitizer's existing rules.
                    return null;
                }

                if (ticks >= DateTime.MinValue.Ticks)
                {
                    entry.FetchedAtUtc = new DateTime(ticks, DateTimeKind.Utc);
                }
            }

            if (map.TryGetValue("attributes", out value))
            {
                var list = value as List<object>;
                if (list != null)
                {
                    // Restored attribute sets go BACK through the fetch-time
                    // normalizer (vocabulary, value bounds, count cap): a
                    // corrupted or hand-edited record degrades to a safe
                    // subset instead of reshaping revalidation requests with
                    // invented names or oversized values.
                    var restored = new Dictionary<string, string>(StringComparer.Ordinal);
                    for (var i = 0; i < list.Count; i++)
                    {
                        var pairMap = list[i] as Dictionary<string, object>;
                        object nameValue = null;
                        object valueValue = null;
                        var name = pairMap != null && pairMap.TryGetValue("name", out nameValue)
                            ? nameValue as string
                            : null;
                        var attrValue = pairMap != null && pairMap.TryGetValue("value", out valueValue)
                            ? valueValue as string
                            : null;
                        if (!string.IsNullOrEmpty(name) && attrValue != null)
                        {
                            restored[name] = attrValue;
                        }
                    }

                    int dropped;
                    var normalized = NormalizeExperimentAttributes(restored, out dropped);
                    if (normalized.Count > 0)
                    {
                        entry.Attributes = normalized;
                    }
                }
            }

            return entry;
        }
    }

    /// <summary>One experiment fact RESTORED from the analytics spool at
    /// relaunch, described for the condemnation plane: its event id plus
    /// the assignment coordinates its envelope props carried
    /// (<c>experiment_key</c>, <c>experiment_version</c>,
    /// <c>assignment_key</c>). The coordinates let the registration recover
    /// the fact's TRUE freshness from the matching serving entry — a fact
    /// a post-condemnation fresh install emitted must not purge as
    /// pre-sentinel just because it spooled across a relaunch. Null/absent
    /// coordinates register at the conservative floor.</summary>
    internal sealed class ShardPilotRestoredFact
    {
        public string EventId;
        public string ExperimentKey;
        public long? Version;
        public string AssignmentKey;

        /// <summary>The fact's own event time (its envelope's
        /// <c>event_ts</c>), when known.</summary>
        public DateTime? TimestampUtc;
    }

    /// <summary>A durable age-withdrawal intent: an age withdrawal of
    /// <see cref="ExperimentKey"/> for the subject named by
    /// <see cref="FactKeys"/>, resolved at <see cref="AsOfUtcTicks"/>. It is
    /// written with (or before) the withdrawal's durable entry delete and
    /// kept until every located or still-emitting withdrawn fact has a
    /// durable purge, so a
    /// process death in between leaves the next launch something to
    /// withdraw the restored copies by. New intents cover the known keys'
    /// whole population, sparing later applications by their durably recorded
    /// event IDs. Older records retain their timestamp boundary.</summary>
    internal sealed class ShardPilotWithdrawalIntent
    {
        public string Id;
        public bool WholeKeyPopulation;
        public List<string> SparedEventIds;
        // The withdrawal's owner, independently of the assignment-map header.
        // Records omitting this field use their enclosing record scope.
        public string Scope;
        public string ExperimentKey;
        public List<string> FactKeys;
        public long AsOfUtcTicks;

        /// <summary>Whether a restored fact is one this intent
        /// withdrew.</summary>
        public bool Covers(ShardPilotRestoredFact fact)
        {
            return fact != null && FactKeys != null &&
                   string.Equals(fact.ExperimentKey, ExperimentKey, StringComparison.Ordinal) &&
                   fact.AssignmentKey != null && FactKeys.Contains(fact.AssignmentKey) &&
                   (SparedEventIds == null || !SparedEventIds.Contains(fact.EventId)) &&
                   (WholeKeyPopulation || (fact.TimestampUtc.HasValue && fact.TimestampUtc.Value.Ticks <= AsOfUtcTicks));
        }
    }

    /// <summary>
    /// The experiment-assignment consumer: fetches the
    /// server-evaluated assignment for one (app, environment, experiment,
    /// subject) tuple, serves the assigned variant to game code with a
    /// durable last-known-good cache, revalidates cached assignments on the
    /// 300s±10% cadence (the SDK-side kill-switch reach), and emits
    /// exposure/outcome facts through the normal analytics pipeline.
    ///
    /// DARK BY DEFAULT: constructed only when
    /// <see cref="ShardPilotConfig.ExperimentsEnabled"/> is true (default
    /// false); while off, zero experiment code paths execute — no subject-id
    /// mint, no assignment fetch, no revalidation, no exposure emit, no new
    /// persistence keys. The server side is equally dark: while the platform
    /// flags are off the endpoint answers 403, which this client treats
    /// fail-closed exactly like bad auth.
    ///
    /// Consent posture (assignment plane): fetches are GRANTED-ONLY — while
    /// analytics consent is unknown or denied (either denial flavor,
    /// including the age-gate-forced one) no assignment request is made, no
    /// subject id is minted, nothing is served (getters return null) and the
    /// revalidation cadence does not run, so a forced-minor session produces
    /// ZERO experiment traffic on both planes. This is deliberately stricter
    /// than remote-config delivery (available without identifiers when
    /// consent is not granted): an
    /// assignment whose exposure can structurally never land is dead weight,
    /// and serving it would corrupt experiment population integrity. A
    /// consent downgrade mid-session stops fetching and serving; the durable
    /// cache record is retained but not served, and a later re-grant serves
    /// it again.
    ///
    /// Explicit applications use CP exposure/outcome apply endpoints. Captured
    /// occurrences retry in a shared FIFO; only bound sealed facts enter analytics.
    /// Fetches, cache restores and session renewals never create applications.
    ///
    /// Threading: host calls, the analytics worker's tick, and the sender's
    /// main-thread completions may all touch this instance concurrently.
    /// One instance lock guards all mutable state; the client-facing
    /// delegates are never invoked while it is held (the subject-id and
    /// consent reads are lock-free field reads by contract), sweeps are
    /// serialized by a dedicated sweep lock, and the fetch fence plus the
    /// sentinel condemnation registry are process-wide statics keyed by the
    /// cache path — sibling instances left behind by a facade
    /// re-initialization share the durable record, so their installs must be
    /// ordered against each other and a sentinel drop seen by one instance
    /// must stop the others' serving too.
    /// </summary>
    internal sealed partial class ShardPilotExperiments
    {
        /// <summary>Withdraws already-ACCEPTED experiment facts from the
        /// analytics pipeline by event id: the real-subjects sentinel
        /// condemns a scope, and facts carrying its withdrawn subject-fact
        /// keys that are still queue-resident (or held in the worker's
        /// retained batch behind backoff) must not ship on the next flush.
        /// Invoked OUTSIDE the consumer's lock. Returns true when every
        /// withdrawn id is durably owed (its pending-purge marker landed) or
        /// nothing needed a marker.</summary>
        internal delegate bool ShardPilotExperimentFactPurger(List<string> eventIds);

        /// <summary>Withdraws already-ACCEPTED facts of ONE experiment from
        /// the analytics pipeline (the live queue, the restored backlog, the
        /// worker's retained batch, the offline spool): an
        /// <c>age_ineligible</c> refusal withdraws the subject's owed
        /// applications. A fact matches on its event name (exposure or
        /// outcome), its <c>experiment_key</c> prop, and an
        /// <c>assignment_key</c> prop in <paramref name="subjectFactKeys"/>
        /// (an empty set matches nothing). A fact
        /// <paramref name="emittedAfterWithdrawal"/> reports is a later
        /// application's and is spared. Invoked OUTSIDE the consumer's
        /// lock. Returns true when the withdrawn facts' pending-purge marker
        /// is durable, or nothing needed one.</summary>
        internal delegate bool ShardPilotExperimentKeyFactWithdrawer(
            string experimentKey, IList<string> subjectFactKeys, Func<string, bool> emittedAfterWithdrawal,
            Action<IList<string>> selectedForPurge);

        private sealed class OwedExposure
        {
            public ShardPilotExperimentEntry Entry;
            public string Session;

            /// <summary>The analytics session id captured at ARM time — the
            /// envelope identity the fact enqueues with, whatever session is
            /// current when it finally drains.</summary>
            public string SessionId;

            /// <summary>The anonymous id captured at ARM time: the actor the
            /// treatment actually ran for. An identity rotation between the
            /// application and the drain must not re-attribute the fact —
            /// erasure reachability follows the actor who was treated. The
            /// first-real-session migration deliberately keeps it: adopting
            /// a session does not change who was treated.</summary>
            public string AnonymousId;

            public ShardPilotExperimentApplication Application;
            public ShardPilotSealedExperimentFact Sealed;
            public long ReadGeneration;
            public long CaptureOrder;
            public bool InFlight;
            public int Attempts;
            public DateTime NotBefore;
        }

        private sealed class DurablePendingSync
        {
            public DateTime AsOfUtc;
            public bool Drop;
            // A fresh retired-scope refusal must first observe the record
            // it resolves. Unreadable storage postpones that decision;
            // after observation, retries use the fixed decision stamp.
            public bool FreshDrop;
            public PendingWithdrawalIntent Withdrawal;

            /// <summary>The cache scope the intent was DECIDED under. A
            /// subject re-mint retires the scope: owed WRITES are cancelled
            /// (their source entries are gone), while owed DROPS still land
            /// against the retired scope's record — via a retry that never
            /// consults current memory, which describes another subject.</summary>
            public string Scope;

            /// <summary>The experiment the intent converges. The map is
            /// keyed by the (scope, experiment) COMPOSITE — a retired
            /// scope's owed drop and a new scope's intent for the same
            /// experiment are different obligations and must coexist, never
            /// overwrite each other.</summary>
            public string ExperimentKey;
        }

        /// <summary>Each experiment's owed exposures are a small FIFO of
        /// applied-entry snapshots (bounded; overflowing drops the OLDEST
        /// with a diagnostic), so a new application displacing the slot
        /// cannot silently lose the previous application's still-unemitted
        /// fact.</summary>
        private const int MaxOwedExposures = 8;

        private readonly ShardPilotConfig _config;
        private readonly string _cachePath;

        /// <summary>Conservative condemnation token. Constructive fences,
        /// emissions, progress and cache mint gates instead use the journal
        /// owner's confirmed namespace, bound before their first use.</summary>
        private readonly string _cachePathKeyToken;
        private string _cachePathIdentityToken;
        private readonly string _subjectStoreIdentity;
        private readonly object _identityGate = new object();
        // The static fences and mint gates outlive clients; retain their
        // confirmed names for the same lifetime, without retaining files.
        private static readonly Dictionary<string, ShardPilotWithdrawalJournalOwner.IdentityReference> CacheIdentities =
            new Dictionary<string, ShardPilotWithdrawalJournalOwner.IdentityReference>(StringComparer.Ordinal);

        internal static string ConfirmCacheIdentity(string path, string store)
        {
            var claim = ShardPilotWithdrawalWork.ClaimIdentity(path, store);
            if (claim == null) return null;
            var key = claim.Key;
            try
            {
                lock (StaticGate)
                    if (!CacheIdentities.ContainsKey(key)) { CacheIdentities.Add(key, claim); claim = null; }
                return key;
            }
            finally { claim?.Dispose(); }
        }

        internal bool BindCacheIdentity(bool setup = false)
        {
            if (string.IsNullOrEmpty(_cachePath)) return true;
            lock (_identityGate)
            {
                if (_cachePathIdentityToken != null) return true;
                if (setup && _config.ExperimentsEnabled)
                {
                    lock (_egressGate)
                    {
                        bool live; lock (_lock) live = !_tornDown && !_closingApplications && !_automaticLaneDisowned;
                        if (live && _callbackHost != null && _callbackHost.IsLive && _hostCurrent() &&
                            _consent() == ShardPilotConsentState.Granted)
                            lock (StorageGate)
                                try { Directory.CreateDirectory(Path.GetDirectoryName(_cachePath)); } catch { }
                    }
                }
                var key = ConfirmCacheIdentity(_cachePath, ShardPilotWithdrawalWork.StoreFor(_config));
                if (key == null) return false;
                lock (_lock)
                {
                    _cachePathIdentityToken = key;
                    _mintGate = MintGateFor(_subjectStoreIdentity ?? key);
                }
                return true;
            }
        }

        private readonly IShardPilotHttpRequestSender _sender;
        private readonly Func<string> _subjectId;
        private readonly Func<string, bool> _storeSubjectId;

        /// <summary>Re-reads the PERSISTED subject id (store access allowed —
        /// never called under the instance lock). Consulted at
        /// mint-decision time so overlapping clients converge: a sibling
        /// that minted and persisted after this instance's construction is
        /// adopted instead of being overwritten by a second mint.</summary>
        private readonly Func<string> _reloadSubjectId;

        private readonly Func<ShardPilotConsentState> _consent;

        /// <summary>The client's consent-denial epoch, read LOCK-FREE (the
        /// consumer may consult it under its own lock, so it must never
        /// take the client's gate). Every denial — chosen, forced-minor,
        /// or globally applied — advances it, so a dispatch-time capture
        /// compared later detects a deny that a re-grant has since papered
        /// over: current-state reads alone cannot.</summary>
        private readonly Func<long> _consentEpoch;

        /// <summary>Serializes the FINAL pre-send consent-epoch re-read and
        /// the transport handoff against consent decisions: the client
        /// advances its denial epoch INSIDE this same gate, so once any
        /// denial's advance is observable, zero assignment requests can
        /// start — the granted-only contract holds at the wire, not just at
        /// the settle. Held only across that tiny lock-free re-read and the
        /// non-blocking transport start; a synchronous test sender's
        /// callback is DEFERRED out of the hold (see
        /// <see cref="EgressDispatch"/>), so no other lock is ever taken
        /// inside it. The client's ordering is gate → egress gate; this
        /// consumer never holds another lock when taking it.</summary>
        private readonly object _egressGate;
        private readonly Func<string> _analyticsSessionId;
        private readonly Func<string> _anonymousId;
        private readonly Func<bool> _admitCurrentSession;
        private readonly ShardPilotExperimentFactPurger _purgeFacts;
        private readonly ShardPilotExperimentKeyFactWithdrawer _withdrawKeyFacts;
        private readonly Action<string> _diagnose;
        private readonly string _credentialFingerprint;

        private readonly object _lock = new object();
        private readonly object _sweepLock = new object();

        /// <summary>Counts <c>age_ineligible</c> withdrawals. A fact
        /// emission captures it when it reads its source (the serving entry
        /// or the owed snapshot) and re-checks it around the unlocked
        /// enqueue: a withdrawal of the same application that lands in that
        /// window refuses the emit, or withdraws the enqueued fact by id,
        /// instead of letting it outrun the pipeline scan. Guarded by
        /// <see cref="_lock"/>.</summary>
        private long _withdrawalGeneration;

        /// <summary>Per experiment key, the generation of the latest
        /// <c>age_ineligible</c> withdrawal that named each subject-fact key
        /// (see <see cref="_withdrawalGeneration"/>). Kept per KEY: a second
        /// withdrawal naming other keys (a rotated version's, or another
        /// subject's after a re-mint) must not hide the first from a fact
        /// built under the first key whose emission is still in flight. An
        /// entry goes with the fact-key history's prune, once no live fact
        /// and no emission in progress carries its key (see
        /// <see cref="PruneFactKeyState"/>). Guarded by
        /// <see cref="_lock"/>.</summary>
        private readonly Dictionary<string, Dictionary<string, long>> _withdrawnKeyGenerations =
            new Dictionary<string, Dictionary<string, long>>(StringComparer.Ordinal);

        private sealed class WithdrawnApplications
        {
            public long Generation;

            /// <summary>The refused subject's fact keys. The pipeline
            /// withdrawal matches facts by these alone, so an empty set (no
            /// key known at all) matches no queued, spooled or held fact:
            /// facts name their subject only by this key. The owed
            /// applications are matched by the refused scope, which carries
            /// the subject, instead.</summary>
            public List<string> SubjectFactKeys;

            /// <summary>The durable intent this withdrawal recorded, retired
            /// once its pipeline withdrawal's pending-purge marker is
            /// durable.</summary>
            public PendingWithdrawalIntent Intent;
        }

        /// <summary>A withdrawal intent held in memory: the durable form,
        /// the record scope it is written under, and whether a write of that
        /// record has carried it.</summary>
        private sealed class PendingWithdrawalIntent
        {
            public string Scope;
            public ShardPilotWithdrawalIntent Intent;
            private WithdrawalFrame _frame;
            public WithdrawalFrame Frame
            {
                get { return State?.Shared?.Current(_frame) ?? _frame; }
                set { _frame = value; State?.Shared?.Remember(value); }
            }
            public int Busy;
            public bool Durable;
            public bool Retired;
            public WithdrawalState State;
            public bool ScanPending;

            /// <summary>Read from the record at construction: a previous
            /// process's withdrawal whose marker never became durable. It is
            /// applied to the restored spool and retired then.</summary>
            public bool Adopted;
        }

        /// <summary>The local age-withdrawal intents not yet retired. Every
        /// cache write merges these with all outstanding scopes on disk;
        /// retirement removes only the completed identities. Guarded by
        /// <see cref="_lock"/>.</summary>
        private readonly List<PendingWithdrawalIntent> _withdrawalIntents = new List<PendingWithdrawalIntent>();

        // A successful merged write removes these identities from disk.
        // Pending objects also retain their retired state across debt handoff.
        private readonly HashSet<string> _retiredWithdrawalIntents = new HashSet<string>(StringComparer.Ordinal);

        private sealed class WithdrawalState
        {
            public string Cache, Scope, Identity, Experiment;
            public ShardPilotWithdrawalWork.SharedState Shared;
            private WithdrawalFrame _frame;
            public WithdrawalFrame Frame
            {
                get { return Shared?.Current(_frame) ?? _frame; }
                set { _frame = value; Shared?.Remember(value); }
            }
            public List<string> FactKeys;
            public long Order;
            public long AsOfUtcTicks;
            public int Scans;
            public bool PopulationKnown, Retired, WholeKeyPopulation;
            public readonly HashSet<string> Spared = new HashSet<string>(StringComparer.Ordinal);
            public readonly HashSet<string> Unconfirmed = new HashSet<string>(StringComparer.Ordinal);
        }

        private sealed class FactEmission
        {
            public string Cache, Scope, Experiment, EventId;
            public List<string> FactKeys;
            public long Order;
        }

        // These hold process-local liveness, never wire data. A replacement
        // reader must observe an older owner's still-running emission.
        private static readonly Dictionary<string, WithdrawalState> WithdrawalStates =
            new Dictionary<string, WithdrawalState>(StringComparer.Ordinal);
        private static readonly HashSet<FactEmission> FactEmissions = new HashSet<FactEmission>();
        private static readonly Dictionary<string, FactEmission> UnconfirmedEmissions =
            new Dictionary<string, FactEmission>(StringComparer.Ordinal);
        private static long _applicationOrder;
        private string _withdrawalRegistryPath;
        private bool _withdrawalMetadataUncertain;

        /// <summary>Set when the restored spool's facts an adopted intent
        /// covers could not be durably withdrawn: the adopted intents stay
        /// for the next launch. Guarded by <see cref="_lock"/>.</summary>
        private bool _adoptedIntentsOwed;

        /// <summary>The fact-key history in memory: cache scope (which
        /// carries the subject) → experiment key → subject-fact keys that no
        /// serving entry carries any more while facts built under them may
        /// still be live. A key enters when it leaves the record's entries
        /// (<see cref="RetireFactKeyLocked"/>), rides every durable record
        /// write as the record-level "fact_key_history" section, joins
        /// every age withdrawal of its experiment, and is pruned once no
        /// live fact references it (<see cref="PruneFactKeyState"/>).
        /// Guarded by <see cref="_lock"/>.</summary>
        private readonly Dictionary<string, Dictionary<string, List<string>>> _factKeyHistory =
            new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.Ordinal);

        /// <summary>Scopes whose history a prune shrank since their record
        /// last landed: the next write of such a record writes the in-memory
        /// history as it is, instead of merging in the section on disk,
        /// which still holds the pruned keys. Guarded by
        /// <see cref="_lock"/>.</summary>
        private readonly HashSet<string> _factKeyHistoryPruned = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>Per experiment key, the scope and fact key of the
        /// serving entry as of its last durable sync, seeded at construction
        /// from the restored record: the in-memory half of the retirement,
        /// which also covers a write that has no previous record to diff (a
        /// consumer with no durable record, or a record that cannot be read
        /// back). Guarded by <see cref="_lock"/>.</summary>
        private readonly Dictionary<string, KeyValuePair<string, string>> _syncedFactKeys =
            new Dictionary<string, KeyValuePair<string, string>>(StringComparer.Ordinal);

        /// <summary>Facts between their source read and the end of their
        /// emission, as "experiment key U+001F fact key" → count. Such a
        /// fact may be in no pipeline holder yet (its owed snapshot can
        /// even be gone), so the prune counts it as live: neither its key's
        /// history nor its key's withdrawal generation may go while it can
        /// still enqueue. Guarded by <see cref="_lock"/>.</summary>
        private readonly Dictionary<string, int> _factKeysInEmission =
            new Dictionary<string, int>(StringComparer.Ordinal);

        /// <summary>False until the client has restored its spool: before
        /// that, the restored facts are in no holder yet, and a prune would
        /// forget the keys they carry. Guarded by <see cref="_lock"/>.</summary>
        private bool _factKeyPruneArmed;

        /// <summary>The host's non-adult age declaration per experiment key,
        /// from the fetch call that made it until that fetch ends. While it
        /// stands an age-gated (client-id) assignment of the experiment is not
        /// served (see <see cref="LoadServableEntryLocked"/>), its owed
        /// exposures are not swept, and no revalidation re-sends the held
        /// entry's earlier declaration; when the fetch ends without the
        /// server's answer applied, the declaration withdraws the experiment
        /// (<see cref="EndNonAdultDeclaration"/>). A later host declaration
        /// replaces it. Guarded by <see cref="_lock"/>.</summary>
        private readonly Dictionary<string, NonAdultDeclaration> _declaring =
            new Dictionary<string, NonAdultDeclaration>(StringComparer.Ordinal);

        // Persisted targeting is history, not a host decision for this launch.
        private readonly HashSet<string> _launchUndecided = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<string> _launchDecided = new HashSet<string>(StringComparer.Ordinal);

        internal bool MayPublishRestoredFact(string experimentKey)
        {
            lock (_lock)
            {
                return experimentKey != null && _launchDecided.Contains(experimentKey);
            }
        }

        private sealed class NonAdultDeclaration
        {
            /// <summary>A fetch sequence reserved at the declaration: every
            /// fetch dispatched before it carries a lower one, the declaring
            /// fetch a higher one.</summary>
            public long FenceSeq;
            public string Scope;

            /// <summary>The declaring fetch's own sequence once dispatched (the
            /// grammar re-mint retry's, when it rides one).</summary>
            public long Seq;

            /// <summary>The server's answer to the declaring fetch was applied
            /// to the assignment state; that answer governs.</summary>
            public bool Decided;

            /// <summary>The call's original known fact population. A remint
            /// clears serving state but cannot erase the declaration's reach
            /// over facts already accepted for that original subject.</summary>
            public readonly List<string> FactKeys = new List<string>();
        }

        private readonly Dictionary<string, ShardPilotExperimentEntry> _entries =
            new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);

        /// <summary>Applications whose exposure fact is still OWED:
        /// experiment key → a bounded FIFO of applied-entry snapshots, each
        /// carrying THE SESSION THE APPLICATION BELONGS TO. The snapshot —
        /// not the serving cache — is the emission source, so an owed fact
        /// for a variant that really ran survives a later drop of the live
        /// entry, an ordinary auth latch, and a subject re-mint (exposure
        /// facts are facts about the PAST; only the real-subjects sentinel —
        /// whose fact keys must not outlive it — discards them).</summary>
        private readonly Dictionary<string, List<OwedExposure>> _pendingExposure =
            new Dictionary<string, List<OwedExposure>>(StringComparer.Ordinal);

        /// <summary>Durable-write convergence: (scope, experiment) composite
        /// key → an OWED sync (memory changed, disk did not), retried every
        /// tick; the recorded intent lets an ordinary fail-closed latch
        /// cancel owed WRITES (the 401/403 canon retains the durable record)
        /// while authoritative drops still land. The composite key keeps a
        /// retired scope's owed drop alive next to a new scope's intent for
        /// the same experiment.</summary>
        private readonly Dictionary<string, DurablePendingSync> _durablePending =
            new Dictionary<string, DurablePendingSync>(StringComparer.Ordinal);


        private DateTime? _durableClearPendingAsOfUtc;

        /// <summary>The scope the owed whole-record clear was decided under
        /// (the sentinel's scope) — its retries and demotions read THAT
        /// scope's record, current subject notwithstanding.</summary>
        private string _durableClearPendingScope;

        /// <summary>Covers the MAXIMUM the analytics pipeline can hold —
        /// the queue (default buffer 1000), the spool backlog (2000), the
        /// worker's held batch, and slack — so an id can never be evicted
        /// while its fact is still pipeline-resident and withdrawable. Ids
        /// confirmed OUT of the pipeline (delivered) are evicted eagerly.</summary>
        /// <summary>FLOOR for the condemnation-plane index bound; the
        /// per-instance bound derives from configured capacity in the
        /// constructor.</summary>
        private const int MinTrackedFactScopes = 4096;

        /// <summary>The condemnation index bound: the configured MAXIMUM
        /// pipeline residency (event queue + restorable spool + held
        /// batch) plus re-arm slack, floored at the old fixed bound. A
        /// fact id must stay indexed for as long as its fact can possibly
        /// be pipeline-resident — a fixed bound under an enlarged
        /// BufferSize/SpoolMaxEvents would FIFO-evict ids whose facts are
        /// still queued, and a later sentinel could not withdraw
        /// them.</summary>
        private readonly int _maxTrackedFactScopes;

        /// <summary>Emitted-fact index for the CONDEMNATION plane: event id
        /// → cache scope, bounded FIFO. This index
        /// this survives session renewals and registers prior-session drains
        /// too — a queue-resident fact from any session must be locatable
        /// when its scope's subject-fact keys are withdrawn by the
        /// real-subjects sentinel.</summary>
        private readonly Dictionary<string, EmittedFactScope> _factScopes =
            new Dictionary<string, EmittedFactScope>(StringComparer.Ordinal);

        /// <summary>The bounded index's FIFO order, as (event id,
        /// GENERATION) pairs: a withdrawal (or a delivery settle) removes
        /// the MAP entry but cannot remove the middle of this queue, and a
        /// same-session re-emit of the same tuple REUSES the deterministic
        /// event id — re-enqueueing it with a fresh generation. The
        /// cap-eviction dequeue therefore removes the map entry ONLY when
        /// the dequeued generation matches the mapping's own; a stale
        /// order entry (its mapping withdrawn or superseded by a re-track)
        /// just drops, so wrapping the index can never evict a FRESH
        /// mapping through a stale head — a still-queued fact carrying a
        /// withdrawn subject-fact key stays locatable for the
        /// sentinel.</summary>
        private readonly Queue<KeyValuePair<string, long>> _factScopeOrder =
            new Queue<KeyValuePair<string, long>>();

        /// <summary>Monotonic generation for <see cref="_factScopes"/>
        /// entries (guarded by <c>_lock</c> like the index itself).</summary>
        private long _factScopeGeneration;

        /// <summary>The last process-wide condemnation version this instance
        /// swept its emitted facts against — lets a SIBLING instance's
        /// sentinel trigger this instance's queue purge at its next
        /// tick.</summary>
        private long _seenCondemnationVersion;

        private string _sessionMarker = ShardPilotIds.NewUuidV7();

        /// <summary>Session-start renewals STAGED but not yet committed,
        /// ordered oldest-first by stake: the client's overflow-to-spool
        /// session start runs its commit before the provisional spool
        /// append is confirmed, and a refused append rolls the whole
        /// session mutation back — so the renewal's effects (marker
        /// rotation, adoption, re-arms) must not land until the append
        /// confirms. Each stage is BOUND to the overflow operation that
        /// staged it by the stake <see cref="BeginProvisionalSessionRenewal"/>
        /// returns: rollback abandons ONLY its own stage and commit applies
        /// ONLY its own — a concurrent overflow operation whose own append
        /// was rejected must never abandon (or prematurely land) another
        /// operation's staged renewal, and a raced second session start
        /// must never displace the first's still-pending stage. While ANY
        /// stage is pending, owed sweeps pause and current-session reads
        /// serve the session id from before the FIRST pending transition,
        /// so no consumer state and no emitted envelope can ever name a
        /// session the SDK then refuses. Written under
        /// <see cref="_lock"/>.</summary>
        private readonly List<ProvisionalRenewal> _provisionalRenewals =
            new List<ProvisionalRenewal>();

        /// <summary>Stake allocator for staged renewals (monotonic, under
        /// <see cref="_lock"/>); 0 is never allocated, so callers can use
        /// it as "this operation staged nothing".</summary>
        private long _renewalStakeSeq;

        /// <summary>The highest stake whose renewal actually RAN —
        /// committed stages and direct in-gate renewals alike. A staged
        /// commit resolving AFTER a later transition's renewal already
        /// rotated the marker is superseded: running it would rotate the
        /// marker backward and re-arm exposures under a session the client
        /// has already moved past.</summary>
        private long _renewalStakeCommitted;

        private sealed class ProvisionalRenewal
        {
            public long Stake;
            public bool IsRenewal;
            public string PreviousSessionId;
            public string PendingSessionId;
        }

        private bool _tornDown;
        private bool _closingApplications;
        private bool _authBlocked;
        private long _authEpoch;
        private bool _automaticLaneDisowned;
        private readonly ShardPilotDispatcher _callbackHost;
        private readonly Func<bool> _hostCurrent;

        // The replaced-client warning the first disown owes; the step that
        // completes the handoff logs it (#159).
        private bool _replacedNoticeOwed;

        /// <summary>PER-ENTRY revalidation deadlines, keyed by experiment
        /// key. One shared deadline would let ANY fresh install postpone
        /// every other entry's revalidation: an experiment the host fetches
        /// explicitly every couple of minutes would keep resetting the
        /// shared clock and starve a sibling's cadence indefinitely —
        /// kill/not-assigned updates for the starved entry would never be
        /// seen. Each install arms only ITS OWN key's deadline, and the
        /// tick revalidates exactly the entries that are due. Guarded by
        /// <c>_lock</c>; pruned as cached entries go away.</summary>
        private readonly Dictionary<string, DateTime> _revalidateAtByKey =
            new Dictionary<string, DateTime>(StringComparer.Ordinal);

        private DateTime? _retryAfterUtc;
        private int _backoffAttempt;
        private bool _reminted;

        private static readonly object StaticGate = new object();

        /// <summary>Serializes every durable-record and condemnation-marker
        /// read-modify-write across ALL instances: a facade
        /// re-initialization can leave several live consumers pointing at
        /// one shardpilot.experiments.json, and per-instance locks cannot
        /// stop one instance's read-modify-write from racing another's (a
        /// last writer would silently drop the other's assignment; a read
        /// during a write parses a torn record as empty). Re-entrant, and
        /// always acquired AFTER an instance lock, never before.</summary>
        private static readonly object StorageGate = new object();

        private static long _fetchSeq;

        /// <summary>Per-(cache path, scope, experiment) fence settled by
        /// authoritative outcomes; shared across instances like the
        /// remote-config fence, so siblings pointing at one record order
        /// their installs against each other.</summary>
        private static readonly Dictionary<string, long> Settled =
            new Dictionary<string, long>(StringComparer.Ordinal);

        /// <summary>Per-(cache path, scope) sentinel condemnation stamps: a
        /// real-subjects sentinel drop condemns the scope process-wide, so a
        /// sibling instance's held entries stop serving too and a durable
        /// record that survived a failed clear is refused on load. A fresh
        /// authorized install (stamped above the condemnation) lifts
        /// it.</summary>
        private static readonly Dictionary<string, DateTime> Condemned =
            new Dictionary<string, DateTime>(StringComparer.Ordinal);

        /// <summary>Bumped (under <see cref="StaticGate"/>) on every
        /// condemnation write, so sibling instances notice a sentinel they
        /// did not receive themselves and purge their own already-accepted
        /// queue-resident facts for the condemned scope.</summary>
        private static long CondemnationVersion;

        internal Exception WriteFailureForTesting;

        /// <summary>Invoked between a fetch's dispatch-intent consent read
        /// and its pre-mint gate — lets a test land a consent flip inside
        /// the exact race window the gate closes. No-op in production.</summary>
        internal Action ConsentRaceHookForTesting;

        /// <summary>Invoked between the unlocked pre-dispatch consent gate
        /// and the egress-gated transport handoff — lets a test land a
        /// denial inside the exact check-to-send window the egress gate
        /// closes. No-op in production.</summary>
        internal Action PreEgressHookForTesting;

        /// <summary>Invoked between the unlocked pre-mint consent gate and
        /// the egress-gated mint commit — lets a test land a denial inside
        /// the exact check-to-mint window the gated commit closes. No-op in
        /// production.</summary>
        internal Action PreMintHookForTesting;

        /// <summary>Invoked as a host declaration begins, before its
        /// instance lock, and inside an adult call between choosing its
        /// fence scope and publishing the fence. No-op in production.</summary>
        internal Action HostDeclarationHookForTesting, AdultFenceHookForTesting;

        /// <summary>Invoked in the settle path right after the initial gate
        /// snapshot (epoch/scope/fence) is taken and the outcome is applied,
        /// BEFORE any commit stage runs — lets a test settle a NEWER
        /// response inside the snapshot-to-commit gap the under-lock
        /// recomputes close. No-op in production.</summary>
        internal Action GateSnapshotHookForTesting;

        /// <summary>Static flavor of <see cref="WriteFailureForTesting"/>:
        /// reachable from CONSTRUCTOR-time writes (the denied-launch owed
        /// scrub), where no instance field can have been set yet. Cleared
        /// by <see cref="ResetStaticsForTesting"/>.</summary>
        internal static Exception StaticWriteFailureForTesting;

        internal static Action<string> ClearAvailabilityCheckedForTesting;

        internal Exception DeleteFailureForTesting;

        /// <summary>When non-null, the bounded record read throws it AFTER
        /// the existence check — models a PRESENT but transiently
        /// unreadable cache file (a sharing/IO failure), the case the
        /// unreadable-fail-closed contract keeps distinguishable from
        /// absence. Never set in production.</summary>
        internal Exception RecordReadFailureForTesting;

        internal Func<DateTime> NowOverrideForTesting;
        internal Func<double> JitterOverrideForTesting;

        /// <summary>Invoked inside the egress-gate hold, after the final
        /// torn/epoch re-reads and before the transport handoff — lets a
        /// test park a send in the exact window the teardown barrier must
        /// serialize with. Never set in production.</summary>
        internal Action EgressGateHookForTesting;

        internal static void ResetStaticsForTesting()
        {
            var workStates = ShardPilotWithdrawalWork.DetachForReset();
            ShardPilotWithdrawalJournalOwner.IdentityReference[] identities;
            lock (StaticGate)
            {
                identities = CacheIdentities.Values.ToArray();
                CacheIdentities.Clear();
                _fetchSeq = 0;
                _applicationOrder = 0;
                WithdrawalStates.Clear();
                FactEmissions.Clear();
                UnconfirmedEmissions.Clear();
                Settled.Clear();
                Condemned.Clear();
                MintGates.Clear();
            }
            foreach (var identity in identities) identity.Dispose();
            foreach (var state in workStates) state.CloseQuiescent();

            StaticWriteFailureForTesting = null;
            ClearAvailabilityCheckedForTesting = null;
        }

        internal ShardPilotExperiments(
            ShardPilotConfig config,
            string cachePath,
            IShardPilotHttpRequestSender sender,
            Func<string> subjectId,
            Func<string, bool> storeSubjectId,
            Func<string> reloadSubjectId,
            Func<ShardPilotConsentState> consent,
            Func<string> analyticsSessionId,
            Func<string> anonymousId,
            Action<string> diagnose,
            ShardPilotExperimentFactPurger purgeFacts = null,
            Func<long> consentEpoch = null,
            object egressGate = null,
            string subjectStoreIdentity = null,
            Func<bool> admitCurrentSession = null,
            ShardPilotExperimentKeyFactWithdrawer withdrawKeyFacts = null,
            SealedFactEmitter emitSealed = null, Func<bool> hostCurrent = null,
            Func<ShardPilotWithdrawalWork, WithdrawalFrame, string, bool, WithdrawalFrame> withdrawalDriver = null)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            _config = config;
            WithdrawalDriver = withdrawalDriver;
            _callbackHost = ShardPilotDispatcher.CapturedHost;
            _hostCurrent = hostCurrent ?? (() => true);
            _cachePath = cachePath ?? string.Empty;
            _cachePathKeyToken = _cachePath.ToLowerInvariant();
            _subjectStoreIdentity = subjectStoreIdentity;
            _mintGate = subjectStoreIdentity != null || string.IsNullOrEmpty(_cachePath)
                ? MintGateFor(subjectStoreIdentity ?? string.Empty) : new object();
            var configuredResidency =
                (long)config.BufferSize + config.SpoolMaxEvents + config.BatchSize + 256;
            _maxTrackedFactScopes = configuredResidency > int.MaxValue
                ? int.MaxValue
                : Math.Max(MinTrackedFactScopes, (int)configuredResidency);
            _sender = sender;
            _subjectId = subjectId;
            _storeSubjectId = storeSubjectId;
            _reloadSubjectId = reloadSubjectId;
            _consent = consent;
            _consentEpoch = consentEpoch;
            _egressGate = egressGate ?? new object();
            _analyticsSessionId = analyticsSessionId;
            _anonymousId = anonymousId;
            _emitSealed = emitSealed;
            _admitCurrentSession = admitCurrentSession;
            _purgeFacts = purgeFacts;
            _withdrawKeyFacts = withdrawKeyFacts;
            _diagnose = diagnose;
            _credentialFingerprint = ShardPilotExperimentsLogic.CredentialFingerprint(config.PublishableKey);
            BindCacheIdentity(true);

            // The condemnation-sweep baseline: this instance has emitted no
            // facts yet, so any condemnation that already exists has nothing
            // of ours to purge — only versions bumped AFTER this moment
            // trigger a sweep.
            lock (StaticGate)
            {
                _seenCondemnationVersion = CondemnationVersion;
            }

            // Restore history under its exact scope. Client-id assignments
            // await a fresh host age decision on every launch; a stored adult
            // attribute is not current authority. Synthetic assignments retain
            // their ordinary consent-gated behavior. No subject
            // id is minted here. WITHOUT a stored subject (a missing or
            // corrupt spcid record) the SAME path runs under the tenant
            // PROBE scope — the empty subject component: serving entries
            // can never match it, so nothing serves pre-mint, but the
            // condemnation and parked-drops markers are honored
            // identically, tombstoned drops re-arm, same-tenant retired
            // lineage is retained.
            var subject = CurrentSubjectId();
            var scope = ScopeFor(subject);
            DiscoverWithdrawals();
            {
                Dictionary<string, ShardPilotExperimentEntry> restored;
                lock (_lock)
                {
                    // A persisted condemnation marker means a sentinel's
                    // durable clear was still owed when a previous process
                    // died: honor it BEFORE restoring — seed the
                    // process-wide condemnation (so nothing condemned
                    // serves) and re-arm the owed clear so this process
                    // converges the disk. A corrupt marker fails CLOSED:
                    // nothing restores at all, and the record is cleared
                    // best-effort.
                    string markerScope;
                    DateTime markerAsOf;
                    bool markerCorrupt;
                    if (TryReadCondemnationMarkerLocked(out markerScope, out markerAsOf, out markerCorrupt))
                    {
                        lock (StaticGate)
                        {
                            DateTime known;
                            if (!Condemned.TryGetValue(CondemnationKey(markerScope), out known) ||
                                markerAsOf > known)
                            {
                                Condemned[CondemnationKey(markerScope)] = markerAsOf;

                                // Live siblings that emitted facts for the
                                // marker's scope sweep them at their next
                                // tick.
                                CondemnationVersion++;
                            }
                        }

                        _durableClearPendingAsOfUtc = markerAsOf;
                        _durableClearPendingScope = markerScope;
                    }
                    else if (markerCorrupt)
                    {
                        lock (StorageGate)
                        {
                            ClearDurableRecordLocked();
                            MaybeRemoveCondemnationMarkerLocked();
                        }
                        return;
                    }

                    string historyScope;
                    Dictionary<string, List<string>> history;
                    restored = ReadDurableEntriesLocked(scope, out historyScope, out history);
                    AdoptFactKeyHistoryLocked(scope, historyScope, history);
                    if (_withdrawalMetadataUncertain) restored.Clear();

                    foreach (var pair in restored)
                    {
                        // The key the record carries for this experiment is
                        // what the next sync of the entry retires when it
                        // replaces or drops it (served or not, see below):
                        // that sync may be unable to read the record back,
                        // and then nothing else would know the key.
                        if (pair.Value.SubjectFactKey != null)
                        {
                            _syncedFactKeys[pair.Key] =
                                new KeyValuePair<string, string>(scope, pair.Value.SubjectFactKey);
                        }

                        if (AgeGated(pair.Value)) _launchUndecided.Add(pair.Key);

                        pair.Value.SubjectKey = subject;
                        _entries[pair.Key] = pair.Value;

                    }

                    foreach (var pending in _withdrawalIntents)
                    {
                        var frame = pending.Frame; ShardPilotExperimentEntry held;
                        WithdrawalKind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
                        if ((kind == WithdrawalKind.AgeDrop || kind == WithdrawalKind.Drop) && frame.Scope == scope &&
                            _entries.TryGetValue(frame.Experiment, out held) && (!frame.Fixed || held.FetchedAtUtc.Ticks <= frame.BoundaryTicks))
                            _entries.Remove(frame.Experiment);
                    }
                }
            }
        }

        private DateTime NowUtc()
        {
            var now = NowOverrideForTesting;
            return now != null ? now() : DateTime.UtcNow;
        }

        /// <summary>One tick above the given stamp, with the top of the
        /// DateTime range pinned: every freshness raise (install floors,
        /// condemnation floors, drop stamps) goes through here so a
        /// degenerate MaxValue stamp can never make the raise throw inside
        /// the assignment callback/worker. Belt and braces — the restore
        /// sanitizer already drops MaxValue-stamped records and refuses
        /// MaxValue condemnation markers, so a pinned (non-strict) raise is
        /// unreachable from SDK-written state.</summary>
        private static DateTime RaiseAboveStamp(DateTime stamp)
        {
            return stamp.Ticks >= DateTime.MaxValue.Ticks ? DateTime.MaxValue : stamp.AddTicks(1);
        }

        private double NextJitter()
        {
            var jitter = JitterOverrideForTesting;
            if (jitter != null)
            {
                return jitter();
            }

            lock (RandomGate)
            {
                return SharedRandom.NextDouble();
            }
        }

        private static readonly object RandomGate = new object();
        private static readonly Random SharedRandom = new Random();

        private string CurrentAnalyticsSessionId()
        {
            // While a session-start renewal is STAGED (the client's
            // overflow commit awaiting its spool-append confirmation), the
            // client's session field already names the PENDING session — a
            // session the SDK may yet refuse. Every consumer capture in
            // that window serves the PRE-start id instead, exactly as if it
            // ran before the SessionStart call: a confirmed start's
            // adoption then claims empty/prior captures as always, and an
            // abandoned start leaves nothing naming the refused session.
            // With several stages pending (raced overflow session starts),
            // the OLDEST stage's previous id is the last session the SDK
            // actually confirmed — every later stage's "previous" is itself
            // still pending. All renewal-sensitive callers run under _lock,
            // where the latch is race-free.
            if (_provisionalRenewals.Count > 0)
            {
                return _provisionalRenewals[0].PreviousSessionId ?? string.Empty;
            }

            var read = _analyticsSessionId;
            var value = read != null ? read() : null;
            return value ?? string.Empty;
        }

        private string CurrentAnonymousId()
        {
            var read = _anonymousId;
            var value = read != null ? read() : null;
            return value ?? string.Empty;
        }

        private void Diagnose(string code)
        {
            var hook = _diagnose;
            if (hook != null)
            {
                try
                {
                    hook(code);
                }
                catch
                {
                    // The hook is integrator-adjacent code; never let it
                    // break the fetch path.
                }
            }
        }

        // ── subject id ───────────────────────────────────────────────────────

        /// <summary>The stored subject id when it is wire-valid, else null.
        /// Never mints.</summary>
        private string CurrentSubjectId()
        {
            var value = _subjectId();
            return ShardPilotExperimentsLogic.ValidSubjectId(value) ? value : null;
        }

        /// <summary>The subject id a fetch uses: the persisted one, or a
        /// lazy first-need mint. A missing, corrupt, or non-conforming
        /// stored value re-mints (fresh-install semantics: the subject
        /// re-buckets). A failed persist is diagnosed and the minted id
        /// still rules this process, so one session stays
        /// self-consistent.</summary>
        /// <summary>Serializes the whole check → reload → mint decision —
        /// PROCESS-WIDE, not per instance: the gate is shared by every live
        /// consumer whose subject id lives in the same settings-store
        /// backend, because two overlapping clients sharing one store could
        /// otherwise both reload no persisted spcid_ and mint DIFFERENT
        /// subjects concurrently — whichever store write won would churn
        /// the installation's bucketing. Under the shared gate the loser's
        /// in-lock reload sees the winner's persisted id and ADOPTS it
        /// (converge-on-winner). Keyed by the client-provided SUBJECT-ID
        /// STORE identity — the spcid_ key is GLOBAL to the store while
        /// cache paths are per-config, so two clients sharing one store
        /// but resolving different cache paths (a direct replacement
        /// changing SpoolDir) must still take ONE gate; the identity
        /// token is canonical lower-invariant (the file store's directory
        /// token, which the Unity store's file-routed side shares). An
        /// identity-less or storeless construction falls back to the cache
        /// owner's confirmed cache namespace: the fallback gate shares the
        /// identity used by fences, emissions and progress. Ordered before the instance
        /// lock (adopt takes it inside); store IO runs under this gate but
        /// never under the instance lock.</summary>
        private object _mintGate;

        /// <summary>The shared mint gates, keyed by subject-id store
        /// identity (cache path for identity-less constructions — see
        /// <see cref="_mintGate"/>). Guarded by <see cref="StaticGate"/>;
        /// cleared by <see cref="ResetStaticsForTesting"/>.</summary>
        private static readonly Dictionary<string, object> MintGates =
            new Dictionary<string, object>(StringComparer.Ordinal);

        private static object MintGateFor(string gateKey)
        {
            lock (StaticGate)
            {
                object gate;
                if (!MintGates.TryGetValue(gateKey, out gate))
                {
                    gate = new object();
                    MintGates[gateKey] = gate;
                }

                return gate;
            }
        }

        private string EnsureSubjectId()
        {
            lock (_mintGate)
            {
                var existing = CurrentSubjectId();
                if (existing != null)
                {
                    return existing;
                }

                // Mint-decision convergence: before minting, re-read the
                // PERSISTED id — an overlapping sibling client (a facade
                // re-initialization window) may have minted and persisted
                // since this instance's construction, and a second mint here
                // would overwrite it and churn the subject's bucketing. A
                // valid persisted id is adopted instead. (This instance held
                // no subject, so no entries or exposure state can exist to
                // reset.)
                var reload = _reloadSubjectId;
                if (reload != null)
                {
                    var persisted = reload();
                    if (ShardPilotExperimentsLogic.ValidSubjectId(persisted))
                    {
                        return persisted;
                    }
                }

                return AdoptMintedSubjectIdUnderMintLock();
            }
        }

        /// <summary>A new subject is a new cache scope AND a new exposure
        /// subject: assignments fetched for the old subject must neither
        /// serve nor expose GOING FORWARD, and the session's exposure
        /// de-dupe resets — the de-dupe tuple is per (experiment, version,
        /// SUBJECT), so the new subject's first application must emit even
        /// where the old subject's already did. Owed exposure snapshots are
        /// deliberately RETAINED: a treatment that already ran under the old
        /// subject is a fact about the past, its fact carries the
        /// server-minted fact key (never the subject id), and its tuple —
        /// old subject included — stays distinct from anything the new
        /// subject arms.</summary>
        private string AdoptMintedSubjectId()
        {
            lock (_mintGate)
            {
                return AdoptMintedSubjectIdUnderMintLock();
            }
        }

        private string AdoptMintedSubjectIdUnderMintLock()
        {
            var minted = ShardPilotExperimentsLogic.MintSubjectId();
            lock (_lock)
            {
                // Unsealed applications remain memory-only under their captured
                // subject. They never become restart obligations in the cache.
                _entries.Clear();
                _lastApplications.Clear();
                _appliedTuples.Clear();

                // The rotation retires the old scope. Owed WRITES are
                // cancelled — their source entries were just cleared and a
                // retry against the new scope would describe another
                // subject. Owed DROPS survive with the scope they were
                // decided under: an authoritative kill must still reach the
                // retired scope's record (a foreign-scope retry that never
                // consults current memory).
                var cancelled = new List<string>();
                foreach (var pair in _durablePending)
                {
                    if (!pair.Value.Drop)
                    {
                        cancelled.Add(pair.Key);
                    }
                }

                for (var i = 0; i < cancelled.Count; i++)
                {
                    _durablePending.Remove(cancelled[i]);
                }
            }

            if (!_storeSubjectId(minted))
            {
                Diagnose("subject_id_persist_failed");
            }

            return minted;
        }

        private string ScopeFor(string subjectId)
        {
            return ShardPilotExperimentsLogic.BuildScope(
                _config.WorkspaceKey,
                _config.AppKey,
                _config.EnvironmentKey,
                subjectId,
                _credentialFingerprint,
                _config.AppUrl);
        }

        private static string ConsentRefusal(ShardPilotConsentState state)
        {
            if (state == ShardPilotConsentState.Granted)
            {
                return null;
            }

            return ShardPilotConsent.IsDeniedState(state)
                ? ShardPilotConsent.DeniedError
                : ShardPilotConsent.UnknownError;
        }

        /// <summary>The current consent-denial epoch (0 when the host wired
        /// none — the state read then stands alone, as before).</summary>
        private long ConsentEpochNow()
        {
            var read = _consentEpoch;
            return read != null ? read() : 0;
        }

        // ── durable record ───────────────────────────────────────────────────

        /// <summary>The (scope, experiment) composite key for owed durable
        /// intents: a retired scope's drop and the current scope's intent
        /// for the same experiment must occupy different slots.</summary>
        private static string DurableIntentKey(string scope, string experimentKey)
        {
            return (scope ?? string.Empty) + "\u001f" + (experimentKey ?? string.Empty);
        }

        private string CondemnationKey(string scope)
        {
            return _cachePathKeyToken + "|" + scope;
        }

        private string FenceKey(string scope, string experimentKey)
        {
            return _cachePathIdentityToken + "|" + scope + "\u001f" + experimentKey;
        }

        /// <summary>The usable durable entries for the given scope; corrupt
        /// = miss, foreign scope = miss, and every entry stamped at or below
        /// the scope's sentinel condemnation is refused — a record that
        /// survived a failed sentinel clear must not be reloaded and
        /// re-served (by this instance or any sibling). The record's
        /// fact-key history comes from the SAME read and parse, with the
        /// record scope it belongs to (null when there is no record): a
        /// second read could fail where this one succeeded, and restore the
        /// entries without the keys that match their older facts.</summary>
        private Dictionary<string, ShardPilotExperimentEntry> ReadDurableEntriesLocked(
            string scope,
            out string historyScope,
            out Dictionary<string, List<string>> factKeyHistory)
        {
            var usable = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            historyScope = null;
            factKeyHistory = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            if (string.IsNullOrEmpty(_cachePath))
            {
                return usable;
            }

            string recordScope;
            Dictionary<string, ShardPilotExperimentEntry> entries;
            Dictionary<string, List<string>> history;
            try
            {
                var text = ReadBoundedRecordTextLocked();
                if (text == null ||
                    !ShardPilotExperimentsLogic.TryParseRecord(
                        text, out recordScope, out entries, out history))
                {
                    return usable;
                }
            }
            catch
            {
                return usable;
            }

            historyScope = recordScope;
            factKeyHistory = history;

            if (!string.Equals(recordScope, scope, StringComparison.Ordinal))
            {
                // A same-tenant body under ANOTHER subject's scope is the
                // RETIRED record (a death after a subject re-mint, before
                // the first new-scope write). Its entries are a miss for
                // serving, but their (scope, version, fact-key) coordinates
                // are retained as LINEAGE: a fact restored from the
                // analytics spool that was emitted under the retired scope
                // proves its origin against them, so the condemnation-plane
                // registration can keep it under its ORIGINAL scope instead
                // of misattributing it to the current one (see
                // <see cref="RegisterRestoredFacts"/>). Another tenant's
                // body proves nothing about this pipeline's facts.
                if (ShardPilotExperimentsLogic.ScopesShareTenant(recordScope, scope))
                {
                    foreach (var pair in entries)
                    {
                        if (pair.Value.SubjectFactKey != null)
                        {
                            _retiredRecordLineage[pair.Key] = new RetiredRecordLineage
                            {
                                Scope = recordScope,
                                Version = pair.Value.Version,
                                SubjectFactKey = pair.Value.SubjectFactKey
                            };
                        }
                    }
                }

                return usable;
            }

            DateTime condemnedAt;
            var condemned = TryGetCondemnation(scope, out condemnedAt);
            foreach (var pair in entries)
            {
                if (condemned && pair.Value.FetchedAtUtc <= condemnedAt)
                {
                    continue;
                }

                usable[pair.Key] = pair.Value;
            }

            return usable;
        }

        private bool TryGetCondemnation(string scope, out DateTime condemnedAt)
        {
            lock (StaticGate)
            {
                return Condemned.TryGetValue(CondemnationKey(scope), out condemnedAt);
            }
        }

        private bool WriteDurableRecordLocked(
            string scope,
            Dictionary<string, ShardPilotExperimentEntry> entries,
            List<ShardPilotWithdrawalIntent> preservedWithdrawalIntents = null,
            Dictionary<string, List<string>> preservedFactKeyHistory = null)
        {
            bool unavailable;
            ReadBoundedRecordTextLocked(out unavailable);
            if (unavailable) return false;
            if (string.IsNullOrEmpty(_cachePath))
            {
                return true;
            }

            lock (StorageGate)
                try
                {
                    // Computed ahead of the write so a key this write retires
                    // is in the in-memory history even when the write fails.
                    var factKeyHistory = preservedFactKeyHistory ?? FactKeyHistoryForWriteLocked(scope, entries);
                    var failure = WriteFailureForTesting ?? StaticWriteFailureForTesting;
                    if (failure != null)
                    {
                        throw failure;
                    }

                    lock (StaticGate)
                        foreach (var state in WithdrawalStates.Values)
                        {
                            WithdrawalKind kind; ShardPilotExperimentEntry held, written;
                            if (state.Cache != WithdrawalRegistryPathLocked() || state.Scope != scope || state.Retired ||
                                !ShardPilotWithdrawalForm.TryKind(state.Frame, out kind) ||
                                (kind != WithdrawalKind.AgeDrop && kind != WithdrawalKind.Drop) ||
                                !_entries.TryGetValue(state.Experiment, out held) || !entries.TryGetValue(state.Experiment, out written) ||
                                !ReferenceEquals(held, written)) continue;
                            if (!state.Frame.Fixed) return false;
                            if (written.FetchedAtUtc.Ticks <= state.AsOfUtcTicks) written.FetchedAtUtc = RaiseAboveStamp(new DateTime(state.AsOfUtcTicks));
                        }
                    var intents = preservedWithdrawalIntents ?? new List<ShardPilotWithdrawalIntent>();
                    var serialized = ShardPilotExperimentsLogic.SerializeRecord(
                        scope, entries, factKeyHistory, intents);
                    if (Encoding.UTF8.GetByteCount(serialized) > ShardPilotExperimentsLogic.MaxRecordBytes)
                    {
                        return false;
                    }

                    var directory = Path.GetDirectoryName(_cachePath);
                    if (!string.IsNullOrEmpty(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }

                    File.WriteAllText(_cachePath, serialized);
                    _retiredWithdrawalIntents.Clear();
                    AcknowledgeRetiredWithdrawalStatesLocked();
                    if (preservedFactKeyHistory == null) _factKeyHistoryPruned.Remove(scope);

                    return true;
                }
                catch
                {
                    return false;
                }
        }

        private static bool WithdrawalValueEqual(object left, object right)
        {
            var map = left as Dictionary<string, object>;
            if (map != null)
            {
                var other = right as Dictionary<string, object>; if (other == null || map.Count != other.Count) return false;
                foreach (var pair in map)
                { object value; if (!other.TryGetValue(pair.Key, out value) || !WithdrawalValueEqual(pair.Value, value)) return false; }
                return true;
            }
            var list = left as List<object>;
            if (list == null) return Equals(left, right);
            var items = right as List<object>; if (items == null || list.Count != items.Count) return false;
            for (var i = 0; i < list.Count; i++) if (!WithdrawalValueEqual(list[i], items[i])) return false;
            return true;
        }
        private static bool WithdrawalRecordLossless(string text, string serialized)
        {
            Dictionary<string, object> raw, rewritten;
            return ShardPilotJsonParser.TryParseUniqueObject(text, out raw, out _) &&
                ShardPilotJsonParser.TryParseObject(serialized, out rewritten) && WithdrawalValueEqual(raw, rewritten);
        }
        private static bool ReadWithdrawalAssignment(string path, out string scope,
            out Dictionary<string, ShardPilotExperimentEntry> entries, out Dictionary<string, List<string>> history,
            out List<ShardPilotWithdrawalIntent> intents)
        {
            scope = null; entries = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            history = new Dictionary<string, List<string>>(StringComparer.Ordinal); intents = new List<ShardPilotWithdrawalIntent>();
            if (string.IsNullOrEmpty(path)) return true;
            try
            {
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    if (stream.Length > ShardPilotExperimentsLogic.MaxRecordBytes) return false;
                    var bytes = new byte[(int)stream.Length]; var count = 0;
                    while (count < bytes.Length)
                    { var read = stream.Read(bytes, count, bytes.Length - count); if (read == 0) return false; count += read; }
                    if (stream.Length != bytes.Length || stream.ReadByte() != -1) return false;
                    var text = new UTF8Encoding(false, true).GetString(bytes);
                    return ShardPilotExperimentsLogic.TryParseRecord(text, out scope, out entries, out history, out intents) &&
                        WithdrawalRecordLossless(text, ShardPilotExperimentsLogic.SerializeRecord(scope, entries, history, intents));
                }
            }
            catch (FileNotFoundException) { return true; }
            catch (DirectoryNotFoundException) { return true; }
            catch { return false; }
        }
        internal static long? ObserveWithdrawalAssignment(string path, WithdrawalFrame frame, Exception failure)
        {
            WithdrawalKind kind; if (!ShardPilotWithdrawalForm.TryKind(frame, out kind)) return null;
            if (kind == WithdrawalKind.AgeFacts || kind == WithdrawalKind.Purge) return 0;
            lock (StorageGate)
            {
                string scope; Dictionary<string, ShardPilotExperimentEntry> entries;
                Dictionary<string, List<string>> history; ShardPilotExperimentEntry entry; List<ShardPilotWithdrawalIntent> intents;
                if (failure != null || !ReadWithdrawalAssignment(path, out scope, out entries, out history, out intents)) return null;
                return scope == frame.Scope && entries.TryGetValue(frame.Experiment, out entry) ? entry.FetchedAtUtc.Ticks : 0;
            }
        }
        internal static bool ApplyWithdrawalAssignment(string path, WithdrawalFrame frame, Exception failure)
        {
            WithdrawalKind kind; if (!ShardPilotWithdrawalForm.TryKind(frame, out kind)) return false;
            if (kind == WithdrawalKind.AgeFacts || kind == WithdrawalKind.Purge) return true;
            lock (StorageGate)
            {
                string scope; Dictionary<string, ShardPilotExperimentEntry> entries;
                Dictionary<string, List<string>> history; ShardPilotExperimentEntry entry; List<ShardPilotWithdrawalIntent> intents;
                if (!ReadWithdrawalAssignment(path, out scope, out entries, out history, out intents)) return false;
                if (scope != frame.Scope || !entries.TryGetValue(frame.Experiment, out entry) || entry.FetchedAtUtc.Ticks > frame.BoundaryTicks) return true;
                if (failure != null) return false;
                entries.Remove(frame.Experiment); List<string> keys;
                if (!history.TryGetValue(frame.Experiment, out keys)) history[frame.Experiment] = keys = new List<string>();
                AddValidFactKey(keys, entry.SubjectFactKey);
                try
                {
                    var text = ShardPilotExperimentsLogic.SerializeRecord(scope, entries, history, intents);
                    if (Encoding.UTF8.GetByteCount(text) > ShardPilotExperimentsLogic.MaxRecordBytes) return false;
                    Directory.CreateDirectory(Path.GetDirectoryName(path)); File.WriteAllText(path, text); return true;
                }
                catch { return false; }
            }
        }
        internal static bool WithdrawalEmissionsQuiet(string path, WithdrawalFrame frame)
        {
            WithdrawalKind kind; if (!ShardPilotWithdrawalForm.TryKind(frame, out kind)) return false;
            var cache = ConfirmCacheIdentity(path, frame.Store);
            if (!string.IsNullOrEmpty(path) && cache == null) return false;
            lock (StaticGate)
                foreach (var emission in FactEmissions)
                {
                    if (!string.IsNullOrEmpty(path) && emission.Cache != cache) continue;
                    if (kind == WithdrawalKind.Purge && frame.Pending.Contains(emission.EventId)) return false;
                    if ((int)kind < 2 && emission.Experiment == frame.Experiment && !frame.Spared.Contains(emission.EventId))
                        foreach (var key in emission.FactKeys) if (frame.Keys.Contains(key)) return false;
                }
            return true;
        }

        internal Func<ShardPilotWithdrawalWork, WithdrawalFrame, string, bool, WithdrawalFrame> WithdrawalDriver;
        internal static void ObserveWithdrawalProgress(string path, WithdrawalFrame frame)
        {
            if (frame == null) return;
            var cache = ConfirmCacheIdentity(path, frame.Store);
            if (cache == null) return;
            lock (StaticGate)
                foreach (var state in WithdrawalStates.Values)
                    if (state.Cache == cache && state.Scope == frame.Scope && state.Frame.Id == frame.Id)
                    { state.Frame = frame; state.AsOfUtcTicks = frame.BoundaryTicks; state.Retired = frame.Complete; }
        }
        private PendingWithdrawalIntent QueueWithdrawalLocked(string scope, string experiment, DateTime boundary,
            WithdrawalKind kind, List<string> keys, WithdrawalFrame restored = null)
        {
            var frame = restored ?? new WithdrawalFrame(ShardPilotWithdrawalWork.StoreFor(_config), scope,
                ShardPilotWithdrawalForm.NewId(kind), experiment, 1, Math.Min(boundary.Ticks, DateTime.MaxValue.Ticks - 1),
                false, false, false, (int)kind < 2, (int)kind < 2 ? 1 : 0, keys, new string[0], new string[0]);
            var existing = _withdrawalIntents.Find(p => p.Frame.Id == frame.Id && p.Scope == scope);
            if (existing != null) return existing;
            var pending = new PendingWithdrawalIntent { Scope = scope, Frame = frame, Adopted = restored != null,
                Intent = new ShardPilotWithdrawalIntent { Id = frame.Id, Scope = scope, ExperimentKey = experiment,
                    FactKeys = new List<string>(frame.Keys), SparedEventIds = new List<string>(frame.Spared),
                    WholeKeyPopulation = frame.WholePopulation, AsOfUtcTicks = frame.BoundaryTicks } };
            lock (StorageGate) RegisterWithdrawalStateLocked(pending, created: restored == null);
            _withdrawalIntents.Add(pending); return pending;
        }
        private void DiscoverWithdrawals()
        {
            if (!string.IsNullOrEmpty(_cachePath) && !BindCacheIdentity()) { _withdrawalMetadataUncertain = true; return; }
            var shared = ShardPilotWithdrawalWork.Register(_config, _cachePath, _cachePathIdentityToken);
            bool absent; WithdrawalFrame[] frames = null;
            using (var work = ShardPilotWithdrawalWork.Open(_config, _cachePath, false, out absent))
            {
                var known = work == null ? absent : work.TryRead(out frames);
                frames = work == null || !known ? new WithdrawalFrame[0] : frames;
                lock (_lock)
                    foreach (var frame in frames.Concat(shared?.Unfinished() ?? new WithdrawalFrame[0]))
                    {
                        WithdrawalKind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
                        QueueWithdrawalLocked(frame.Scope, frame.Experiment, new DateTime(frame.BoundaryTicks),
                            kind, new List<string>(frame.Keys), frame);
                    }
                _withdrawalMetadataUncertain = !known;
            }
        }
        internal void ResumeWithdrawals(bool scan = true)
        {
            DiscoverWithdrawals();
            PendingWithdrawalIntent[] pending; lock (_lock) pending = _withdrawalIntents.ToArray();
            foreach (var item in pending) ResumeWithdrawal(item, scan);
        }
        private bool ResumeWithdrawal(PendingWithdrawalIntent pending, bool scan = true)
        {
            if (Interlocked.CompareExchange(ref pending.Busy, 1, 0) != 0) return false;
            try { return ResumeWithdrawalOwned(pending, scan); }
            finally { Volatile.Write(ref pending.Busy, 0); }
        }
        private bool ResumeWithdrawalOwned(PendingWithdrawalIntent pending, bool scan)
        {
            bool absent;
            using (var work = pending.State.Shared.Borrow(true, out absent))
            {
                if (work == null) return false;
                if (!work.TryRecover()) return false;
                WithdrawalFrame current;
                if (!work.TryGet(pending.Scope, pending.Frame.Id, out current)) return false;
                if (current == null && !work.TryPersist(null, pending.Frame, out current)) return false;
                if (!current.Complete)
                {
                    WithdrawalFrame proposed;
                    lock (StaticGate)
                        proposed = new WithdrawalFrame(current.Store, current.Scope, current.Id, current.Experiment,
                            current.Revision + 1, current.BoundaryTicks, current.Fixed, false, false, current.WholePopulation,
                            current.Scans, pending.State.FactKeys, pending.State.Spared,
                            new List<string>(current.Pending).Concat(pending.State.Unconfirmed));
                    if (!work.TryPersist(current, proposed, out current)) return false;
                    var driver = WithdrawalDriver;
                    current = driver != null ? driver(work, current, _cachePath, scan) : work.Converge(current,
                        f => ObserveWithdrawalAssignment(_cachePath, f, RecordReadFailureForTesting),
                        f => RecordReadFailureForTesting == null && ApplyWithdrawalAssignment(_cachePath, f, WriteFailureForTesting ?? StaticWriteFailureForTesting),
                        f => scan && StandaloneWithdrawalPipeline(f));
                    if (current == null) return false;
                }
                lock (_lock)
                {
                    pending.Frame = current;
                    lock (StaticGate) { pending.State.AsOfUtcTicks = current.BoundaryTicks; pending.State.Retired = current.Complete;
                        if (current.Pipeline) pending.State.Unconfirmed.Clear(); }
                    RetireWithdrawalIntentLocked(pending);
                }
                return true;
            }
        }
        private bool StandaloneWithdrawalPipeline(WithdrawalFrame frame)
        {
            WithdrawalKind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
            if (kind == WithdrawalKind.Drop) return true;
            if (!WithdrawalEmissionsQuiet(_cachePath, frame)) return false;
            return kind == WithdrawalKind.Purge ? (_purgeFacts != null && _purgeFacts(new List<string>(frame.Pending))) :
                (_withdrawKeyFacts != null && _withdrawKeyFacts(frame.Experiment, frame.Keys, frame.Spared.Contains, null));
        }

        private static string WithdrawalIntentIdentity(string scope, ShardPilotWithdrawalIntent intent)
        {
            scope = scope ?? string.Empty;
            var experiment = intent.ExperimentKey ?? string.Empty;
            if (!string.IsNullOrEmpty(intent.Id)) return scope.Length.ToString(CultureInfo.InvariantCulture) + ":" + scope +
                experiment.Length.ToString(CultureInfo.InvariantCulture) + ":" + experiment + ":" + intent.Id;
            var keys = new List<string>(intent.FactKeys);
            keys.Sort(StringComparer.Ordinal);
            return scope.Length.ToString(CultureInfo.InvariantCulture) + ":" + scope +
                   experiment.Length.ToString(CultureInfo.InvariantCulture) + ":" + experiment + ":" +
                   intent.AsOfUtcTicks.ToString(CultureInfo.InvariantCulture) + ":" + string.Join(",", keys.ToArray());
        }

        private string WithdrawalRegistryPathLocked()
        {
            if (_withdrawalRegistryPath == null)
                _withdrawalRegistryPath = string.IsNullOrEmpty(_cachePath)
                    ? Guid.NewGuid().ToString("N") : _cachePathIdentityToken;
            return _withdrawalRegistryPath;
        }

        private static long NextApplicationOrder()
        {
            lock (StaticGate) return ++_applicationOrder;
        }

        private static bool WithdrawalMatches(WithdrawalState state, FactEmission emission)
        {
            if (state.Cache != emission.Cache || state.Experiment != emission.Experiment ||
                !ShardPilotExperimentsLogic.ScopesShareTenant(state.Scope, emission.Scope)) return false;
            for (var i = 0; i < emission.FactKeys.Count; i++)
                if (state.FactKeys.Contains(emission.FactKeys[i])) return true;
            return false;
        }

        private static bool HasOlderEmissionLocked(WithdrawalState state)
        {
            foreach (var emission in FactEmissions)
                if (emission.Order <= state.Order && WithdrawalMatches(state, emission)) return true;
            return false;
        }

        private WithdrawalState RegisterWithdrawalStateLocked(PendingWithdrawalIntent pending, bool created = false, bool beginScan = false)
        {
            var cache = WithdrawalRegistryPathLocked();
            var identity = WithdrawalIntentIdentity(pending.Scope, pending.Intent);
            var key = cache.Length.ToString(CultureInfo.InvariantCulture) + ":" + cache + identity;
            lock (StaticGate)
            {
                WithdrawalState state;
                if (pending.State != null) state = pending.State;
                else if (!WithdrawalStates.TryGetValue(key, out state))
                {
                    state = new WithdrawalState
                    {
                        Cache = cache, Scope = pending.Scope, Identity = identity,
                        Experiment = pending.Intent.ExperimentKey, FactKeys = new List<string>(pending.Intent.FactKeys),
                        Order = created ? ++_applicationOrder : 0
                    };
                    WithdrawalStates[key] = state;
                }
                pending.State = state;
                state.Shared = ShardPilotWithdrawalWork.Register(_config, _cachePath, _cachePathIdentityToken, pending.Frame);
                state.Frame = pending.Frame;
                foreach (var factKey in pending.Intent.FactKeys)
                    if (!state.FactKeys.Contains(factKey)) state.FactKeys.Add(factKey);
                state.AsOfUtcTicks = Math.Max(state.AsOfUtcTicks, pending.Intent.AsOfUtcTicks);
                state.WholeKeyPopulation |= pending.Intent.WholeKeyPopulation;
                if (pending.Intent.SparedEventIds != null) state.Spared.UnionWith(pending.Intent.SparedEventIds);
                foreach (var emission in UnconfirmedEmissions.Values)
                    if (emission.Order <= state.Order && WithdrawalMatches(state, emission)) state.Unconfirmed.Add(emission.EventId);
                if (beginScan && !pending.ScanPending)
                {
                    pending.ScanPending = true;
                    state.Scans++;
                }
                return state;
            }
        }

        private void CompleteWithdrawalScanLocked(PendingWithdrawalIntent pending, bool known)
        {
            var state = RegisterWithdrawalStateLocked(pending);
            lock (StaticGate)
            {
                if (pending.ScanPending) { pending.ScanPending = false; state.Scans--; }
                state.PopulationKnown |= known;
            }
            if (known) RetireWithdrawalIntentLocked(pending);
        }

        private void AcknowledgeRetiredWithdrawalStatesLocked(HashSet<string> kept = null)
        {
            var cache = WithdrawalRegistryPathLocked();
            lock (StaticGate)
            {
                var removed = new List<string>();
                foreach (var pair in WithdrawalStates)
                    if (pair.Value.Cache == cache && pair.Value.Retired &&
                        (kept == null || !kept.Contains(pair.Value.Identity))) removed.Add(pair.Key);
                foreach (var key in removed) WithdrawalStates.Remove(key);
            }
        }

        // Certify the actual sealed ID as newer before it can reach a spool.
        // No unresolved matching debt means no extra record write.
        private bool PreserveReadmissionBeforeEmissionLocked(string experimentKey, OwedExposure capture)
        {
            DiscoverWithdrawals();
            if (_withdrawalMetadataUncertain) return false;
            var probe = new FactEmission { Cache = WithdrawalRegistryPathLocked(), Scope = ScopeFor(capture.Entry.SubjectKey),
                Experiment = experimentKey, EventId = capture.Sealed.EventId, Order = capture.CaptureOrder,
                FactKeys = new List<string> { capture.Entry.SubjectFactKey, capture.Sealed.SubjectFactKey } };
            var affected = new List<WithdrawalState>();
            lock (StaticGate)
                foreach (var state in WithdrawalStates.Values)
                {
                    if (state.Retired || !WithdrawalMatches(state, probe)) continue;
                    if (probe.Order <= state.Order) return false;
                    state.Spared.Add(probe.EventId); affected.Add(state);
                }
            foreach (var state in affected)
                if (!ResumeWithdrawal(new PendingWithdrawalIntent { Scope = state.Scope, Frame = state.Frame, State = state }) ||
                    !state.Frame.Spared.Contains(probe.EventId)) return false;
            return true;
        }

        private void RecordLatePurgeResult(FactEmission emission, bool durable)
        {
            lock (StaticGate)
                foreach (var state in WithdrawalStates.Values)
                    if (!state.Retired && emission.Order <= state.Order && WithdrawalMatches(state, emission))
                        state.Unconfirmed.Add(emission.EventId);
            ResumeWithdrawals();
        }

        private void ConfirmPurgeIds(IList<string> ids)
        {
            var cache = WithdrawalRegistryPathLocked();
            lock (StaticGate)
            {
                foreach (var id in ids)
                    UnconfirmedEmissions.Remove(cache.Length.ToString(CultureInfo.InvariantCulture) + ":" + cache + id);
                foreach (var state in WithdrawalStates.Values)
                    if (state.Cache == cache) state.Unconfirmed.ExceptWith(ids);
            }
        }

        private void RetryLateFactPurges()
        {
            ResumeWithdrawals();
        }

        private static bool EmissionWasWithdrawn(FactEmission emission)
        {
            lock (StaticGate)
                foreach (var state in WithdrawalStates.Values)
                    if (emission.Order <= state.Order && WithdrawalMatches(state, emission)) return true;
            return false;
        }

        /// <summary>Merge the whole intent section under StorageGate. A writer
        /// owns its assignment map, not another scope's outstanding purge debt.</summary>
        private List<PendingWithdrawalIntent> WithdrawalIntentsForWriteLocked(
            List<ShardPilotWithdrawalIntent> preserved)
        {
            return new List<PendingWithdrawalIntent>();
        }

        private void RetireWithdrawalIntentLocked(PendingWithdrawalIntent pending)
        {
            if (pending.Frame == null || !pending.Frame.Complete) return;
            pending.Retired = true;
            lock (StaticGate) pending.State.Retired = true;
            _withdrawalIntents.Remove(pending);
        }

        private List<ShardPilotWithdrawalIntent> WithdrawalIntentFormsLocked(List<PendingWithdrawalIntent> intents)
        {
            var forms = new List<ShardPilotWithdrawalIntent>(intents.Count);
            for (var i = 0; i < intents.Count; i++)
            {
                forms.Add(WithdrawalIntentFormForWriteLocked(intents[i]));
            }

            return forms;
        }

        private ShardPilotWithdrawalIntent WithdrawalIntentFormForWriteLocked(PendingWithdrawalIntent pending)
        {
            var source = pending.Intent;
            var state = RegisterWithdrawalStateLocked(pending);
            lock (StaticGate)
                return new ShardPilotWithdrawalIntent
                {
                    Scope = pending.Scope, Id = source.Id, WholeKeyPopulation = state.WholeKeyPopulation,
                    ExperimentKey = source.ExperimentKey, FactKeys = new List<string>(state.FactKeys),
                    AsOfUtcTicks = state.AsOfUtcTicks, SparedEventIds = new List<string>(state.Spared)
                };
        }

        /// <summary>Makes a new withdrawal intent durable when the write
        /// that recorded it carried none: a scope-preserving rewrite of the
        /// file's record. Preserve its actual scope and unrelated entries;
        /// the refused key is deleted only from its own assignment map.
        /// An unreadable file remains untouched (the storage-down residual).
        /// </summary>
        private void PersistWithdrawalIntentLocked(PendingWithdrawalIntent intent, bool dropAssignment)
        {
            // Assignment-cache writes carry no withdrawal authority.
        }

        /// <summary>Construction: apply only intents belonging to this tenant,
        /// even when the assignment-map header belongs to another app. Keep
        /// adopted intents until the restored spool is durably withdrawn.</summary>
        private void AdoptWithdrawalIntentsLocked(string scope)
        {
            // Discovery reads only the journal, outside the consumer lock.
        }

        private void RefreshWithdrawalMetadataLocked()
        {
            // Discovery reads only the journal, outside the consumer lock.
        }

        private bool ClearDurableRecordLocked()
        {
            lock (StorageGate)
            {
                bool unavailable;
                ReadBoundedRecordTextLocked(out unavailable);
                if (unavailable) return false;
                ClearAvailabilityCheckedForTesting?.Invoke(_cachePath);
                if (string.IsNullOrEmpty(_cachePath))
                {
                    return true;
                }

                try
                {
                    var failure = DeleteFailureForTesting;
                    if (failure != null)
                    {
                        throw failure;
                    }

                    // Clearing assignments must not erase already-durable
                    // purge debt, including another app's. If even the
                    // intent-only record cannot be written, keep the old file.
                    var intents = WithdrawalIntentsForWriteLocked(null);
                    if (intents.Count > 0)
                    {
                        Dictionary<string, ShardPilotExperimentEntry> ignoredEntries;
                        var scope = RawDurableRecordLocked(out ignoredEntries) ?? intents[0].Scope;
                        return WriteDurableRecordLocked(scope,
                            new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal));
                    }
                    if (File.Exists(_cachePath))
                    {
                        File.Delete(_cachePath);
                    }

                    _retiredWithdrawalIntents.Clear();
                    AcknowledgeRetiredWithdrawalStatesLocked();
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        /// <summary>The raw stored record for the scope (condemnation NOT
        /// applied — the sync fence must see what is really on disk), or a
        /// fresh empty map.</summary>
        private Dictionary<string, ShardPilotExperimentEntry> RawDurableEntriesLocked(string scope)
        {
            bool ignored;
            return RawDurableEntriesLocked(scope, out ignored);
        }

        /// <summary>Flavor that reports an UNREADABLE (present but
        /// IO-failing) cache file separately from an empty/absent/foreign
        /// one — see <see cref="ReadBoundedRecordTextLocked(out bool)"/>:
        /// the marker lift must fail closed on it.</summary>
        private Dictionary<string, ShardPilotExperimentEntry> RawDurableEntriesLocked(
            string scope,
            out bool unreadable)
        {
            unreadable = false;
            if (string.IsNullOrEmpty(_cachePath))
            {
                return new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            }

            try
            {
                var text = ReadBoundedRecordTextLocked(out unreadable);
                if (text != null)
                {
                    string recordScope;
                    Dictionary<string, ShardPilotExperimentEntry> entries;
                    if (ShardPilotExperimentsLogic.TryParseRecord(text, out recordScope, out entries))
                    {
                        if (string.Equals(recordScope, scope, StringComparison.Ordinal))
                        {
                            return entries;
                        }
                    }
                }
            }
            catch
            {
                // Unreadable = empty; the next write rebuilds the record.
            }

            return new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
        }

        /// <summary>The record file's ACTUAL top-level scope with its entries
        /// — UN-GATED: unlike
        /// <see cref="RawDurableEntriesLocked(string)"/> this does not
        /// scope-miss a foreign record — destructive read-modify-writes (the
        /// fenced sentinel clear) must see whose
        /// record the file really holds, or a rewrite keyed to the caller's
        /// scope would silently wipe another client/subject's newer
        /// assignments. Returns null (with an empty map) when there is no
        /// parseable record.</summary>
        private string RawDurableRecordLocked(
            out Dictionary<string, ShardPilotExperimentEntry> entries)
        {
            bool ignored;
            return RawDurableRecordLocked(out entries, out ignored);
        }

        /// <summary>Flavor that reports an UNREADABLE (present but
        /// IO-failing) cache file separately from "no parseable record" —
        /// see <see cref="ReadBoundedRecordTextLocked(out bool)"/>: the
        /// fenced sentinel clear must fail closed on it instead of
        /// reporting the disk confirmed clean.</summary>
        private string RawDurableRecordLocked(
            out Dictionary<string, ShardPilotExperimentEntry> entries,
            out bool unreadable)
        {
            unreadable = false;
            entries = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            if (string.IsNullOrEmpty(_cachePath))
            {
                return null;
            }

            try
            {
                var text = ReadBoundedRecordTextLocked(out unreadable);
                if (text != null)
                {
                    string recordScope;
                    Dictionary<string, ShardPilotExperimentEntry> parsedEntries;
                    if (ShardPilotExperimentsLogic.TryParseRecord(
                            text, out recordScope, out parsedEntries))
                    {
                        entries = parsedEntries;
                        return recordScope;
                    }
                }
            }
            catch
            {
                // Unreadable = no record; the next write rebuilds it.
            }

            return null;
        }

        /// <summary>Bounded record read: a file past twice the write-side
        /// clamp cannot be a record this SDK wrote — reading it whole would
        /// only feed a corrupt or foreign blob to the parser (and the
        /// allocator), so it is a miss without a read. Returns null on any
        /// miss.</summary>
        private string ReadBoundedRecordTextLocked()
        {
            bool ignored;
            return ReadBoundedRecordTextLocked(out ignored);
        }

        /// <summary>Flavor that keeps ABSENT and UNREADABLE apart — the
        /// store contract the durable reads document: a missing file (or a
        /// deterministic miss — oversized, and corrupt text downstream) is
        /// provably not a record, while a PRESENT file whose read throws
        /// (a transient sharing/IO failure) proves nothing: the same bytes
        /// may read fine on a later launch. Destructive conclusions — a
        /// sentinel clear settling, the condemnation marker lifting — must
        /// fail CLOSED on <paramref name="unreadable"/>, or withdrawn
        /// state under the unread file restores later with no marker left
        /// to suppress it. Restore still treats an unreadable record as a
        /// miss; record writes separately defer when they cannot preserve
        /// its unknown withdrawal intents.</summary>
        private string ReadBoundedRecordTextLocked(out bool unreadable)
        {
            unreadable = false;
            lock (StorageGate)
            {
                if (string.IsNullOrEmpty(_cachePath))
                {
                    return null;
                }

                try
                {
                    // Probe the actual record: existence checks also return
                    // false for access errors, which are not an empty cache.
                    var info = new FileInfo(_cachePath);
                    var length = info.Length;
                    var readFailure = RecordReadFailureForTesting;
                    if (readFailure != null)
                    {
                        throw readFailure;
                    }

                    if (length > (long)ShardPilotExperimentsLogic.MaxRecordBytes * 2)
                    {
                        return null;
                    }

                    return File.ReadAllText(_cachePath);
                }
                catch (FileNotFoundException)
                {
                    // Deleted in the exists-to-read window: genuinely
                    // absent, not unreadable.
                    return null;
                }
                catch (DirectoryNotFoundException)
                {
                    return null;
                }
                catch
                {
                    unreadable = true;
                    return null;
                }
            }
        }

        /// <summary>
        /// Converge one experiment's durable entry to the in-memory truth:
        /// an entry present in memory is written, an absent one is dropped.
        /// <paramref name="asOfUtc"/> stamps the state change and drives the
        /// sibling freshness fence; <paramref name="isRetry"/> marks an
        /// owed-sync retry — only there does the fence apply to DROPS (at
        /// decision time a drop resolves whatever the shared record holds,
        /// clock rollbacks included, and its stamp is raised ABOVE that
        /// record; a retry may instead run after a sibling client persisted
        /// a genuinely newer assignment the drop never saw, and yields to
        /// it). The durable record is maintained INCREMENTALLY, decoupled
        /// from the in-memory serving set: a fail-closed latch clears
        /// serving while the disk deliberately retains the record, so
        /// writing the serving set wholesale would clobber retained sibling
        /// entries.
        /// </summary>
        private void SyncDurableEntryLocked(string scope, string experimentKey, DateTime asOfUtc, bool isRetry)
        {
            RetireSyncedFactKeyLocked(scope, experimentKey);

            // The whole read-modify-write is atomic against sibling
            // instances (StorageGate is re-entrant for the helpers inside).
            if (!_entries.ContainsKey(experimentKey))
            {
                if (!DurableDropTargetKnownAbsent(scope, experimentKey))
                    QueueWithdrawalLocked(scope, experimentKey, asOfUtc, WithdrawalKind.Drop, new List<string>());
                PersistFactKeyHistoryLocked(scope);
                _durablePending.Remove(DurableIntentKey(scope, experimentKey)); return;
            }
            lock (StorageGate)
            {
                SyncDurableEntryUnderStorageGateLocked(scope, experimentKey, asOfUtc, isRetry);
            }
        }

        private bool DurableDropTargetKnownAbsent(string scope, string experimentKey)
        {
            lock (StorageGate)
            {
                string recordScope;
                Dictionary<string, ShardPilotExperimentEntry> entries;
                Dictionary<string, List<string>> history;
                List<ShardPilotWithdrawalIntent> intents;
                return RecordReadFailureForTesting == null &&
                    ReadWithdrawalAssignment(_cachePath, out recordScope, out entries, out history, out intents) &&
                    (recordScope != scope || !entries.ContainsKey(experimentKey));
            }
        }

        private void SyncDurableEntryUnderStorageGateLocked(string scope, string experimentKey, DateTime asOfUtc, bool isRetry)
        {
            ShardPilotExperimentEntry entry;
            _entries.TryGetValue(experimentKey, out entry);
            var record = RawDurableEntriesLocked(scope);
            ShardPilotExperimentEntry stored;
            record.TryGetValue(experimentKey, out stored);
            if (entry != null)
            {
                if (stored != null && stored.FetchedAtUtc > entry.FetchedAtUtc)
                {
                    if (isRetry)
                    {
                        // A same-namespace sibling persisted a FRESHER entry
                        // after this write was decided: never roll the
                        // shared record back.
                        _durablePending.Remove(DurableIntentKey(scope, experimentKey));
                        return;
                    }

                    // At DECISION time a fresh authoritative write
                    // supersedes the stored record exactly like a drop does:
                    // the wall clock can move backward across a refresh, and
                    // yielding on raw stamps would leave the SUPERSEDED
                    // variant as reload truth while memory serves the new
                    // one. Raise the entry's stamp above the record instead.
                    entry.FetchedAtUtc = RaiseAboveStamp(stored.FetchedAtUtc);
                }

                record[experimentKey] = entry;
            }
            else
            {
                if (stored == null)
                {
                    _durablePending.Remove(DurableIntentKey(scope, experimentKey));

                    // The entry was never on disk (its initial write may have
                    // failed): the key it just retired still goes to disk.
                    PersistFactKeyHistoryLocked(scope);
                    return;
                }

                if (isRetry && stored.FetchedAtUtc > asOfUtc)
                {
                    // The RETRY of an owed drop found an entry stamped after
                    // the drop was decided: a sibling client persisted a
                    // newer assignment this drop never resolved. Deleting it
                    // would lose newer valid state, so the outranked drop
                    // settles instead.
                    _durablePending.Remove(DurableIntentKey(scope, experimentKey));
                    return;
                }

                if (!isRetry && stored.FetchedAtUtc >= asOfUtc)
                {
                    // At DECISION time a drop always wins over the stored
                    // record it resolves: the wall clock can move backward,
                    // and fencing the delete on raw stamps would let a
                    // rollback revive a killed variant at the next launch.
                    asOfUtc = RaiseAboveStamp(stored.FetchedAtUtc);
                }

                record.Remove(experimentKey);
            }

            // Combined-record saves FOLD the other owed same-scope intents
            // (drops and writes alike, fence-honoring like their own
            // retries): this store is one record per file, so a save that
            // ignored a sibling key's owed drop would re-persist the very
            // entry that drop exists to remove. A successful save settles
            // the folded intents with it.
            var folded = FoldOwedIntentsLocked(scope, experimentKey, record);
            if (WriteDurableRecordLocked(scope, record))
            {
                _durablePending.Remove(DurableIntentKey(scope, experimentKey));
                for (var i = 0; i < folded.Count; i++)
                {
                    _durablePending.Remove(folded[i]);
                }

                return;
            }

            if (entry != null && stored != null)
            {
                // The refresh write failed with a superseded entry still
                // stored: tombstone it best-effort (the smaller record often
                // fits where the refreshed one did not), so a relaunch
                // starts clean rather than serving the variant memory
                // already replaced. When even the reduced rewrite fails, the
                // record falls back to its fact-key history alone, and only
                // when that fails too is the whole record file deleted (the
                // remote-config tombstone's posture): losing sibling entries
                // to a relaunch-from-defaults is the acceptable price —
                // reviving the superseded variant is not. The owed retry
                // below still converges the disk to the full entry when
                // storage recovers. The refreshed entry is evicted from this
                // save with the stored one, so its key retires into the
                // history the reduced record carries (the stored key retires
                // through the write's own diff): facts built under it would
                // otherwise relaunch with nothing on disk naming it.
                RetireFactKeyLocked(scope, experimentKey, entry.SubjectFactKey);
                record.Remove(experimentKey);
                if (!WriteDurableRecordLocked(scope, record) &&
                    !WriteDurableRecordLocked(
                        scope,
                        new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal)))
                {
                    // The history goes last: a reduced record that still
                    // does not fit (another waiting entry folded into it,
                    // say) was retried above as the history alone, which
                    // revives no variant.
                    ClearDurableRecordLocked();
                }
            }

            _durablePending[DurableIntentKey(scope, experimentKey)] = new DurablePendingSync
            {
                AsOfUtc = asOfUtc,
                Drop = entry == null,
                Scope = scope,
                ExperimentKey = experimentKey
            };
            Diagnose("assignment_cache_persist_failed");
        }

        /// <summary>Applies every OTHER owed same-scope intent to the record
        /// about to be saved, honoring each intent's own retry fence (drops
        /// yield to strictly-newer stored writes; writes yield to fresher
        /// stored entries). Returns the keys whose intents the save will
        /// settle when it succeeds — including intents the fence outranked,
        /// which are settled either way.</summary>
        private List<string> FoldOwedIntentsLocked(
            string scope,
            string savingKey,
            Dictionary<string, ShardPilotExperimentEntry> record)
        {
            var folded = new List<string>();
            foreach (var pair in _durablePending)
            {
                if (string.Equals(pair.Value.ExperimentKey, savingKey, StringComparison.Ordinal) ||
                    !string.Equals(pair.Value.Scope, scope, StringComparison.Ordinal))
                {
                    continue;
                }

                var foldKey = pair.Value.ExperimentKey;
                ShardPilotExperimentEntry stored;
                record.TryGetValue(foldKey, out stored);
                if (pair.Value.Drop)
                {
                    if (stored == null || stored.FetchedAtUtc <= pair.Value.AsOfUtc)
                    {
                        record.Remove(foldKey);
                    }

                    // A stored entry stamped after the drop was decided is a
                    // genuinely newer sibling write: the outranked drop
                    // settles without deleting it, same as its own retry.
                    folded.Add(pair.Key);
                }
                else
                {
                    ShardPilotExperimentEntry memory;
                    if (_entries.TryGetValue(foldKey, out memory))
                    {
                        if (stored == null || stored.FetchedAtUtc <= memory.FetchedAtUtc)
                        {
                            record[foldKey] = memory;
                        }

                        folded.Add(pair.Key);
                    }
                    else
                    {
                        // The write's source entry is gone (an auth latch
                        // raced this save): the intent is stale — settle it
                        // without touching the stored record.
                        folded.Add(pair.Key);
                    }
                }
            }

            return folded;
        }

        /// <summary>Converges a demoted whole-scope clear without consulting
        /// serving memory or raising the original marker's fixed boundary.</summary>
        private void SyncForeignScopeDropLocked(string scope, string experimentKey, DateTime asOfUtc)
        {
            lock (StorageGate) SyncForeignScopeDropUnderStorageGateLocked(scope, experimentKey, asOfUtc);
        }

        private void SyncForeignScopeDropUnderStorageGateLocked(string scope, string experimentKey, DateTime asOfUtc)
        {
            // Demoted whole-scope clears retain the already fixed marker boundary.
            bool unreadable; Dictionary<string, ShardPilotExperimentEntry> entries; ShardPilotExperimentEntry held;
            var recordScope = RawDurableRecordLocked(out entries, out unreadable);
            if (unreadable) return;
            var key = DurableIntentKey(scope, experimentKey);
            if (recordScope != scope || !entries.TryGetValue(experimentKey, out held) || held.FetchedAtUtc > asOfUtc)
            { _durablePending.Remove(key); return; }
            entries.Remove(experimentKey);
            if (WriteDurableRecordLocked(scope, entries)) _durablePending.Remove(key);
        }

        /// <summary>
        /// Demote an owed whole-record clear into per-key drops for exactly
        /// the keys the clear still covers — everything on disk that memory
        /// does not hold — each stamped by the clear decision, raised above
        /// the covered record (the sentinel is decisive over the state it
        /// withdrew), and let the per-key sync converge those. Runs from the
        /// retry tick AND at fresh-install time: a fresh authorized
        /// assignment landing after the failed clear supersedes the
        /// whole-record form immediately, so a later ordinary auth latch —
        /// which empties memory while RETAINING the durable record — can
        /// never leave the stale clear armed to wipe state written after
        /// it.
        /// </summary>
        private void DemoteOwedClearLocked()
        {
            if (!_durableClearPendingAsOfUtc.HasValue)
            {
                return;
            }

            var clearAsOf = _durableClearPendingAsOfUtc.Value;
            var clearScope = _durableClearPendingScope;
            _durableClearPendingAsOfUtc = null;
            _durableClearPendingScope = null;
            var subject = CurrentSubjectId();
            var sameScope = subject != null &&
                            string.Equals(clearScope, ScopeFor(subject), StringComparison.Ordinal);
            var record = RawDurableEntriesLocked(clearScope);
            foreach (var pair in record)
            {
                // A key held in the serving set supersedes the clear ONLY
                // when the serving set describes the clear's own scope — a
                // re-minted subject's entries are another scope's state and
                // say nothing about the retired record.
                if (sameScope && _entries.ContainsKey(pair.Key))
                {
                    continue;
                }

                // The demoted drops carry the CLEAR'S OWN stamp — already
                // raised above the state the sentinel withdrew at decision
                // time — and are deliberately NOT re-raised above records
                // stored since: a stored entry stamped after the clear
                // decision is a sibling's newer write, and the per-key drop
                // fence must YIELD to it instead of blind-wiping it.
                _durablePending[DurableIntentKey(clearScope, pair.Key)] = new DurablePendingSync
                {
                    AsOfUtc = clearAsOf,
                    Drop = true,
                    Scope = clearScope,
                    ExperimentKey = pair.Key
                };
            }
        }

        /// <summary>
        /// Retry every owed durable write (and an owed whole-record clear)
        /// so the disk converges as soon as storage recovers. Local disk
        /// housekeeping only: no network, no serving decisions, so it runs
        /// regardless of the consent state — a kill drop decided under grant
        /// must land durably even if consent flips.
        /// </summary>

        // ── durable condemnation marker (crash-safe sentinel) ────────────────

        private string CondemnationMarkerPath
        {
            get { return string.IsNullOrEmpty(_cachePath) ? null : _cachePath + ".condemned"; }
        }

        /// <summary>Persists the real-subjects condemnation BEFORE the
        /// durable clear is attempted: should the process die with the
        /// clear owed, the next launch reads this marker, refuses the
        /// withdrawn record, and re-attempts the clear — the in-memory owed
        /// state alone would die with the process (the hole this closes). Bounded,
        /// corrupt-fails-closed on read.</summary>
        private bool WriteCondemnationMarkerLocked(string scope, DateTime asOfUtc)
        {
            var path = CondemnationMarkerPath;
            if (path == null)
            {
                return true;
            }

            lock (StorageGate)
            {
                try
                {
                    var failure = WriteFailureForTesting ?? StaticWriteFailureForTesting;
                    if (failure != null)
                    {
                        throw failure;
                    }

                    var directory = Path.GetDirectoryName(path);
                    if (!string.IsNullOrEmpty(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }

                    var builder = new StringBuilder(160);
                    builder.Append("{\"scope\":");
                    ShardPilotJson.AppendQuoted(builder, scope ?? string.Empty);
                    builder.Append(",\"as_of_ticks\":");
                    builder.Append(asOfUtc.Ticks.ToString(CultureInfo.InvariantCulture));
                    builder.Append('}');
                    File.WriteAllText(path, builder.ToString());
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        /// <summary>Reads the persisted condemnation marker. Returns false
        /// when none exists; a marker that exists but cannot be parsed
        /// reports <paramref name="corrupt"/> — the caller fails CLOSED
        /// (skip the restore entirely) rather than serving state a dead
        /// sentinel may have withdrawn.</summary>
        private bool TryReadCondemnationMarkerLocked(out string scope, out DateTime asOfUtc, out bool corrupt)
        {
            scope = null;
            asOfUtc = default(DateTime);
            corrupt = false;
            var path = CondemnationMarkerPath;
            if (path == null)
            {
                return false;
            }

            lock (StorageGate)
            {
                try
                {
                    if (!File.Exists(path))
                    {
                        return false;
                    }

                    if (new FileInfo(path).Length > 8192)
                    {
                        corrupt = true;
                        return false;
                    }

                    Dictionary<string, object> decoded;
                    if (!ShardPilotJsonParser.TryParseObject(File.ReadAllText(path), out decoded))
                    {
                        corrupt = true;
                        return false;
                    }

                    object scopeValue;
                    scope = decoded.TryGetValue("scope", out scopeValue) ? scopeValue as string : null;
                    object ticksValue;
                    // A MaxValue stamp is corrupt like an out-of-range one:
                    // no condemnation this SDK writes can carry it (floors
                    // derive from the wall clock and sanitized stored
                    // stamps), and the install freshness raise puts
                    // AddTicks(1) above the condemnation — treating it as
                    // corrupt keeps the fail-closed nothing-restores rule
                    // instead of arming a stamp the raise would throw on.
                    if (string.IsNullOrEmpty(scope) ||
                        !decoded.TryGetValue("as_of_ticks", out ticksValue) || !(ticksValue is long) ||
                        (long)ticksValue < DateTime.MinValue.Ticks || (long)ticksValue >= DateTime.MaxValue.Ticks)
                    {
                        corrupt = true;
                        scope = null;
                        return false;
                    }

                    asOfUtc = new DateTime((long)ticksValue, DateTimeKind.Utc);
                    return true;
                }
                catch
                {
                    corrupt = true;
                    return false;
                }
            }
        }

        /// <summary>Removes the marker ONLY when the disk truth is clean:
        /// no entry of the marker's scope stamped at or below the
        /// condemnation remains. Crash-safe by construction — the marker
        /// outlives demotions and partial clears until the withdrawn state
        /// is really gone.</summary>
        private void MaybeRemoveCondemnationMarkerLocked()
        {
            var path = CondemnationMarkerPath;
            if (path == null)
            {
                return;
            }

            lock (StorageGate)
            {
                string scope;
                DateTime asOf;
                bool corrupt;
                if (!TryReadCondemnationMarkerLocked(out scope, out asOf, out corrupt))
                {
                    if (!corrupt)
                    {
                        return;
                    }

                    // A corrupt marker heals only once the record file is
                    // gone (the fail-closed posture below never restored
                    // from it anyway).
                    if (!File.Exists(_cachePath))
                    {
                        TryDeleteMarkerLocked(path);
                    }

                    return;
                }

                bool unreadable;
                var record = RawDurableEntriesLocked(scope, out unreadable);
                if (unreadable)
                {
                    // The record file is present but transiently unreadable:
                    // the empty read proves nothing about what it holds, and
                    // the marker is the only crash-safe seed of the
                    // condemnation for the launch that finally reads it —
                    // lifting it here would let the withdrawn record restore unfenced once the file reads
                    // again. Unreadable fails closed: the marker stays.
                    return;
                }

                foreach (var pair in record)
                {
                    if (pair.Value.FetchedAtUtc <= asOf)
                    {
                        return;
                    }
                }

                TryDeleteMarkerLocked(path);
            }
        }

        private void TryDeleteMarkerLocked(string path)
        {
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
            catch
            {
                // Best-effort: a surviving marker is only ever conservative.
            }
        }

        internal void ParkDestructiveDebtsDurably()
        {
            ResumeWithdrawals();
            lock (_lock)
            {
                if (_disownedOwedClearAsOfUtc.HasValue)
                {
                    string markerScope;
                    DateTime markerAsOf;
                    bool markerCorrupt;
                    if (TryReadCondemnationMarkerLocked(out markerScope, out markerAsOf, out markerCorrupt) ||
                        markerCorrupt)
                    {
                        // A live (or corrupt — fail-closed regardless)
                        // marker already covers the clear leg for the next
                        // launch; the parked copy stays for a potential
                        // in-memory adopter.
                        return;
                    }

                    if (WriteCondemnationMarkerLocked(
                            _disownedOwedClearScope, _disownedOwedClearAsOfUtc.Value))
                    {
                        _disownedOwedClearAsOfUtc = null;
                        _disownedOwedClearScope = null;
                    }
                    else
                    {
                        Diagnose("parked_clear_marker_write_failed");
                    }
                }
            }
        }

        /// <summary>The sentinel's durable clear, STAMP-FENCED like every
        /// other authoritative drop: entries stamped at or below the
        /// condemnation go; entries a sibling persisted with a newer stamp
        /// survive (the whole file is deleted only when nothing survives).
        /// Returns true when no condemned entry of the scope remains on
        /// disk.</summary>
        private bool FencedSentinelClearLocked(string scope, DateTime condemnFloor)
        {
            lock (StorageGate)
            {
                // The file's ACTUAL top-level scope decides what the clear
                // may touch. A record ANOTHER client/subject wrote since the
                // clear was decided (a retried sentinel scrub can run
                // arbitrarily late) is not this sentinel's to delete or
                // re-key: only the CONDEMNED scope's entries are fenced, a
                // foreign record's entries pass through verbatim under their
                // own scope, and the whole-file delete below is reachable
                // only when the record IS the condemned scope's (or there is
                // none).
                Dictionary<string, ShardPilotExperimentEntry> stored;
                bool unreadable;
                var recordScope = RawDurableRecordLocked(out stored, out unreadable);
                if (unreadable)
                {
                    // The file is PRESENT but the read failed (a transient
                    // sharing/IO error): the empty result proves nothing —
                    // treating it as "no condemned entry remains" would let
                    // the caller settle the clear and lift the .condemned
                    // marker over a file that may still hold the withdrawn
                    // record, which then restores at a later launch with no
                    // marker left to suppress it. UNREADABLE fails CLOSED
                    // under the durable store's absent-versus-unreadable
                    // distinction: the clear stays owed and the retry machinery keeps
                    // driving it until the disk answers.
                    return false;
                }

                var condemnedRecord = string.Equals(recordScope, scope, StringComparison.Ordinal);
                var record = condemnedRecord
                    ? stored
                    : new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);

                if (record.Count == 0)
                {
                    return true;
                }

                var survivors = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
                foreach (var pair in record)
                {
                    if (pair.Value.FetchedAtUtc > condemnFloor)
                    {
                        survivors[pair.Key] = pair.Value;
                    }
                }

                if (survivors.Count == record.Count)
                {
                    return true;
                }

                if (survivors.Count == 0)
                {
                    return ClearDurableRecordLocked();
                }

                return WriteDurableRecordLocked(scope, survivors);
            }
        }

        internal void RetryDurableSync()
        {
            ResumeWithdrawals();
            RetryConstructiveSync();
            ResumeWithdrawals();
            lock (_lock) MaybeRemoveCondemnationMarkerLocked();
        }
        private void RetryConstructiveSync()
        {
            lock (_lock)
            {
                var subject = CurrentSubjectId();
                var scope = subject != null ? ScopeFor(subject) : null;

                // A DISOWNED client runs no assignment-section machinery at
                // all — no per-key intent, no owed clear, no marker lift:
                // the disown already settled its debts, and this guard keeps
                // any straggler from rewriting the file the successor owns
                // (structural insurance — nothing can arm a new debt behind
                // the disown-gated install).
                if (_automaticLaneDisowned)
                {
                    return;
                }

                if (_durableClearPendingAsOfUtc.HasValue)
                {
                    var clearScope = _durableClearPendingScope;
                    bool clearUnreadable;
                    var clearStored = RawDurableEntriesLocked(clearScope, out clearUnreadable);

                    if (clearUnreadable)
                    {
                        // The cache file is present but the read failed
                        // transiently: an empty result here is
                        // INCONCLUSIVE, not proof the withdrawn record left
                        // the disk — settling would strand withdrawn state
                        // under the unread file for a later launch to
                        // restore. The clear stays owed; the next retry
                        // re-reads.
                    }
                    else if (clearStored.Count == 0)
                    {
                        // The withdrawn record is no longer on disk (cleared
                        // meanwhile, or overwritten by another scope's
                        // record — which the clear must NOT delete): the
                        // owed clear settles.
                        _durableClearPendingAsOfUtc = null;
                        _durableClearPendingScope = null;
                    }
                    else
                    {
                        // An owed clear RETRY never blind-wipes the file: a
                        // sibling client may have persisted a genuinely
                        // newer entry since the clear was decided. It
                        // demotes to stamp-fenced per-key drops instead —
                        // each yields to strictly-newer stored writes — and
                        // the per-key retries below converge them.
                        DemoteOwedClearLocked();
                    }
                }

                if (_durablePending.Count == 0)
                {
                    // The marker lift must be considered on THIS pass too: a clear that just settled above leaves no
                    // per-key intents, and skipping the check here would
                    // strand the marker until an unrelated intent shows up.
                    MaybeRemoveCondemnationMarkerLocked();
                    return;
                }

                var keys = new List<string>(_durablePending.Keys);
                keys.Sort(StringComparer.Ordinal);
                for (var i = 0; i < keys.Count; i++)
                {
                    DurablePendingSync pending;
                    if (!_durablePending.TryGetValue(keys[i], out pending))
                    {
                        continue;
                    }

                    if (pending.Drop) SyncForeignScopeDropLocked(pending.Scope, pending.ExperimentKey, pending.AsOfUtc);
                    else if (scope != null && string.Equals(pending.Scope, scope, StringComparison.Ordinal))
                    {
                        SyncDurableEntryLocked(scope, pending.ExperimentKey, pending.AsOfUtc, true);
                    }
                    else
                    {
                        // A foreign-scope WRITE cannot exist (rotation
                        // cancels them at adopt time); a straggler is stale
                        // — settle it without touching anything.
                        _durablePending.Remove(keys[i]);
                    }
                }

                // The persisted condemnation marker lifts only once the
                // disk truly holds no condemned entry (crash-safe: it
                // outlives demotions until the drops land); parked-drop
                // tombstones lift per key as their debts leave the owed map.
                MaybeRemoveCondemnationMarkerLocked();
            }
        }

        // ── exposure lane ────────────────────────────────────────────────────

        /// <summary>The current scope, or null pre-mint — the shape the
        /// scope-preserving owed flush accepts.</summary>
        private string CurrentScopeOrNullLocked()
        {
            var subject = CurrentSubjectId();
            return subject != null ? ScopeFor(subject) : null;
        }

        private sealed class EmittedFactScope
        {
            public string Scope;

            /// <summary>The source entry's freshness stamp: the
            /// condemnation fence compares it, so a fact emitted from a
            /// POST-condemnation fresh install (which lifts the condemned
            /// state) is never purged by an older sentinel.</summary>
            public DateTime FetchedAtUtc;

            /// <summary>Which order-queue entry OWNS this mapping: the
            /// cap eviction removes the mapping only when the dequeued
            /// (id, generation) pair matches — see
            /// <see cref="_factScopeOrder"/>.</summary>
            public long Generation;

            /// <summary>The <see cref="_withdrawalGeneration"/> the fact's
            /// source was read under: an age withdrawal of a later
            /// generation withdraws it, one it predates spares it. Zero for
            /// restored facts.</summary>
            public long WithdrawalGeneration;
        }

        /// <summary>Registers an emitted fact on the CONDEMNATION plane —
        /// every session's facts, prior-session drains included, unlike the
        /// session-scoped delivery index.</summary>
        private void TrackFactScopeLocked(
            string eventId, string scope, DateTime fetchedAtUtc, long withdrawalGeneration = 0)
        {
            if (_factScopes.ContainsKey(eventId))
            {
                return;
            }

            var generation = ++_factScopeGeneration;
            _factScopes[eventId] = new EmittedFactScope
            {
                Scope = scope,
                FetchedAtUtc = fetchedAtUtc,
                Generation = generation,
                WithdrawalGeneration = withdrawalGeneration
            };
            _factScopeOrder.Enqueue(new KeyValuePair<string, long>(eventId, generation));
            while (_factScopeOrder.Count > _maxTrackedFactScopes)
            {
                // Generation-fenced eviction: the dequeued pair evicts the
                // mapping only when it is the SAME registration. A
                // withdrawn never-shipped fact leaves a stale pair behind,
                // and its tuple's same-session re-emit REUSES the
                // deterministic event id — an unconditional removal here
                // would let that stale head delete the FRESH mapping once
                // the bounded index wraps, and a later sentinel would miss
                // a still-queued fact carrying a withdrawn subject-fact
                // key. Stale pairs just drop; the map stays bounded by the
                // queue (every mapping has exactly one live pair).
                var evicted = _factScopeOrder.Dequeue();
                EmittedFactScope current;
                if (_factScopes.TryGetValue(evicted.Key, out current) &&
                    current.Generation == evicted.Value)
                {
                    _factScopes.Remove(evicted.Key);
                }
            }
        }

        /// <summary>The event ids of this instance's emitted facts that a
        /// condemnation covers (scope match AND stamped at or below the
        /// condemnation), removed from the index as they are collected.</summary>
        private List<string> CollectCondemnedFactIdsLocked()
        {
            ForgetCondemnedApplicationsLocked();
            List<string> purge = null;
            foreach (var pair in _factScopes)
            {
                DateTime condemnedAt;
                if (TryGetCondemnation(pair.Value.Scope, out condemnedAt) &&
                    pair.Value.FetchedAtUtc <= condemnedAt)
                {
                    if (purge == null)
                    {
                        purge = new List<string>();
                    }

                    purge.Add(pair.Key);
                }
            }

            if (purge != null)
            {
                for (var i = 0; i < purge.Count; i++)
                {
                    _factScopes.Remove(purge[i]);

                }

                purge.Sort(StringComparer.Ordinal);
            }

            return purge;
        }

        /// <summary>A SIBLING instance's sentinel condemned a scope since
        /// this instance last looked: withdraw this instance's own
        /// already-accepted facts for it too. Version-gated so the scan only
        /// runs when a condemnation actually landed.</summary>
        private void SweepCondemnedFacts()
        {
            List<string> purge;
            lock (_lock)
            {
                long version;
                lock (StaticGate)
                {
                    version = CondemnationVersion;
                }

                if (version == _seenCondemnationVersion)
                {
                    return;
                }

                _seenCondemnationVersion = version;
                purge = CollectCondemnedFactIdsLocked();
            }

            InvokeFactPurge(purge);
        }

        /// <summary>One retired-record entry's identifying coordinates,
        /// retained at load time so restored spool facts can prove a
        /// retired-scope origin (see <see cref="RegisterRestoredFacts"/>).</summary>
        private sealed class RetiredRecordLineage
        {
            public string Scope;
            public long Version;
            public string SubjectFactKey;
        }

        /// <summary>Per-experiment lineage of a same-tenant RETIRED record
        /// body observed at load (empty when the body was this scope's own,
        /// foreign-tenant, or absent). Guarded by <c>_lock</c>.</summary>
        private readonly Dictionary<string, RetiredRecordLineage> _retiredRecordLineage =
            new Dictionary<string, RetiredRecordLineage>(StringComparer.Ordinal);

        /// <summary>Resolves the ORIGINAL scope of a restored fact from its
        /// own envelope coordinates, when SDK-written durable state proves
        /// it: the retired record body's lineage retained at load, matching the
        /// fact's full (experiment, version, subject-fact-key) triple. The
        /// subject-fact key is server-minted per subject, so a match
        /// identifies the scope lineage; a foreign tenant's state is never
        /// consulted. False = unprovable.</summary>
        private bool TryResolveRestoredFactScopeLocked(
            ShardPilotRestoredFact fact, out string originalScope)
        {
            originalScope = null;
            if (string.IsNullOrEmpty(fact.ExperimentKey) || !fact.Version.HasValue ||
                string.IsNullOrEmpty(fact.AssignmentKey))
            {
                return false;
            }

            RetiredRecordLineage lineage;
            if (_retiredRecordLineage.TryGetValue(fact.ExperimentKey, out lineage) &&
                lineage.Version == fact.Version.Value &&
                string.Equals(lineage.SubjectFactKey, fact.AssignmentKey, StringComparison.Ordinal))
            {
                originalScope = lineage.Scope;
                return true;
            }

            return false;
        }

        /// <summary>Registers experiment facts RESTORED from the analytics
        /// spool at relaunch on the condemnation plane. Restored copies are
        /// previous-process emissions — the emit-time pre-registration
        /// never saw them — so without this a real-subjects sentinel
        /// landing before the restored batch flushes finds no id to
        /// withdraw and the facts publish with withdrawn subject-fact keys.
        /// Each fact registers under its ORIGINAL scope where its own
        /// envelope coordinates prove one: a fact emitted under a RETIRED
        /// scope (pre-re-mint) and spooled across launches matches the
        /// retired record's lineage and keeps that scope — a CURRENT-scope sentinel must spare it, the
        /// same preservation the owed-exposure machinery gives retired
        /// facts — while its own scope's condemnation still withdraws it.
        /// The stamp RECOVERS the fact's true freshness where it can be
        /// proven: a restored fact whose coordinates (experiment key,
        /// version, assignment key) match the SERVING entry re-registers at
        /// that entry's <c>FetchedAtUtc</c> — a fact a post-re-enable fresh
        /// install emitted must survive a STALE condemnation marker (a
        /// failed clear's) exactly like the entry itself does. Anything
        /// unprovable keeps the conservative floor under the CURRENT scope:
        /// any sentinel this process sees withdraws it. A condemnation
        /// already standing at restore time for any registered scope purges
        /// its facts immediately.</summary>
        internal void RegisterRestoredFacts(List<ShardPilotRestoredFact> facts)
        {
            if (facts == null || facts.Count == 0)
            {
                return;
            }

            // Include facts whose assignment or persisted subject is gone.
            // A late same-launch handoff retains any host decision already
            // accepted for its key, even if nothing existed at decision time.
            lock (_lock)
            {
                for (var i = 0; i < facts.Count; i++)
                {
                    var key = facts[i] == null ? null : facts[i].ExperimentKey;
                    if (key != null && !_launchDecided.Contains(key)) _launchUndecided.Add(key);
                }
            }

            var subject = CurrentSubjectId();
            if (subject == null)
            {
                // No persisted subject = no scope to key the condemnation
                // on (and no assignment this install could have emitted
                // under); the load-time pending-purge filter remains the
                // covering mechanism.
                return;
            }

            var scope = ScopeFor(subject);
            List<string> purge = null;
            lock (_lock)
            {
                var condemnedScopeRegistered = false;
                for (var i = 0; i < facts.Count; i++)
                {
                    var fact = facts[i];
                    if (fact == null || string.IsNullOrEmpty(fact.EventId))
                    {
                        continue;
                    }

                    var factScope = scope;
                    var stamp = DateTime.MinValue;
                    ShardPilotExperimentEntry serving;
                    string originalScope;
                    if (!string.IsNullOrEmpty(fact.ExperimentKey) && fact.Version.HasValue &&
                        !string.IsNullOrEmpty(fact.AssignmentKey) &&
                        _entries.TryGetValue(fact.ExperimentKey, out serving) && serving != null &&
                        serving.Version == fact.Version.Value &&
                        string.Equals(serving.SubjectFactKey, fact.AssignmentKey, StringComparison.Ordinal))
                    {
                        stamp = serving.FetchedAtUtc;
                    }
                    else if (TryResolveRestoredFactScopeLocked(fact, out originalScope))
                    {
                        // A PROVEN retired-scope fact keeps its original
                        // scope at the floor stamp: its own scope's
                        // condemnation (standing or later) withdraws it,
                        // while the current scope's sentinels leave it to
                        // flush.
                        factScope = originalScope;
                    }

                    DateTime registeredScopeCondemnedAt;
                    if (TryGetCondemnation(factScope, out registeredScopeCondemnedAt))
                    {
                        condemnedScopeRegistered = true;
                    }

                    TrackFactScopeLocked(fact.EventId, factScope, stamp);
                }

                if (condemnedScopeRegistered)
                {
                    purge = CollectCondemnedFactIdsLocked();
                }
            }

            InvokeFactPurge(purge);
        }

        /// <summary>Invokes the client's fact purger OUTSIDE the consumer
        /// lock (the purger takes the client's own gate). Returns true when
        /// the purged ids are durably owed, or there was nothing to
        /// purge.</summary>
        private bool InvokeFactPurge(List<string> eventIds)
        {
            if (eventIds == null || eventIds.Count == 0)
            {
                return true;
            }

            var purge = _purgeFacts;
            if (purge == null)
            {
                return true;
            }

            try
            {
                return purge(eventIds);
            }
            catch
            {
                // The purger is client plumbing; never let it break the
                // consumer's paths.
                return false;
            }
        }

        /// <summary>Discards the CONDEMNED scope's owed exposure state —
        /// its armed snapshots — while every other scope's rides on: a
        /// retired scope's owed facts (kept across a subject re-mint) are
        /// not this sentinel's to withdraw. A snapshot that cannot name its
        /// scope is treated as the condemned scope's (the conservative side
        /// of a withdrawal).</summary>
        private void DiscardCondemnedScopeOwedLocked(string scope)
        {
            var armedKeys = new List<string>(_pendingExposure.Keys);
            for (var i = 0; i < armedKeys.Count; i++)
            {
                var list = _pendingExposure[armedKeys[i]];
                for (var j = list.Count - 1; j >= 0; j--)
                {
                    var entry = list[j].Entry;
                    if (entry == null || entry.SubjectKey == null ||
                        string.Equals(ScopeFor(entry.SubjectKey), scope, StringComparison.Ordinal))
                    {
                        list.RemoveAt(j);
                    }
                }

                if (list.Count == 0)
                {
                    _pendingExposure.Remove(armedKeys[i]);
                }
            }
        }

        /// <summary>
        /// The <c>age_ineligible</c> drop. The server refused THIS subject
        /// for the experiment, so what the ordinary drop keeps as facts —
        /// exposures still owed for applications that already ran — is
        /// withdrawn instead: the
        /// in-memory owed snapshots, and, through the returned withdrawal
        /// the caller hands to the client outside this lock, the facts
        /// already accepted into the analytics pipeline. Nothing is captured
        /// durably, so a relaunch has nothing to re-arm. The withdrawal is
        /// scoped to this experiment and the refused subject: the owed
        /// applications by the refused scope, the pipeline's facts by the
        /// subject's fact keys — the dropped entry's, the refusal's echo,
        /// the keys earlier versions and drops left in the fact-key history,
        /// and the keys of the owed applications withdrawn here. Other
        /// experiments' and other subjects' owed applications are
        /// untouched. With <paramref name="keepAssignment"/> only the owed
        /// applications are withdrawn: the held entry and its durable copy
        /// stay (a synthetic-subject assignment a non-adult declaration
        /// leaves serving).
        /// </summary>
        private WithdrawnApplications WithdrawRefusedApplicationsLocked(
            string scope, string experimentKey, string refusedFactKey, DateTime resolvedAtUtc,
            bool keepAssignment = false, IList<string> declaredFactKeys = null)
        {
            ShardPilotExperimentEntry dropped;
            _entries.TryGetValue(experimentKey, out dropped);
            // A pending application can settle after remint. Its refusal
            // governs its captured scope, never the successor's held entry.
            if (dropped != null && !string.Equals(ScopeFor(dropped.SubjectKey), scope, StringComparison.Ordinal))
            {
                dropped = null;
            }
            if (!keepAssignment && dropped != null)
            {
                _entries.Remove(experimentKey);
            }

            // The drop outranks the entry it resolves, as the ordinary
            // drop's stamp does: the wall clock can move backward.
            var asOf = resolvedAtUtc;
            if (dropped != null && dropped.FetchedAtUtc >= asOf)
            {
                asOf = RaiseAboveStamp(dropped.FetchedAtUtc);
            }

            var factKeys = new List<string>(2);
            if (dropped != null)
            {
                AddValidFactKey(factKeys, dropped.SubjectFactKey);
            }

            if (refusedFactKey != null && !factKeys.Contains(refusedFactKey))
            {
                factKeys.Add(refusedFactKey);
            }

            // Both the unanswered end and an authoritative refusal after a
            // remint still cover the original call's known fact population.
            if (declaredFactKeys != null)
                for (var i = 0; i < declaredFactKeys.Count; i++) AddValidFactKey(factKeys, declaredFactKeys[i]);

            // The subject's keys from earlier versions: facts built under a
            // key a republish rotated out, or under the key of an entry a
            // drop deleted, can still be queued, held, spooled or restored,
            // and they belong to this refusal as much as the live entry's.
            Dictionary<string, List<string>> scopeHistory;
            List<string> retiredKeys;
            if (_factKeyHistory.TryGetValue(scope, out scopeHistory) &&
                scopeHistory.TryGetValue(experimentKey, out retiredKeys))
            {
                for (var i = 0; i < retiredKeys.Count; i++)
                {
                    AddValidFactKey(factKeys, retiredKeys[i]);
                }
            }

            // The in-memory owed snapshots: the sweep must not drain them.
            List<OwedExposure> owed;
            if (_pendingExposure.TryGetValue(experimentKey, out owed))
            {
                for (var i = owed.Count - 1; i >= 0; i--)
                {
                    var snapshot = owed[i];
                    var snapshotScope = snapshot.Entry != null && snapshot.Entry.SubjectKey != null
                        ? ScopeFor(snapshot.Entry.SubjectKey) : null;
                    if (IsRefusedApplication(snapshot.Entry, snapshotScope, scope, factKeys))
                    {
                        if (snapshot.Entry != null)
                        {
                            AddValidFactKey(factKeys, snapshot.Entry.SubjectFactKey);
                            AddValidFactKey(factKeys, snapshot.Sealed?.SubjectFactKey);
                        }

                        owed.RemoveAt(i);
                    }
                }

                if (owed.Count == 0)
                {
                    _pendingExposure.Remove(experimentKey);
                }
            }

            ForgetApplicationsLocked(scope, experimentKey);

            lock (StaticGate)
                foreach (var emission in FactEmissions)
                    if (emission.Cache == WithdrawalRegistryPathLocked() && emission.Scope == scope && emission.Experiment == experimentKey)
                        foreach (var key in emission.FactKeys) AddValidFactKey(factKeys, key);
            var intent = QueueWithdrawalLocked(scope, experimentKey, asOf,
                keepAssignment ? WithdrawalKind.AgeFacts : WithdrawalKind.AgeDrop, factKeys);

            var withdrawal = new WithdrawnApplications
            {
                Generation = ++_withdrawalGeneration,
                SubjectFactKeys = factKeys,
                Intent = intent
            };

            // Every named key records THIS generation; the other keys of the
            // experiment keep theirs.
            Dictionary<string, long> generations;
            if (!_withdrawnKeyGenerations.TryGetValue(experimentKey, out generations))
            {
                generations = new Dictionary<string, long>(StringComparer.Ordinal);
                _withdrawnKeyGenerations[experimentKey] = generations;
            }

            for (var i = 0; i < factKeys.Count; i++)
            {
                generations[factKeys[i]] = withdrawal.Generation;
            }

            if (generations.Count == 0)
            {
                _withdrawnKeyGenerations.Remove(experimentKey);
            }

            return withdrawal;
        }

        private static void AddValidFactKey(List<string> factKeys, string factKey)
        {
            if (ShardPilotExperimentsLogic.ValidSubjectFactKey(factKey) && !factKeys.Contains(factKey))
            {
                factKeys.Add(factKey);
            }
        }

        /// <summary>True when an owed application belongs to the refused
        /// subject: its scope, which carries the subject, is the refused
        /// scope, whatever its fact key. The server mints a new fact key
        /// for every published version, so a same-scope snapshot under
        /// another key is the refused subject's application of an earlier
        /// version, not another subject's; the refusal withdraws it too.
        /// An application that names no scope belongs to the refused
        /// subject only through a known matching fact key — unknown keys
        /// never match another subject's record.</summary>
        private static bool IsRefusedApplication(
            ShardPilotExperimentEntry entry, string entryScope, string scope, List<string> factKeys)
        {
            if (entryScope == null)
            {
                return entry != null && entry.SubjectFactKey != null && factKeys.Contains(entry.SubjectFactKey);
            }

            return string.Equals(entryScope, scope, StringComparison.Ordinal);
        }

        /// <summary>True when an <c>age_ineligible</c> withdrawal of this
        /// experiment that named the fact key landed after
        /// <paramref name="generation"/> was read (a withdrawal that knew no
        /// fact key names no fact).</summary>
        private bool WithdrawnSinceLocked(string experimentKey, string subjectFactKey, long generation)
        {
            long withdrawnAt;
            return TryGetKeyWithdrawalLocked(experimentKey, subjectFactKey, out withdrawnAt) &&
                   withdrawnAt > generation;
        }

        /// <summary>The generation of the latest age withdrawal that named
        /// this experiment's fact key, if one is recorded.</summary>
        private bool TryGetKeyWithdrawalLocked(string experimentKey, string subjectFactKey, out long generation)
        {
            generation = 0;
            Dictionary<string, long> generations;
            return experimentKey != null && subjectFactKey != null &&
                   _withdrawnKeyGenerations.TryGetValue(experimentKey, out generations) &&
                   generations.TryGetValue(subjectFactKey, out generation);
        }

        /// <summary>Hands an age refusal's pipeline withdrawal to the client
        /// OUTSIDE the consumer lock (the withdrawer takes the client's own
        /// gate).</summary>
        private void InvokeKeyFactWithdrawal(string experimentKey, WithdrawnApplications withdrawal)
        {
            if (_closingApplications) return;
            if (withdrawal == null) { ResumeWithdrawals(); return; }
            ResumeWithdrawals(false);
            AgeWithdrawalPreScanHookForTesting?.Invoke();
            ResumeWithdrawals();
        }

        /// <summary>True for a fact this instance emitted from a source read
        /// at or after <paramref name="generation"/>: a later application
        /// (a re-admission installed before the withdrawal's pipeline scan
        /// ran), not one the withdrawal covers. Called by the client under
        /// its gate (gate → consumer lock, the existing order).</summary>
        private bool EmittedSince(string eventId, long generation)
        {
            lock (_lock)
            {
                EmittedFactScope registered;
                return eventId != null && _factScopes.TryGetValue(eventId, out registered) &&
                       registered.WithdrawalGeneration >= generation;
            }
        }

        /// <summary>Test seam: runs after an age refusal's install released
        /// the lock and before its pipeline scan. Never assigned in
        /// production.</summary>
        internal Action AgeWithdrawalPreScanHookForTesting;

        /// <summary>Whether any age withdrawal was recorded (the client's
        /// transport-handoff check skips its per-fact work otherwise).</summary>
        internal bool HasAgeWithdrawals
        {
            get
            {
                lock (_lock)
                {
                    return _withdrawnKeyGenerations.Count > 0;
                }
            }
        }

        /// <summary>True for a fact a recorded age withdrawal covers: a
        /// withdrawal of its experiment named its subject fact key, and its
        /// source predates the latest such withdrawal (a fact this instance
        /// did not emit — restored, or no longer indexed — predates every
        /// withdrawal). The client consults it at the transport handoff,
        /// under its gate.</summary>
        internal bool IsAgeWithdrawnFact(string eventId, string experimentKey, string subjectFactKey)
        {
            lock (_lock)
            {
                long withdrawnAt;
                if (!TryGetKeyWithdrawalLocked(experimentKey, subjectFactKey, out withdrawnAt))
                {
                    return false;
                }

                EmittedFactScope registered;
                var built = eventId != null && _factScopes.TryGetValue(eventId, out registered)
                    ? registered.WithdrawalGeneration
                    : 0;
                return built < withdrawnAt;
            }
        }

        // ── fact-key history ─────────────────────────────────────────────────

        /// <summary>The liveness token the prune and the client's
        /// live-fact collection share: "experiment key U+001F fact
        /// key".</summary>
        internal static string FactKeyToken(string experimentKey, string subjectFactKey)
        {
            return experimentKey + "\u001f" + subjectFactKey;
        }

        /// <summary>Marks a fact whose source (the serving entry or an owed
        /// snapshot) was read in THIS lock hold, together with the
        /// withdrawal generation, as in emission until
        /// <see cref="LeaveFactEmission"/>. Until its enqueue and late purge
        /// return, the
        /// fact can be in no pipeline holder, and its owed snapshot can be
        /// gone, yet the emit-window checks still read its key's withdrawal
        /// generation: the prune counts it as live. Returns the release
        /// token, or null when the source names no key.</summary>
        private FactEmission EnterFactEmissionLocked(string experimentKey, OwedExposure capture)
        {
            var entry = capture.Entry;
            if (experimentKey == null || entry == null || entry.SubjectFactKey == null)
            {
                return null;
            }

            var keys = new List<string>();
            AddValidFactKey(keys, entry.SubjectFactKey);
            AddValidFactKey(keys, capture.Sealed.SubjectFactKey);
            var emission = new FactEmission
            {
                Cache = WithdrawalRegistryPathLocked(), Scope = ScopeFor(entry.SubjectKey),
                Experiment = experimentKey, EventId = capture.Sealed.EventId,
                FactKeys = keys, Order = capture.CaptureOrder
            };
            foreach (var key in keys)
            {
                var token = FactKeyToken(experimentKey, key);
                int count;
                _factKeysInEmission.TryGetValue(token, out count);
                _factKeysInEmission[token] = count + 1;
            }
            lock (StaticGate) FactEmissions.Add(emission);
            return emission;
        }

        private void LeaveFactEmission(FactEmission emission)
        {
            if (emission == null)
            {
                return;
            }

            lock (_lock)
            {
                foreach (var key in emission.FactKeys)
                {
                    var token = FactKeyToken(emission.Experiment, key);
                    int count;
                    if (!_factKeysInEmission.TryGetValue(token, out count)) continue;
                    if (count > 1) _factKeysInEmission[token] = count - 1;
                    else _factKeysInEmission.Remove(token);
                }
                lock (StaticGate)
                {
                    FactEmissions.Remove(emission);
                    foreach (var state in WithdrawalStates.Values)
                        if (state.Frame?.Complete == true)
                            state.Retired = true;
                }
            }
            ResumeWithdrawals();
        }

        /// <summary>Adds a subject-fact key that no longer rides a serving
        /// entry of <paramref name="scope"/> to that scope's history for the
        /// experiment.</summary>
        private void RetireFactKeyLocked(string scope, string experimentKey, string subjectFactKey)
        {
            if (scope == null || string.IsNullOrEmpty(experimentKey) ||
                !ShardPilotExperimentsLogic.ValidSubjectFactKey(subjectFactKey))
            {
                return;
            }

            Dictionary<string, List<string>> scopeHistory;
            if (!_factKeyHistory.TryGetValue(scope, out scopeHistory))
            {
                scopeHistory = new Dictionary<string, List<string>>(StringComparer.Ordinal);
                _factKeyHistory[scope] = scopeHistory;
            }

            List<string> keys;
            if (!scopeHistory.TryGetValue(experimentKey, out keys))
            {
                keys = new List<string>();
                scopeHistory[experimentKey] = keys;
            }

            if (!keys.Contains(subjectFactKey))
            {
                keys.Add(subjectFactKey);
            }
        }

        /// <summary>The in-memory half of the retirement, run by every
        /// durable sync of one experiment's entry: when the serving entry
        /// synced last time carried another key (a republish installed a
        /// rotated key) or the entry is gone (any drop, the kill switch
        /// included), that key leaves the entries here, before the write
        /// that lands the change carries the history.</summary>
        private void RetireSyncedFactKeyLocked(string scope, string experimentKey)
        {
            ShardPilotExperimentEntry entry;
            _entries.TryGetValue(experimentKey, out entry);
            var current = entry != null ? entry.SubjectFactKey : null;
            KeyValuePair<string, string> synced;
            if (_syncedFactKeys.TryGetValue(experimentKey, out synced) &&
                string.Equals(synced.Key, scope, StringComparison.Ordinal) &&
                !string.Equals(synced.Value, current, StringComparison.Ordinal))
            {
                RetireFactKeyLocked(scope, experimentKey, synced.Value);
            }

            if (current != null)
            {
                _syncedFactKeys[experimentKey] = new KeyValuePair<string, string>(scope, current);
            }
            else
            {
                _syncedFactKeys.Remove(experimentKey);
            }
        }

        /// <summary>The "fact_key_history" section a write of
        /// <paramref name="scope"/>'s record carries. The durable half of the
        /// retirement: the record this write replaces is diffed against
        /// <paramref name="entries"/>, and a same-scope entry whose key the
        /// write removes or replaces retires into the history in this very
        /// write, whichever path decided it (an install, any drop, an owed
        /// retry, a fold of other owed intents, a sentinel clear). The
        /// section already on disk is merged in, so a sibling instance's
        /// retirements survive this rewrite, except for a scope whose history
        /// a prune shrank since its record last landed.</summary>
        private Dictionary<string, List<string>> FactKeyHistoryForWriteLocked(
            string scope, Dictionary<string, ShardPilotExperimentEntry> entries)
        {
            Dictionary<string, ShardPilotExperimentEntry> previous;
            Dictionary<string, List<string>> previousHistory;
            var sameScope = string.Equals(
                ReadRecordFactKeyHistoryLocked(out previous, out previousHistory), scope, StringComparison.Ordinal);
            if (sameScope)
            {
                foreach (var pair in previous)
                {
                    var retiring = pair.Value.SubjectFactKey;
                    ShardPilotExperimentEntry next;
                    if (retiring != null &&
                        (!entries.TryGetValue(pair.Key, out next) || next == null ||
                         !string.Equals(next.SubjectFactKey, retiring, StringComparison.Ordinal)))
                    {
                        RetireFactKeyLocked(scope, pair.Key, retiring);
                    }
                }
            }

            var merged = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            Dictionary<string, List<string>> retained;
            if (_factKeyHistory.TryGetValue(scope, out retained))
            {
                foreach (var pair in retained)
                {
                    merged[pair.Key] = new List<string>(pair.Value);
                }
            }

            if (sameScope && !_factKeyHistoryPruned.Contains(scope))
            {
                foreach (var pair in previousHistory)
                {
                    List<string> keys;
                    if (!merged.TryGetValue(pair.Key, out keys))
                    {
                        keys = new List<string>();
                        merged[pair.Key] = keys;
                    }

                    for (var i = 0; i < pair.Value.Count; i++)
                    {
                        if (!keys.Contains(pair.Value[i]))
                        {
                            keys.Add(pair.Value[i]);
                        }
                    }
                }
            }

            return merged;
        }

        /// <summary>The file's record scope, entries and fact-key history,
        /// un-gated like <see cref="RawDurableRecordLocked(out Dictionary{string, ShardPilotExperimentEntry})"/>;
        /// null (with empty maps) when there is no parseable
        /// record.</summary>
        private string ReadRecordFactKeyHistoryLocked(
            out Dictionary<string, ShardPilotExperimentEntry> entries,
            out Dictionary<string, List<string>> factKeyHistory)
        {
            entries = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            factKeyHistory = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            if (string.IsNullOrEmpty(_cachePath))
            {
                return null;
            }

            try
            {
                var text = ReadBoundedRecordTextLocked();
                string recordScope;
                if (text != null && ShardPilotExperimentsLogic.TryParseRecord(
                        text, out recordScope, out entries, out factKeyHistory))
                {
                    return recordScope;
                }
            }
            catch
            {
                // Unreadable = no record, like the other raw reads.
            }

            entries = new Dictionary<string, ShardPilotExperimentEntry>(StringComparer.Ordinal);
            factKeyHistory = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            return null;
        }

        /// <summary>Writes <paramref name="scope"/>'s history when no other
        /// write carries it: a rewrite of the file's own record (or a fresh
        /// one when there is none) with its entries as they are on disk. Best-effort; another scope's record, or a file that
        /// cannot be read, is left alone.</summary>
        private void PersistFactKeyHistoryLocked(string scope)
        {
            Dictionary<string, List<string>> scopeHistory;
            if (!_factKeyHistory.TryGetValue(scope, out scopeHistory) || scopeHistory.Count == 0)
            {
                return;
            }

            lock (StorageGate)
            {
                Dictionary<string, ShardPilotExperimentEntry> onDisk;
                bool unreadable;
                var recordScope = RawDurableRecordLocked(out onDisk, out unreadable);
                if (unreadable || (recordScope != null && !string.Equals(recordScope, scope, StringComparison.Ordinal)))
                {
                    return;
                }

                WriteDurableRecordLocked(scope, onDisk);
            }
        }

        /// <summary>Construction: the history the restored record carries
        /// (read with its entries, see <see cref="ReadDurableEntriesLocked"/>)
        /// becomes this instance's, when the record is this tenant's (the
        /// current subject's, or a retired subject's after a re-mint).
        /// Nothing is pruned here: the client prunes only once it has
        /// restored its spool (<see cref="ArmFactKeyPrune"/>).</summary>
        private void AdoptFactKeyHistoryLocked(
            string scope, string recordScope, Dictionary<string, List<string>> history)
        {
            if (recordScope != null && history.Count > 0 &&
                ShardPilotExperimentsLogic.ScopesShareTenant(recordScope, scope))
            {
                _factKeyHistory[recordScope] = history;
            }
        }

        /// <summary>Called by the client once its spool is restored (or
        /// known to hold nothing): from here on every live fact is in a
        /// holder the prune can see.</summary>
        internal void ArmFactKeyPrune()
        {
            lock (_lock)
            {
                // Every restored fact was registered: the adopted intents
                // were applied, and retire unless their withdrawal could not
                // be made durable.
                if (!_adoptedIntentsOwed)
                {
                    var adopted = _withdrawalIntents.FindAll(pending => pending.Adopted);
                    for (var i = 0; i < adopted.Count; i++) CompleteWithdrawalScanLocked(adopted[i], true);
                }

                _factKeyPruneArmed = true;
            }
        }

        /// <summary>Whether the prune has anything to do: a history or a
        /// withdrawal generation to check, or a shrunk history still to
        /// persist. The client skips its pipeline scan otherwise.</summary>
        internal bool HasFactKeyStateToPrune
        {
            get
            {
                lock (_lock)
                {
                    return _factKeyPruneArmed &&
                           (_factKeyHistory.Count > 0 || _withdrawnKeyGenerations.Count > 0 ||
                            _factKeyHistoryPruned.Count > 0);
                }
            }
        }

        /// <summary>
        /// The liveness prune of the fact-key history and of the per-key
        /// withdrawal generations. <paramref name="pipelineLive"/> holds the
        /// <see cref="FactKeyToken"/> of every experiment fact the client's
        /// pipeline holds: queued, restored from the spool, or in the
        /// worker's held or in-flight batch. The client calls this UNDER ITS
        /// GATE (gate → consumer lock, the existing order), so no fact
        /// enters the queue between that collection and this pass; a fact
        /// between its source read and the end of its enqueue counts through
        /// <see cref="_factKeysInEmission"/>. The owed records are this
        /// consumer's own holders: owed exposure snapshots. A key no live fact carries goes from the history (the
        /// scope is marked for <see cref="FlushPrunedFactKeyHistory"/>) and
        /// from the withdrawal generations. There is no count bound: the
        /// pipeline's own bounds (queue, spool, batch) bound what can stay.
        /// </summary>
        internal void PruneFactKeyState(HashSet<string> pipelineLive)
        {
            lock (_lock)
            {
                if (!_factKeyPruneArmed || (_factKeyHistory.Count == 0 && _withdrawnKeyGenerations.Count == 0))
                {
                    return;
                }

                var live = new HashSet<string>(pipelineLive ?? new HashSet<string>(), StringComparer.Ordinal);
                foreach (var token in _factKeysInEmission.Keys)
                {
                    live.Add(token);
                }
                var cache = WithdrawalRegistryPathLocked();
                var scope = CurrentScopeOrNullLocked();
                lock (StaticGate)
                    foreach (var emission in FactEmissions)
                        if (emission.Cache == cache && ShardPilotExperimentsLogic.ScopesShareTenant(emission.Scope, scope))
                            foreach (var key in emission.FactKeys) live.Add(FactKeyToken(emission.Experiment, key));

                foreach (var pair in _pendingExposure)
                {
                    for (var i = 0; i < pair.Value.Count; i++)
                    {
                        var entry = pair.Value[i].Entry;
                        if (entry != null && entry.SubjectFactKey != null)
                        {
                            live.Add(FactKeyToken(pair.Key, entry.SubjectFactKey));
                        }
                    }
                }

                // A serving entry whose own durable write is still owed (a
                // record the entry does not fit, say) is on disk only
                // through the history: its key stays for as long as it
                // serves, since every fact built meanwhile carries it.
                foreach (var pending in _durablePending.Values)
                {
                    ShardPilotExperimentEntry serving;
                    if (!pending.Drop && _entries.TryGetValue(pending.ExperimentKey, out serving) &&
                        serving.SubjectFactKey != null)
                    {
                        live.Add(FactKeyToken(pending.ExperimentKey, serving.SubjectFactKey));
                    }
                }

                var scopes = new List<string>(_factKeyHistory.Keys);
                for (var s = 0; s < scopes.Count; s++)
                {
                    var scopeHistory = _factKeyHistory[scopes[s]];
                    var experimentKeys = new List<string>(scopeHistory.Keys);
                    for (var e = 0; e < experimentKeys.Count; e++)
                    {
                        var keys = scopeHistory[experimentKeys[e]];
                        var experimentKey = experimentKeys[e];
                        if (keys.RemoveAll(key => !live.Contains(FactKeyToken(experimentKey, key))) > 0)
                        {
                            _factKeyHistoryPruned.Add(scopes[s]);
                        }

                        if (keys.Count == 0)
                        {
                            scopeHistory.Remove(experimentKey);
                        }
                    }

                    if (scopeHistory.Count == 0)
                    {
                        _factKeyHistory.Remove(scopes[s]);
                    }
                }

                var withdrawn = new List<string>(_withdrawnKeyGenerations.Keys);
                for (var e = 0; e < withdrawn.Count; e++)
                {
                    var generations = _withdrawnKeyGenerations[withdrawn[e]];
                    var keys = new List<string>(generations.Keys);
                    for (var i = 0; i < keys.Count; i++)
                    {
                        if (!live.Contains(FactKeyToken(withdrawn[e], keys[i])))
                        {
                            generations.Remove(keys[i]);
                        }
                    }

                    if (generations.Count == 0)
                    {
                        _withdrawnKeyGenerations.Remove(withdrawn[e]);
                    }
                }
            }
        }

        /// <summary>Persists a history the prune shrank. A SCOPE-PRESERVING
        /// rewrite: the file's record keeps its scope and entries exactly as they are on disk, and
        /// only its "fact_key_history" section changes. Runs outside the
        /// client's gate; a failed write stays owed and the next prune
        /// retries it.</summary>
        internal void FlushPrunedFactKeyHistory()
        {
            lock (_lock)
            {
                // Handoff transfers constructive cache maintenance to the
                // successor; the predecessor only drains its captured facts.
                if (_automaticLaneDisowned) return;
                if (_factKeyHistoryPruned.Count == 0)
                {
                    return;
                }

                if (string.IsNullOrEmpty(_cachePath))
                {
                    _factKeyHistoryPruned.Clear();
                    return;
                }

                lock (StorageGate)
                {
                    Dictionary<string, ShardPilotExperimentEntry> stored;
                    bool unreadable;
                    var recordScope = RawDurableRecordLocked(out stored, out unreadable);
                    if (unreadable)
                    {
                        return;
                    }

                    // Only the record the file holds carries a history on
                    // disk; a pruned scope without one has nothing to rewrite.
                    if (recordScope != null && _factKeyHistoryPruned.Contains(recordScope) &&
                        !WriteDurableRecordLocked(recordScope, stored))
                    {
                        return;
                    }

                    _factKeyHistoryPruned.Clear();
                }
            }
        }

        /// <summary>Drain one experiment's owed-exposure queue in order:
        /// emitted and terminally skipped snapshots leave the queue; a
        /// retryable failure (queue full, consent closed) stops the drain
        /// and keeps the remainder armed, so an older application's fact is
        /// never leapfrogged or lost.</summary>
        private void SweepPendingSerialized(string experimentKey)
        { SweepApplications(true, experimentKey); }

        /// <summary>Advance captured applications in FIFO order. Queue pressure retains the same sealed fact for a later tick.</summary>
        internal void SweepOwed()
        { SweepApplications(true); }

        /// <summary>Commit an application domain for a new analytics lifecycle; retain facts captured in earlier lifecycles.</summary>
        internal void OnSessionRenewed(
            bool isRenewal, string previousSessionId = null, string adoptedSessionId = null)
        {
            lock (_lock)
            {
                // A DIRECT (in-gate, non-staged) renewal supersedes every
                // stage still pending with an earlier stake: their session
                // transitions were superseded by this one, and a late
                // staged commit must not rotate the marker backward past a
                // renewal that already ran.
                _renewalStakeCommitted = ++_renewalStakeSeq;
                OnSessionRenewedCore(isRenewal, previousSessionId, adoptedSessionId);
            }
        }

        /// <summary>Commit a new application domain without changing already-captured facts.</summary>
        private void OnSessionRenewedCore(
            bool isRenewal, string previousSessionId, string adoptedSessionId)
        {
            lock (_lock)
            {
                _terminalAssignments.Clear();
                _sessionMarker = ShardPilotIds.NewUuidV7();
                _lastApplications.Clear();
                _appliedTuples.Clear();
            }
        }

        /// <summary>Stages a session-start renewal whose acceptance is not
        /// yet known: the client's overflow-to-spool session start commits
        /// its session transition BEFORE the provisional spool append
        /// confirms, and a refused append rolls that transition back. The
        /// renewal itself is therefore DEFERRED — nothing rotates, adopts,
        /// or re-arms here — while the latch (a) pauses owed sweeps and (b)
        /// serves the PRE-start session id to every capture, so no
        /// interleaving can observe the pending session with the old marker
        /// (the ordering the committed in-gate renewal otherwise
        /// guarantees) and no consumer state can ever name a session the
        /// SDK then refuses. Confirmed by
        /// <see cref="CommitProvisionalSessionRenewal"/>, undone without a
        /// trace by <see cref="AbandonProvisionalSessionRenewal"/> — each
        /// taking the STAKE returned here, so an operation settles only the
        /// stage it owns.</summary>
        internal long BeginProvisionalSessionRenewal(
            bool isRenewal, string previousSessionId, string pendingSessionId)
        {
            lock (_lock)
            {
                var stake = ++_renewalStakeSeq;
                _provisionalRenewals.Add(new ProvisionalRenewal
                {
                    Stake = stake,
                    IsRenewal = isRenewal,
                    PreviousSessionId = previousSessionId,
                    PendingSessionId = pendingSessionId
                });
                return stake;
            }
        }

        /// <summary>The staked session start was accepted (its overflow
        /// append confirmed, or another event bound to its session so its
        /// rollback was skipped): run the real renewal now, with the staged
        /// identities. OWNERSHIP-VERIFIED: only the stage this stake
        /// created commits — stake 0 (an ordinary overflow event that
        /// staged nothing) and a foreign stake leave other operations'
        /// pending stages to their own append verdicts, where the
        /// unconditional legacy commit landed a NEIGHBOR'S stage before its
        /// verdict. A stage whose stake a LATER renewal already superseded
        /// is dropped without running: its transition is history, and
        /// rotating the marker backward would re-arm exposures under a
        /// session the client has moved past. No-op when this stake has no
        /// stage.</summary>
        internal void CommitProvisionalSessionRenewal(long stake)
        {
            // Unlatch and renew in ONE lock hold (the inner renewal lock is
            // a reentrant no-op): were the latch dropped first, a sweep
            // could slip into the gap and observe the new session with the
            // old marker — the exact window the staged commit exists to
            // keep closed.
            lock (_lock)
            {
                var staged = TakeStagedRenewalLocked(stake);
                if (staged == null || staged.Stake <= _renewalStakeCommitted)
                {
                    return;
                }

                _renewalStakeCommitted = staged.Stake;
                OnSessionRenewedCore(staged.IsRenewal, staged.PreviousSessionId, staged.PendingSessionId);
            }
        }

        /// <summary>The staked session start was REFUSED (failed or raced
        /// provisional append, rolled back by the client): drop THIS
        /// OPERATION'S stage — and only it. The rejected operation's
        /// renewal never ran, so its stage vanishes without a trace, while
        /// a concurrent session start's still-pending stage survives for
        /// its own append verdict — the unconditional legacy abandon killed
        /// the neighbor's stage here, and the neighbor's accepted session
        /// then never renewed (stale-marker exposure attribution). No-op
        /// when this stake has no stage (0 = staged nothing).</summary>
        internal void AbandonProvisionalSessionRenewal(long stake)
        {
            lock (_lock)
            {
                TakeStagedRenewalLocked(stake);
            }
        }

        /// <summary>Removes and returns the stage <paramref name="stake"/>
        /// owns, or null (0 never owns one). Caller holds
        /// <see cref="_lock"/>.</summary>
        private ProvisionalRenewal TakeStagedRenewalLocked(long stake)
        {
            if (stake <= 0)
            {
                return null;
            }

            for (var i = 0; i < _provisionalRenewals.Count; i++)
            {
                if (_provisionalRenewals[i].Stake == stake)
                {
                    var staged = _provisionalRenewals[i];
                    _provisionalRenewals.RemoveAt(i);
                    return staged;
                }
            }

            return null;
        }

        /// <summary>True while any session-start renewal is STAGED (an
        /// overflow commit awaiting its append verdict). The client's fact
        /// enqueue consults this under its own gate: an explicit
        /// exposure/outcome emitted in the staged window resolves an EMPTY
        /// envelope session id on a FIRST session (there is no pre-start
        /// session the latch could serve), and the enqueue-side lazy attach
        /// would bind it to the PENDING client session — a session the SDK
        /// may yet refuse. Such facts are refused (retryably) instead; owed
        /// sweeps are already latched by <see cref="SweepOwed"/>.</summary>
        internal bool HasStagedSessionRenewal()
        {
            lock (_lock)
            {
                return _provisionalRenewals.Count > 0;
            }
        }

        /// <summary>Discard applications and accepted-fact indexes after consent denial. A later grant requires a fresh explicit application.</summary>
        internal void OnAnalyticsPurge()
        {
            lock (_lock)
            {
                // The denial withdrew every pending session start. Drop the
                // whole latch before another application can be recorded: otherwise
                // CurrentAnalyticsSessionId would capture the pre-denial
                // session from a stage whose append is still settling.
                _provisionalRenewals.Clear();

                // Unadmitted applications never cross a consent boundary.
                _pendingExposure.Clear();
                _lastApplications.Clear();
                _appliedTuples.Clear();

                // The purge just emptied the analytics pipeline of
                // experiment facts (queue, restored, and the epoch-doomed
                // held batch alike): nothing those indexes cover is
                // pipeline-resident anymore. Purged ids left behind would
                // consume the bounded maps and evict LATER live ids — a
                // subsequent real-subjects sentinel could then miss facts
                // that are actually queued. Transport handoffs are tracked
                // by the analytics pipeline, which owns retries of the same seal.
                _factScopes.Clear();
                _factScopeOrder.Clear();

            }
        }

        /// <summary>Release delivered or server-rejected facts from the condemnation index.</summary>
        internal void OnFactsDelivered(List<string> eventIds)
        {
            if (eventIds == null || eventIds.Count == 0)
            {
                return;
            }

            lock (_lock)
            {
                for (var i = 0; i < eventIds.Count; i++)
                {

                    // Delivered = confirmed OUT of the pipeline: nothing
                    // remains for a condemnation to withdraw, so the
                    // scope index evicts the id eagerly (keeping the
                    // bounded map covering only pipeline-resident facts).
                    _factScopes.Remove(eventIds[i]);
                }
            }
        }

        /// <summary>Release terminally dropped facts from the condemnation index; client accounting owns the drop count.</summary>
        internal void OnFactsSettledWithoutDelivery(List<string> eventIds)
        {
            if (eventIds == null || eventIds.Count == 0)
            {
                return;
            }

            lock (_lock)
            {
                for (var i = 0; i < eventIds.Count; i++)
                {
                    _factScopes.Remove(eventIds[i]);
                }
            }
        }

        // ── fetch, install, revalidation ─────────────────────────────────────

        /// <summary>
        /// Fetches the assignment for one experiment.
        /// <paramref name="attributes"/> is the optional server-evaluated
        /// targeting attribute set; a live owner queues the callback on its
        /// captured host thread, once, with the
        /// outcome of ITS OWN fetch. Host-triggered fetches always run;
        /// while the plane is consent-closed or torn down the refusal is
        /// reported through the callback. No captured host or retired owner
        /// means false with no public callback. Returns true when a request was
        /// dispatched.
        /// </summary>
        public bool FetchExperimentAssignment(
            string experimentKey,
            IDictionary<string, string> attributes,
            Action<ShardPilotExperimentResult> callback)
        {
            if (ReferenceEquals(_callbackHost, null) || !_callbackHost.IsLive || !_hostCurrent()) return false;
            var epoch = ConsentEpochNow();
            long authEpoch; lock (_lock) authEpoch = _authEpoch;
            var scope = string.Empty; var settled = 0L; var claimed = 0;
            var publish = _callbackHost.WrapCallback(callback,
                () => _hostCurrent() && !Volatile.Read(ref _automaticLaneDisowned), result =>
            {
                lock (_lock)
                {
                    if (!result.Ok) return result;
                    var refusal = ConsentRefusal(_consent());
                    if (refusal != null || ConsentEpochNow() != epoch) return ShardPilotExperimentResult.Failure(refusal ?? "consent_changed");
                    GatesNowLocked(settled, scope, experimentKey, authEpoch, out var authorized, out var sameScope, out var fenced);
                    if (!authorized || !sameScope) return ShardPilotExperimentResult.Failure(authorized ? "stale_subject" : "unauthorized");
                    var entry = LoadServableEntryLocked(scope, experimentKey);
                    var published = fenced ? ShardPilotExperimentsLogic.ServeEntryOrFail(entry, "superseded", 0).Result : result;
                    if (published.Assigned && DeclarationWithholdsResultLocked(experimentKey, settled, entry, published, out var code))
                        return ShardPilotExperimentResult.Failure(code);
                    return published;
                }
            });
            return Fetch(experimentKey, attributes, result =>
            {
                if (Interlocked.Exchange(ref claimed, 1) != 0) return;
                scope = ScopeFor(CurrentSubjectId());
                lock (StaticGate) settled = SettledLocked(scope, experimentKey);
                publish?.Invoke(result);
            }, false, null);
        }

        private static void Finish(Action<ShardPilotExperimentResult> callback, ShardPilotExperimentResult result)
        {
            if (callback == null)
            {
                return;
            }

            try
            {
                callback(result);
            }
            catch
            {
                // The callback is game code; never let it break the SDK.
            }
        }

        // ── a host's non-adult age declaration ────────────────────────────────

        /// <summary>
        /// Records a host fetch's age declaration for the experiment. A
        /// non-adult declaration stops the experiment serving at once and
        /// keeps the cadence from re-sending the earlier declaration while its
        /// fetch is in flight; what the subject owes for the experiment waits,
        /// unswept, for the server's answer. An adult declaration replaces a pending
        /// non-adult one (the later declaration decides afresh); a fetch that
        /// declares no age changes nothing. Never mints a subject. Returns
        /// true on admission, with a non-adult declaration or null. Returns
        /// false after close begins, recording no declaration or sequence.
        /// </summary>
        private bool TryBeginHostDeclaration(
            string experimentKey, IDictionary<string, string> attributes, out NonAdultDeclaration declaration, out long sequence)
        {
            var nonAdult = ShardPilotExperimentsLogic.DeclaresNonAdultAge(attributes);
            var adult = !nonAdult && ShardPilotExperimentsLogic.DeclaresAdultAge(attributes);
            declaration = null;
            sequence = 0;
            HostDeclarationHookForTesting?.Invoke();
            lock (_lock)
            {
                // Close seals admission and snapshots pending declarations
                // under this lock. A fetch past its earlier check must not
                // record a new declaration after that abort barrier.
                if (_tornDown || _closingApplications) return false;
                long fence;
                lock (StaticGate)
                {
                    fence = nonAdult || (adult && !_automaticLaneDisowned) ? ++_fetchSeq : 0;
                    sequence = ++_fetchSeq;
                }

                if (!nonAdult && !adult) return true;
                if (!nonAdult)
                {
                    // An adult call decides afresh by call order: it fences
                    // every earlier fetch of the key in every subject scope,
                    // so a re-mint landing at any point cannot carry an older
                    // non-adult answer or its grammar retry past it. A
                    // replaced client fences nothing: the fences are its
                    // successor's now.
                    _declaring.Remove(experimentKey);
                    if (fence == 0) return true;
                    var fenceScope = ScopeFor(null);
                    AdultFenceHookForTesting?.Invoke();
                    lock (StaticGate)
                    {
                        var fenceKey = FenceKey(fenceScope, experimentKey); long settled;
                        Settled.TryGetValue(fenceKey, out settled);
                        if (settled < fence) Settled[fenceKey] = fence;
                    }
                    return true;
                }

                // The reserved sequence sits above every fetch already
                // declared (including those still in preflight) and below
                // this fetch's own, so raising
                // the key's fence to it later fences out exactly the fetches
                // that left with an earlier declaration (install and
                // callback reads alike).
                declaration = new NonAdultDeclaration();
                ShardPilotExperimentEntry live;
                _entries.TryGetValue(experimentKey, out live);
                var declaredSubject = CurrentSubjectId();
                var declaredScope = declaredSubject == null ? null : ScopeFor(declaredSubject);
                declaration.Scope = declaredScope;
                if (declaredSubject != null && live?.SubjectKey == declaredSubject)
                    AddValidFactKey(declaration.FactKeys, live.SubjectFactKey);
                if (declaredScope != null)
                {
                    Dictionary<string, List<string>> history;
                    List<string> keys;
                    if (_factKeyHistory.TryGetValue(declaredScope, out history) && history.TryGetValue(experimentKey, out keys))
                        for (var i = 0; i < keys.Count; i++) AddValidFactKey(declaration.FactKeys, keys[i]);
                    KeyValuePair<string, string> synced;
                    if (_syncedFactKeys.TryGetValue(experimentKey, out synced) && synced.Key == declaredScope)
                        AddValidFactKey(declaration.FactKeys, synced.Value);
                    List<OwedExposure> pending;
                    if (_pendingExposure.TryGetValue(experimentKey, out pending))
                        for (var i = 0; i < pending.Count; i++)
                            if (pending[i].Entry?.SubjectKey == declaredSubject)
                                AddValidFactKey(declaration.FactKeys, pending[i].Entry.SubjectFactKey);
                }

                declaration.FenceSeq = fence;

                _declaring[experimentKey] = declaration;
                return true;
            }
        }

        /// <summary>The declaring fetch's callback, preceded by the
        /// declaration's end: the caller sees the state the declaration
        /// left.</summary>
        private Action<ShardPilotExperimentResult> EndDeclarationBeforeCallback(
            string experimentKey, NonAdultDeclaration declaration, Action<ShardPilotExperimentResult> callback)
        {
            return result =>
            {
                try
                {
                    EndNonAdultDeclaration(experimentKey, declaration);
                }
                finally
                {
                    if (callback != null)
                    {
                        callback(result);
                    }
                }
            };
        }

        /// <summary>The server's answer to a pending non-adult declaration's
        /// own fetch was applied to the assignment state: that answer governs
        /// (the age refusal withdraws, a kill switch keeps what is owed), and
        /// the declaration's end adds nothing. Caller holds
        /// <see cref="_lock"/>.</summary>
        private void DecideDeclarationLocked(string experimentKey, long seq)
        {
            NonAdultDeclaration pending;
            if (_declaring.TryGetValue(experimentKey, out pending) && pending.Seq == seq)
            {
                pending.Decided = true;
            }
        }

        /// <summary>Whether the assignment is subject to the age rule: a
        /// client-id assignment is; a synthetic-subject assignment is not
        /// (the platform assigns it whatever age the host declares).</summary>
        private static bool AgeGated(ShardPilotExperimentEntry entry)
        {
            return !string.Equals(entry.AssignmentUnit, "synthetic_subject_key", StringComparison.Ordinal);
        }

        /// <summary>A pending non-adult declaration hands no caller an
        /// age-gated variant: not another fetch's, whatever its answer
        /// installed or served from cache, and not the declaring fetch's own
        /// cached fallback, which its end is about to withdraw. Only the
        /// declaring fetch's applied answer is served. <paramref name="source"/>
        /// is the entry the result came from (the installed or the cached
        /// one). The refusal keeps the declaring fetch's own error code (its
        /// transient's); another fetch's is <c>superseded</c>. Caller holds
        /// <see cref="_lock"/>.</summary>
        private bool DeclarationWithholdsResultLocked(
            string experimentKey, long seq, ShardPilotExperimentEntry source, ShardPilotExperimentResult result,
            out string code)
        {
            code = "superseded";
            if (_launchUndecided.Contains(experimentKey) && (source == null || AgeGated(source)))
            {
                code = "age_declaration_pending";
                return true;
            }
            NonAdultDeclaration pending;
            if (!_declaring.TryGetValue(experimentKey, out pending) || (pending.Seq == seq && pending.Decided))
            {
                return false;
            }

            if (source != null && !AgeGated(source))
            {
                return false;
            }

            if (pending.Seq == seq && !string.IsNullOrEmpty(result.Error))
            {
                code = result.Error;
            }

            return true;
        }

        /// <summary>
        /// Ends a host's non-adult declaration when its fetch resolves, on
        /// every path. Unless the server's answer was applied, the server has
        /// not confirmed it (a transient failure, an auth latch, a consent
        /// refusal, a fenced-out or stale-scope answer), and the declaration
        /// alone makes the subject ineligible for a client-id assignment, so
        /// it is not held for a later answer: the key's fence rises past
        /// every fetch dispatched before the declaration, so an adult answer
        /// or revalidation that left earlier installs nothing when it lands,
        /// and the age refusal's own withdrawal runs for the current subject
        /// as if the server had answered age_ineligible with no fact key of
        /// its own — the entry stops serving and its durable copy goes, and
        /// every application the subject owes for the experiment is
        /// withdrawn with its accepted facts. A synthetic-subject assignment
        /// (no age gate) keeps serving and keeps its durable copy; the fence
        /// still rises and the owed applications are still withdrawn. A later
        /// declaration that replaced this one, a teardown, a disowned client
        /// (whose writes the successor owns) or no current subject withdraws
        /// nothing.
        /// </summary>
        private void EndNonAdultDeclaration(string experimentKey, NonAdultDeclaration declaration)
        {
            WithdrawnApplications withdrawal;
            lock (_lock)
            {
                NonAdultDeclaration pending;
                if (!_declaring.TryGetValue(experimentKey, out pending) || !ReferenceEquals(pending, declaration))
                {
                    return;
                }

                _declaring.Remove(experimentKey);
                if (declaration.Decided || _tornDown || _automaticLaneDisowned)
                {
                    return;
                }

                var subject = CurrentSubjectId();
                if (subject == null)
                {
                    return;
                }

                var scope = declaration.Scope ?? ScopeFor(subject);
                lock (StaticGate)
                {
                    var currentFence = FenceKey(ScopeFor(subject), experimentKey); long settled;
                    Settled.TryGetValue(currentFence, out settled);
                    if (settled < declaration.FenceSeq) Settled[currentFence] = declaration.FenceSeq;
                }

                // The experiment's assignment as the end sees it: the live
                // entry, or the durable copy an auth latch retained after
                // clearing it from memory. That copy names the subject's fact
                // key, which the withdrawal needs to find the accepted facts.
                ShardPilotExperimentEntry live;
                ShardPilotExperimentEntry retained = null;
                if (!_entries.TryGetValue(experimentKey, out live))
                {
                    RawDurableEntriesLocked(scope).TryGetValue(experimentKey, out retained);
                }

                lock (StaticGate)
                {
                    var fenceKey = FenceKey(scope, experimentKey);
                    long settled;
                    Settled.TryGetValue(fenceKey, out settled);
                    if (settled < declaration.FenceSeq)
                    {
                        Settled[fenceKey] = declaration.FenceSeq;
                    }
                }

                // A synthetic-subject assignment has no age gate: it keeps
                // serving, and its entry and durable copy stay. The
                // applications the subject still owes for the experiment
                // from before it are withdrawn all the same.
                var assignment = live ?? retained;
                var keepSynthetic = assignment != null && !AgeGated(assignment);
                var retainedFactKey = retained != null &&
                                      ShardPilotExperimentsLogic.ValidSubjectFactKey(retained.SubjectFactKey)
                    ? retained.SubjectFactKey
                    : null;
                withdrawal = WithdrawRefusedApplicationsLocked(
                    scope, experimentKey, retainedFactKey, NowUtc(), keepSynthetic, declaration.FactKeys);
            }

            // Outside the lock, like the refusal's own: the withdrawer takes
            // the client's gate.
            InvokeKeyFactWithdrawal(experimentKey, withdrawal);
        }

        /// <summary>
        /// Ends every pending non-adult declaration as a fetch that ends
        /// unanswered does. The client calls this when a close is
        /// submitted, BEFORE the teardown: the shutdown abandons the fetches
        /// still in flight (a torn-down consumer discards their answers, and
        /// its declarations' ends withdraw nothing), so each declaration is
        /// aborted, not answered, and its withdrawal and fence land durably
        /// now, before the close's final flush and persist barrier. A
        /// declaration whose answer was already applied withdraws nothing.
        /// Caller holds no SDK lock (the withdrawal takes the client's gate).
        /// </summary>
        internal void AbortDeclarationsForClose()
        {
            List<KeyValuePair<string, NonAdultDeclaration>> pending;
            lock (_lock)
            {
                if (_declaring.Count == 0)
                {
                    return;
                }

                pending = new List<KeyValuePair<string, NonAdultDeclaration>>(_declaring);
            }

            pending.Sort((left, right) => string.CompareOrdinal(left.Key, right.Key));
            for (var i = 0; i < pending.Count; i++)
            {
                EndNonAdultDeclaration(pending[i].Key, pending[i].Value);
            }
        }

        private bool Fetch(
            string experimentKey,
            IDictionary<string, string> attributes,
            Action<ShardPilotExperimentResult> callback,
            bool isRevalidation,
            List<KeyValuePair<string, string>> presetAttributes,
            NonAdultDeclaration declaration = null,
            long? retrySequence = null)
        {
            if (string.IsNullOrEmpty(experimentKey))
            {
                Finish(callback, ShardPilotExperimentResult.Failure("experiment_key_required"));
                return false;
            }

            lock (_lock)
            {
                if (_tornDown || _closingApplications)
                {
                    Finish(callback, ShardPilotExperimentResult.Failure("shutdown"));
                    return false;
                }
            }

            // A client a newer one replaced serves nothing (#155), so a host
            // fetch on it hands its caller no variant either (#160): the
            // host would apply a treatment whose exposure this client then
            // refuses. Nothing is dispatched and nothing is declared.
            bool replaced;
            lock (_lock)
            {
                replaced = !isRevalidation && _automaticLaneDisowned;
            }

            if (replaced)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(ReplacedCode));
                return false;
            }

            if (!BindCacheIdentity(true))
            {
                Finish(callback, ShardPilotExperimentResult.Failure(ConsentRefusal(_consent()) ?? "storage_unavailable"));
                return false;
            }
            if (_withdrawalMetadataUncertain) DiscoverWithdrawals();

            // A HOST fetch's age declaration takes effect when it is made,
            // before any consent gate: a non-adult declaration made while the
            // plane refuses still withdraws when its fetch ends, or a later
            // re-grant would serve the assignment the host has declared the
            // subject ineligible for. Its end runs before the caller's
            // callback, on every path that resolves this fetch. The grammar
            // re-mint retry carries the declaration it rides instead.
            // A retry belongs to the originating call: allocating again
            // would promote an older declaration above a newer one's fence.
            long seq;
            if (retrySequence.HasValue)
            {
                seq = retrySequence.Value;
            }
            else if (!isRevalidation && presetAttributes == null)
            {
                if (!TryBeginHostDeclaration(experimentKey, attributes, out declaration, out seq))
                {
                    Finish(callback, ShardPilotExperimentResult.Failure("shutdown"));
                    return false;
                }
                if (declaration != null)
                {
                    callback = EndDeclarationBeforeCallback(experimentKey, declaration, callback);
                }
            }
            else
            {
                lock (StaticGate) seq = ++_fetchSeq;
            }

            // GRANTED-ONLY plane: while consent is unknown or denied — the
            // forced-minor state included — no request leaves the device,
            // nothing is minted, nothing is served. The consent-denial
            // EPOCH is captured with this dispatch-intent read: this path
            // is not serialized with the client's consent gate (the
            // consumer must never take the client's lock), so every later
            // stage re-checks state AND epoch — a denial that lands in the
            // window resolves the fetch closed even when a quick re-grant
            // has already returned the current state to granted.
            var consentEpochAtIntent = ConsentEpochNow();
            var refusal = ConsentRefusal(_consent());
            if (refusal != null)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(refusal));
                return false;
            }

            var raceHook = ConsentRaceHookForTesting;
            if (raceHook != null)
            {
                raceHook();
            }

            // PRE-MINT consent gate: a revocation racing the intent read
            // above must not mint a subject id — the granted-only contract
            // covers the mint itself, not just the wire.
            var preMintRefusal = ConsentRefusal(_consent());
            if (preMintRefusal == null && ConsentEpochNow() != consentEpochAtIntent)
            {
                preMintRefusal = "consent_changed";
            }

            if (preMintRefusal != null)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(preMintRefusal));
                return false;
            }

            // Test seam: lets a test land a denial between the unlocked
            // pre-mint gate above and the egress-gated mint commit below.
            // No-op in production.
            var mintHook = PreMintHookForTesting;
            if (mintHook != null)
            {
                mintHook();
            }

            // The MINT COMMIT is serialized with consent decisions exactly
            // like the transport handoff below: the pre-mint gate above is
            // unlocked, so a denial landing between it and the mint would
            // otherwise still mint AND PERSIST a fresh spcid_ under denied
            // consent — no wire traffic, but minted durable state violates
            // the granted-only contract (non-granted states mint nothing).
            // The denial epoch is re-read INSIDE the egress gate immediately
            // before the mint, and the mint itself commits inside the same
            // hold, so after a denial's epoch advance is observable nothing
            // mints. TEARDOWN is re-read in the same hold: a CloseAsync
            // submitted after the early _tornDown check (the client tears
            // the consumer down at close SUBMISSION) must not let this
            // fetch mint and persist a fresh spcid_ after shutdown began —
            // the epoch alone cannot see it, a close advances no consent
            // epoch. Ordering: egress gate → mint gate → consumer lock; the
            // denial path holds only the client gate before this one, and
            // the mint path never reaches back into client locks (this
            // torn-state read takes only the consumer's own lock, the
            // documented innermost position).
            string subject = null;
            var tornAtMint = false;
            lock (_egressGate)
            {
                lock (_lock)
                {
                    tornAtMint = _tornDown || _closingApplications;
                }

                if (!tornAtMint && ConsentEpochNow() == consentEpochAtIntent)
                {
                    subject = EnsureSubjectId();
                }
            }

            if (subject == null)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(
                    tornAtMint ? "shutdown" : "consent_changed"));
                return false;
            }

            var scope = ScopeFor(subject);

            long authEpoch;
            ShardPilotExperimentEntry entry;
            ShardPilotExperimentEntry heldForCadence = null;
            var cadenceYieldsToDeclaration = false;
            lock (_lock)
            {
                authEpoch = _authEpoch;
                entry = LoadServableEntryLocked(scope, experimentKey);

                // The CADENCE reads its remembered targeting context from
                // the RAW held entry, not the servable read: a sibling
                // instance after a real-subjects sentinel still holds the
                // cached assignment while the condemnation suppresses
                // SERVING — its revalidation must keep re-asking with the
                // last host-supplied attributes, or a re-enabled targeted
                // experiment answers targeting_unmatched to an un-targeted
                // probe and drops. Serving suppression is untouched: the
                // raw entry feeds ONLY the request's attribute set.
                if (isRevalidation)
                {
                    _entries.TryGetValue(experimentKey, out heldForCadence);

                    // A pending non-adult declaration stops the cadence: the
                    // held entry's remembered attributes declare the earlier
                    // age, and so does the exact set a revalidation's grammar
                    // re-mint retry carries. (A host fetch's own retry carries
                    // its declaration and proceeds.)
                    cadenceYieldsToDeclaration = _declaring.ContainsKey(experimentKey);
                }

                NonAdultDeclaration pending;
                if (declaration != null && _declaring.TryGetValue(experimentKey, out pending) &&
                    ReferenceEquals(pending, declaration))
                {
                    declaration.Seq = seq;
                }
            }

            if (cadenceYieldsToDeclaration)
            {
                Finish(callback, ShardPilotExperimentResult.Failure("age_declaration_pending"));
                return false;
            }

            List<KeyValuePair<string, string>> normalized;
            if (presetAttributes != null)
            {
                // Internal grammar-remint retry only: the EXACT normalized
                // set of the rejected request rides the retry verbatim — the
                // subject rotation cleared the cached entry the cadence
                // would have re-read them from, and a targeted assignment
                // must be retried with the same input set, not un-targeted.
                normalized = presetAttributes;
            }
            else if (attributes != null)
            {
                int dropped;
                normalized = ShardPilotExperimentsLogic.NormalizeExperimentAttributes(attributes, out dropped);
                if (dropped > 0)
                {
                    Diagnose("attributes_dropped");
                }
            }
            else if (isRevalidation && heldForCadence != null && heldForCadence.Attributes != null)
            {
                // A revalidation re-sends the attributes of the last
                // host-supplied fetch for this experiment (one targeting
                // vocabulary, one value set), so a server-evaluated
                // condition keeps seeing the same subject it matched before
                // — read from the RAW held entry, so a condemned-scope
                // sibling's cadence stays targeted while its serving stays
                // suppressed. Only the CADENCE reuses them: a
                // host-triggered fetch that omits attributes means what it
                // says — it sends none, and none become the entry's
                // remembered set.
                normalized = heldForCadence.Attributes;
            }
            else
            {
                normalized = new List<KeyValuePair<string, string>>();
            }

            if (_sender == null)
            {
                // No transport on this runtime is a transient failure like
                // any other: serve the last-known-good assignment when one
                // exists AND this fetch asked for the same targeting
                // context. Nothing installs and nothing settles the fence.
                var servable = entry != null &&
                    ShardPilotExperimentsLogic.AttributeSetsMatch(normalized, entry.Attributes)
                    ? entry
                    : null;
                Finish(callback, ShardPilotExperimentsLogic.ServeEntryOrFail(servable, "http_unavailable", 0).Result);
                return false;
            }

            var query = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("app_key", _config.AppKey ?? string.Empty),
                new KeyValuePair<string, string>("environment_key", _config.EnvironmentKey ?? string.Empty),
                new KeyValuePair<string, string>("experiment_key", experimentKey),
                new KeyValuePair<string, string>("subject_key", subject)
            };
            for (var i = 0; i < normalized.Count; i++)
            {
                query.Add(normalized[i]);
            }

            var request = new ShardPilotHttpRequest
            {
                Method = "GET",
                Url = ShardPilotExperimentsLogic.BuildUrl(_config.AppUrl, query),
                Bearer = _config.PublishableKey,
                Timeout = _config.HttpTimeout
            };

            // PRE-DISPATCH consent gate: an UNLOCKED re-check with the
            // precise refusal vocabulary. A denial (or a full deny→re-grant,
            // seen through the moved epoch) that landed since the intent
            // read resolves locally — closed result, zero wire traffic.
            var preDispatchRefusal = ConsentRefusal(_consent());
            if (preDispatchRefusal == null && ConsentEpochNow() != consentEpochAtIntent)
            {
                preDispatchRefusal = "consent_changed";
            }

            if (preDispatchRefusal != null)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(preDispatchRefusal));
                return false;
            }

            // Test seam: lets a test land a denial between the unlocked gate
            // above and the egress-gated handoff below. No-op in production.
            var egressHook = PreEgressHookForTesting;
            if (egressHook != null)
            {
                egressHook();
            }

            // The FINAL epoch re-read and the transport handoff are one
            // atomic step against consent decisions: the client advances its
            // denial epoch inside this same egress gate, so a denial either
            // completes first — and this lock-free re-read refuses with zero
            // wire traffic — or waits until the request has been handed to
            // the transport, making that request pre-decision traffic the
            // settle path fails closed. After a denial's epoch advance is
            // observable, no assignment request egresses. The hold covers
            // ONLY this re-read and the non-blocking transport start: a
            // synchronous sender's callback (and the sender-threw fallback)
            // is deferred through the dispatch trap and runs after the gate
            // releases, so the settle path's locks are never taken inside
            // the hold. Every state-shaped refusal was already resolved by
            // the unlocked gate above; any change since then advanced the
            // epoch, so the epoch alone decides here.
            var dispatch = new EgressDispatch();
            Action<ShardPilotHttpResponse> settle = response => HandleResponse(
                seq, scope, experimentKey, authEpoch, consentEpochAtIntent, normalized,
                attributes, callback, isRevalidation, entry, declaration, response);
            var egressRefused = false;
            var tornAtEgress = false;
            lock (_egressGate)
            {
                // TEARDOWN re-read, same hold as the final epoch re-read: a
                // CloseAsync submitted after the early _tornDown check (the
                // client tears the consumer down at close submission)
                // advances no consent epoch, so the epoch comparison alone
                // would still hand this request to the transport after
                // shutdown began — or even after the awaited close resolved.
                // Refuse closed instead: zero egress once teardown is
                // observable.
                lock (_lock)
                {
                    tornAtEgress = _tornDown || _closingApplications;
                }

                if (tornAtEgress || ConsentEpochNow() != consentEpochAtIntent)
                {
                    egressRefused = true;
                }
                else
                {
                    // Test seam: parks this thread INSIDE the egress hold,
                    // between the final gated re-reads and the transport
                    // handoff — the exact window a teardown barrier must
                    // wait out. No-op in production.
                    var egressGateHook = EgressGateHookForTesting;
                    if (egressGateHook != null)
                    {
                        egressGateHook();
                    }

                    try
                    {
                        _sender.Send(request, response => dispatch.Deliver(response, settle));
                    }
                    catch (Exception)
                    {
                        dispatch.Deliver(null, settle);
                    }
                }
            }

            if (egressRefused)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(
                    tornAtEgress ? "shutdown" : "consent_changed"));
                return false;
            }

            dispatch.Open(settle);
            return true;
        }

        /// <summary>Defers a transport callback that fires while the
        /// dispatching thread still holds the egress gate: the settle path
        /// takes the consumer lock (and, on a sentinel, the client's purge
        /// gate), and running it inside the hold would nest those locks
        /// against the client's denial ordering. A synchronous sender's
        /// response is HELD and run by the dispatcher immediately after the
        /// gate releases — same thread, same overall order as before — while
        /// an asynchronous response arriving after <see cref="Open"/> runs
        /// directly on its own thread, exactly as it always did. First
        /// delivery wins; the transport contract already promises a single
        /// callback, so the guard is structural insurance.</summary>
        private sealed class EgressDispatch
        {
            private readonly object _sync = new object();
            private ShardPilotHttpResponse _held;
            private bool _heldSet;
            private bool _open;
            private bool _delivered;

            public void Deliver(ShardPilotHttpResponse response, Action<ShardPilotHttpResponse> settle)
            {
                lock (_sync)
                {
                    if (!_open)
                    {
                        if (!_heldSet)
                        {
                            _held = response;
                            _heldSet = true;
                        }

                        return;
                    }

                    if (_delivered)
                    {
                        return;
                    }

                    _delivered = true;
                }

                settle(response);
            }

            public void Open(Action<ShardPilotHttpResponse> settle)
            {
                ShardPilotHttpResponse response = null;
                var run = false;
                lock (_sync)
                {
                    _open = true;
                    if (_heldSet && !_delivered)
                    {
                        _delivered = true;
                        response = _held;
                        run = true;
                    }
                }

                if (run)
                {
                    settle(response);
                }
            }
        }

        /// <summary>The servable entry for (scope, experiment): the held
        /// entry, suppressed by a process-wide sentinel condemnation stamped
        /// at or above it (a sibling instance's sentinel drop stops this
        /// instance's serving too) and, for an age-gated assignment, by a
        /// pending non-adult age declaration.</summary>
        private ShardPilotExperimentEntry LoadServableEntryLocked(string scope, string experimentKey)
        {
            ShardPilotExperimentEntry entry;
            if (_terminalAssignments.Contains(experimentKey) || !_entries.TryGetValue(experimentKey, out entry))
            {
                return null;
            }

            // A pending non-adult declaration stops an age-gated assignment
            // serving at once: the getters, the explicit exposure, the
            // outcome and every serve of the cached entry refuse until the
            // declaring fetch ends. A synthetic-subject assignment has no age
            // gate and keeps serving.
            if ((_launchUndecided.Contains(experimentKey) || _declaring.ContainsKey(experimentKey)) && AgeGated(entry))
            {
                return null;
            }

            // The entry's OWN subject must still resolve to the requested
            // scope: a re-mint clears the map BEFORE the subject delegate
            // updates, so an old-subject response resolving inside that
            // window can repopulate it with an entry whose SubjectKey is
            // the RETIRED subject — and a key-only lookup would then serve
            // that stale assignment under the NEW subject (getters, the
            // transient serve-stale fallback, the fact producers). A
            // subject/scope mismatch is a MISS at read time, which makes
            // the install-window race harmless however it interleaves —
            // the commit-gate recomputes close the window from the
            // write side, this closes it from the read side. The orphan is
            // deliberately NOT evicted here: callers reach this lookup
            // from enumeration passes (the revalidation batcher), and the
            // next install, latch, or rotation replaces it anyway.
            if (entry.SubjectKey == null ||
                !string.Equals(ScopeFor(entry.SubjectKey), scope, StringComparison.Ordinal))
            {
                return null;
            }

            DateTime condemnedAt;
            if (TryGetCondemnation(scope, out condemnedAt) && entry.FetchedAtUtc <= condemnedAt)
            {
                return null;
            }

            return entry;
        }

        /// <summary>The settle-path gate triple — auth epoch, subject scope,
        /// and the per-key sequence fence — recomputed UNDER the caller's
        /// commit lock (the recompute-under-lock idiom): a snapshot taken before
        /// a later lock may be stale by the time a decision commits inside
        /// it, because a newer authoritative settle, a fail-closed latch, or
        /// a subject re-mint can land in the gap. Every stage that commits a
        /// decision on these gates re-derives them in its own lock hold
        /// instead of trusting the snapshot. The fence read is "superseded
        /// by a STRICTLY newer settle" (settled &gt; seq), which is correct
        /// both BEFORE <see cref="Install"/> (this seq cannot be in the map
        /// yet, so it equals the seq &lt;= settled capture) and AFTER a
        /// successful install (which settles this very seq —
        /// self-settlement is not supersession). Caller must hold
        /// <c>_lock</c>; takes <see cref="StaticGate"/> inside, matching the
        /// established order.</summary>
        private void GatesNowLocked(
            long seq,
            string scope,
            string experimentKey,
            long capturedEpoch,
            out bool epochCurrent,
            out bool scopeCurrent,
            out bool fencedOut)
        {
            epochCurrent = capturedEpoch == _authEpoch;
            var subjectNow = CurrentSubjectId();
            scopeCurrent = subjectNow != null &&
                           string.Equals(ScopeFor(subjectNow), scope, StringComparison.Ordinal);
            lock (StaticGate) fencedOut = SettledLocked(scope, experimentKey) > seq;
        }

        /// <summary>The key's fence for the scope: its own, or a later adult
        /// call's subject-free one. Caller holds <see cref="StaticGate"/>.</summary>
        private long SettledLocked(string scope, string experimentKey)
        {
            long settled, adult;
            Settled.TryGetValue(FenceKey(scope, experimentKey), out settled);
            Settled.TryGetValue(FenceKey(ScopeFor(null), experimentKey), out adult);
            return Math.Max(settled, adult);
        }

        private void HandleResponse(
            long seq,
            string scope,
            string experimentKey,
            long capturedEpoch,
            long consentEpochAtIntent,
            List<KeyValuePair<string, string>> normalizedAttributes,
            IDictionary<string, string> originalAttributes,
            Action<ShardPilotExperimentResult> callback,
            bool isRevalidation,
            ShardPilotExperimentEntry capturedEntry,
            NonAdultDeclaration declaration,
            ShardPilotHttpResponse response)
        {
            var replacedNow = false;
            lock (_lock)
            {
                // A torn-down consumer discards in-flight responses
                // outright: nothing installs, persists, or paces, and game
                // code is not called back after shutdown. A DISOWNED
                // automatic lane discards its in-flight REVALIDATION
                // responses the same way: the facade replaced this client,
                // and a late cadence response must not install or persist
                // the old client's decision into the shared cache the
                // successor now owns. A host-triggered fetch on a disowned
                // client resolves its CALLER as experiments_replaced, with no
                // variant (#160): the client serves nothing, so the host must
                // not apply what the old client fetched. Nothing below runs,
                // so no old-client response of any kind can mutate the
                // shared state (memory, disk, fences, condemnations, subject
                // id) the successor now owns.
                if (_tornDown || _closingApplications || (isRevalidation && _automaticLaneDisowned))
                {
                    return;
                }

                replacedNow = _automaticLaneDisowned;
            }

            if (replacedNow)
            {
                Finish(callback, ShardPilotExperimentResult.Failure(ReplacedCode));
                return;
            }

            // The consent re-check comes first — STATE and EPOCH: a
            // revocation while the request was in flight closes the plane
            // for the caller, and the moved denial epoch closes it even
            // when a quick re-grant already reads granted again (the
            // response was fetched under a consent that did not hold for
            // the whole flight). Closed consent partitions the outcome
            // instead of discarding it wholesale: CONSTRUCTIVE effects
            // (installs, unlatch, pacing, re-mint) are suppressed, while a
            // DESTRUCTIVE server verdict from this granted-dispatch
            // request (authoritative not-assigned, 404, permanent 400, the
            // real-subjects sentinel) still lands its local withdrawal —
            // the platform killed the assignment, and a consent flip must
            // not preserve a variant the server withdrew. The caller
            // receives the closed result in every case.
            var refusalNow = ConsentRefusal(_consent());
            if (refusalNow == null && ConsentEpochNow() != consentEpochAtIntent)
            {
                refusalNow = "consent_changed";
            }

            var consentClosed = refusalNow != null;

            var resolvedAtUtc = NowUtc();
            ShardPilotExperimentOutcome outcome;
            bool fencedOut;
            bool epochCurrent;
            bool scopeCurrent;
            bool sweepNeeded = false;
            ShardPilotExperimentEntry servedFromCache = null;
            lock (_lock)
            {
                // The initial gate snapshot — a DERIVATION input only (which
                // entry may answer a transient, which outcome Apply builds).
                // Every stage below that COMMITS a decision on these gates
                // re-derives them under its own lock via GatesNowLocked: the
                // snapshot is stale the moment this lock releases.
                GatesNowLocked(
                    seq, scope, experimentKey, capturedEpoch,
                    out epochCurrent, out scopeCurrent, out fencedOut);

                // Transient serves come from the CURRENT fenced entry, not
                // the one captured at dispatch: a kill or supersede that
                // resolved while this request was in flight must not be
                // undone in the RESULT either — a caller must never be
                // handed a variant the fenced state no longer serves. Stale
                // epoch/scope reads as no entry.
                ShardPilotExperimentEntry entryNow = null;
                if (epochCurrent && scopeCurrent)
                {
                    entryNow = LoadServableEntryLocked(scope, experimentKey);
                }

                // Serve-stale is attribute-scoped: the cached entry answers
                // a transient failure only when THIS fetch requested the
                // same normalized targeting context the entry was fetched
                // under; a mismatched context gets the closed transient
                // result, never a foreign-targeting variant. (The install
                // path is unaffected — a fresh 200 for the new context
                // replaces the entry regardless.)
                var servableForThisFetch = entryNow != null &&
                    ShardPilotExperimentsLogic.AttributeSetsMatch(normalizedAttributes, entryNow.Attributes)
                    ? entryNow
                    : null;
                servedFromCache = servableForThisFetch;

                outcome = ShardPilotExperimentsLogic.Apply(
                    servableForThisFetch, response, resolvedAtUtc, experimentKey, _config.AppKey, _config.EnvironmentKey);
            }

            // Test seam: lets a test settle a NEWER response inside the
            // snapshot-to-commit gap the recomputes below close. No-op in
            // production.
            var gateSnapshotHook = GateSnapshotHookForTesting;
            if (gateSnapshotHook != null)
            {
                gateSnapshotHook();
            }

            if (outcome.Remint)
            {
                // The persisted subject id failed the wire grammar (storage
                // corruption this client could not detect locally): re-mint
                // once per process and retry as a fresh subject — from the
                // revalidation path too, or a rejected cached subject would
                // never heal until an explicit host fetch happens to run. A
                // second grammar reject with a freshly minted id is a bug,
                // never a loop. The re-mint honors the SAME fences as any
                // other outcome — a grammar reject that is fenced out, stale
                // by epoch, or stale by scope must not rotate the persisted
                // subject, wipe entries, or consume the one-shot budget: it
                // falls through and is discarded like the stale outcome it
                // is.
                var remintNow = false;
                lock (_lock)
                {
                    // The gates are RECOMPUTED under this commit lock (the
                    // recompute-under-lock idiom): the snapshot above predates
                    // this hold, and a newer assignment or withdrawal
                    // settling the same key in the gap must not let a
                    // superseded 400 spend the one-shot budget, clear the
                    // entries, and rotate the persisted subject. The fresher
                    // reads are adopted OUTWARD too — each gate only
                    // tightens (the auth epoch moves forward, a re-minted
                    // scope never returns, the fence only rises), so the
                    // degrade decision below runs on the newest state.
                    GatesNowLocked(
                        seq, scope, experimentKey, capturedEpoch,
                        out epochCurrent, out scopeCurrent, out fencedOut);

                    // Closed consent refuses the re-mint outright (no new
                    // mint, no retry egress); the grammar reject then
                    // degrades below to the ordinary permanent-400 drop —
                    // a DESTRUCTIVE verdict that still lands. A DISOWNED
                    // client refuses it too: rotating the persisted subject
                    // id (and re-dispatching a fetch) is exactly the shared
                    // mutation a replaced instance no longer owns.
                    if (!_reminted && !fencedOut && epochCurrent && scopeCurrent && !consentClosed &&
                        !_automaticLaneDisowned)
                    {
                        _reminted = true;
                        remintNow = true;
                    }
                }

                if (remintNow)
                {
                    // The re-mint is the settle path's own MINT COMMIT and
                    // honors the same rule as the fetch path's: the
                    // consentClosed read above predates this moment, so the
                    // rotation commits inside the egress gate with the
                    // denial epoch re-read — a denial landing since the
                    // settle's consent read means nothing may mint or
                    // persist a fresh spcid_. A refused commit rolls the
                    // one-shot budget back (nothing rotated; a later
                    // genuine grammar reject keeps its remint) and falls
                    // through to the ordinary degrade, whose destructive
                    // drop still lands under closed consent.
                    var remintCommitted = false;
                    var tornAtRemint = false;
                    lock (_egressGate)
                    {
                        // TEARDOWN re-read, same hold as the epoch re-read —
                        // the mint-commit rule the initial mint and the send
                        // handoff already follow: a CloseAsync submitted
                        // after HandleResponse's top teardown guard advances
                        // no consent epoch, so the epoch comparison alone
                        // would still let this remint clear the cached
                        // assignments and persist a fresh spcid_ after
                        // shutdown began (or after a timeout-resolved close).
                        lock (_lock)
                        {
                            tornAtRemint = _tornDown || _closingApplications;
                        }

                        if (!tornAtRemint && ConsentEpochNow() == consentEpochAtIntent)
                        {
                            AdoptMintedSubjectId();
                            remintCommitted = true;
                        }
                    }

                    if (remintCommitted)
                    {
                        Diagnose("subject_id_reminted");
                        // The retry carries the SAME normalized attribute set
                        // the rejected request sent: the adopt above cleared the
                        // cached entry, so the cadence path could no longer
                        // recover its saved attributes on its own.
                        Fetch(experimentKey, originalAttributes, callback, isRevalidation, normalizedAttributes,
                            declaration, seq);
                        return;
                    }

                    lock (_lock)
                    {
                        _reminted = false;
                    }

                    if (tornAtRemint)
                    {
                        // Shutdown refusal mutates NOTHING: no rotation, no
                        // durable drop — the entries stay intact for the
                        // close drain (and a successor's restore), exactly
                        // like the egress path's own torn refusal. A
                        // consent-changed refusal instead falls through to
                        // the ordinary degrade below, whose destructive drop
                        // still lands under closed consent.
                        Finish(callback, ShardPilotExperimentResult.Failure("shutdown"));
                        return;
                    }
                }

                if (epochCurrent && scopeCurrent && !fencedOut)
                {
                    // The one-shot budget is SPENT (a conforming mint still
                    // got the grammar reject — a bug, never a retry loop):
                    // the outcome degrades to the ordinary permanent-400
                    // rule — the cached assignment drops durably and the
                    // caller gets the closed result. The carve-out from the
                    // drop exists only for the path that actually re-mints;
                    // fenced-out or stale rejects fall through and are
                    // discarded like the stale outcomes they are.
                    outcome.Remint = false;
                    outcome.DropEntry = true;
                }
            }

            if (consentClosed && !outcome.DropEntry && !outcome.DropAll)
            {
                // Closed consent, non-destructive outcome: suppressed
                // wholesale — no install, no unlatch, no pacing, no fence
                // settle (exactly like a stale-epoch discard) — and the
                // caller gets the closed result.
                Finish(callback, ShardPilotExperimentResult.Failure(refusalNow));
                return;
            }

            if (outcome.Transient)
            {
                // Pacing is gated on epoch, scope currency, AND the per-key
                // sequence fence exactly like the install: a transient
                // answer for a re-minted-away subject — or one a newer
                // settled outcome already fenced out — is discarded from
                // state, and its Retry-After must be discarded with it. A
                // stale 429/5xx must not park the CURRENT subject's
                // revalidation (and kill checks) for up to the day clamp.
                // All three gates are RECOMPUTED inside this pacing lock
                // (the recompute-under-lock idiom): the earlier snapshot may
                // predate a newer authoritative settle, and a superseded
                // transient must arm nothing — not the shared Retry-After
                // deadline, not the backoff counter.
                lock (_lock)
                {
                    bool paceEpochCurrent;
                    bool paceScopeCurrent;
                    bool paceFencedOut;
                    GatesNowLocked(
                        seq, scope, experimentKey, capturedEpoch,
                        out paceEpochCurrent, out paceScopeCurrent, out paceFencedOut);
                    if (paceEpochCurrent && paceScopeCurrent && !paceFencedOut)
                    {
                        PaceTransientLocked(outcome.RetryAfterSeconds, resolvedAtUtc);
                    }
                }
            }

            List<string> condemnedFactPurge;
            WithdrawnApplications ageWithdrawal;
            sweepNeeded = Install(
                seq, scope, experimentKey, outcome, capturedEpoch, resolvedAtUtc, normalizedAttributes,
                consentClosed, !isRevalidation && ShardPilotExperimentsLogic.HasAgeDeclaration(normalizedAttributes),
                out condemnedFactPurge, out ageWithdrawal);

            // The sentinel's queue withdrawal runs OUTSIDE the consumer
            // lock (the purger takes the client's gate): facts already
            // ACCEPTED into the analytics pipeline for the condemned scope
            // must not ship on the next flush.
            InvokeFactPurge(condemnedFactPurge);

            // The age refusal's pipeline withdrawal, outside the lock for
            // the same reason: the refused subject's accepted facts for this
            // experiment must not ship either.
            InvokeKeyFactWithdrawal(experimentKey, ageWithdrawal);
            if (sweepNeeded)
            {
                lock (_sweepLock)
                {
                    SweepPendingSerialized(experimentKey);
                }
            }

            if (consentClosed)
            {
                // The destructive withdrawal landed above; the caller still
                // receives the CLOSED result — never a verdict fetched
                // under a consent that did not hold for the whole flight.
                Finish(callback, ShardPilotExperimentResult.Failure(refusalNow));
                return;
            }

            // The epoch re-check guards the PUBLIC callback like the
            // install: a response that raced a fail-closed latch was
            // discarded from state above, and its caller must not receive a
            // healthy assignment either — it gets the closed result. (The
            // latch-setting response itself re-derives its own identical
            // closed result here.) The result is decided under the lock
            // and delivered after it releases: the callback may end a
            // pending non-adult declaration, whose withdrawal hands the
            // pipeline scan to the client's gate (gate → consumer lock).
            var settledResult = outcome.Result;
            string withheldCode;
            lock (_lock)
            {
                // The callback gates are RECOMPUTED in this final lock hold
                // (the recompute-under-lock idiom) — the fence included: the
                // snapshot's fence read predates Install, and a newer
                // response settling the same key in between means Install
                // discarded this outcome on its own recheck while a stale
                // false would hand the caller the DISCARDED variant. The
                // post-install read distinguishes self-settlement (this seq
                // — serve this outcome) from supersession (a strictly newer
                // seq — serve the settled state).
                bool callbackEpochCurrent;
                bool callbackScopeCurrent;
                bool callbackFencedOut;
                GatesNowLocked(
                    seq, scope, experimentKey, capturedEpoch,
                    out callbackEpochCurrent, out callbackScopeCurrent, out callbackFencedOut);
                if (!callbackEpochCurrent)
                {
                    settledResult = ShardPilotExperimentResult.Failure("unauthorized");
                }

                // The subject-scope re-check guards the callback the same
                // way: a subject re-minted while this response was in flight
                // makes it ANOTHER subject's assignment — the install
                // discarded it, and the caller must receive the miss, never
                // the discarded variant.
                else if (!callbackScopeCurrent)
                {
                    settledResult = ShardPilotExperimentResult.Failure("stale_subject");
                }

                // And the per-key sequence fence guards it last: an older
                // AUTHORITATIVE response that a newer settled outcome
                // already fenced out was discarded by the install, and its
                // caller must receive the SETTLED current state — the cached
                // assignment the getters serve, or the miss — never the
                // fenced-out variant. Transient results already derive from
                // the current fenced entry and pass through untouched, and a
                // disowned client's authoritative response (nothing settled,
                // nothing newer either) still resolves its caller with the
                // fetched outcome.
                else if (callbackFencedOut && outcome.Authoritative)
                {
                    var current = LoadServableEntryLocked(scope, experimentKey);
                    settledResult = ShardPilotExperimentsLogic.ServeEntryOrFail(current, "superseded", 0).Result;
                }

                // A pending non-adult declaration: no caller receives an
                // age-gated variant of the experiment unless it is the
                // declaring fetch's own applied answer (see
                // DeclarationWithholdsResultLocked).
                else if (settledResult != null && settledResult.Assigned &&
                         DeclarationWithholdsResultLocked(
                             experimentKey, seq, outcome.NewEntry ?? servedFromCache, settledResult,
                             out withheldCode))
                {
                    settledResult = ShardPilotExperimentResult.Failure(withheldCode);
                }
            }

            Finish(callback, settledResult);
        }

        /// <summary>
        /// Settle an authoritative fetch outcome and, when it may, install
        /// it. Gates, in order: this client must still OWN the shared cache
        /// (a disowned instance's responses mutate nothing — constructive
        /// or destructive — however they were triggered), the auth epoch
        /// must still be current (an outcome whose fetch was already in
        /// flight when a fail-closed latch landed is discarded outright —
        /// it must neither unlatch nor reinstall revoked assignments), the
        /// scope must still be current (a subject re-minted while the
        /// response was in flight makes it another subject's assignment),
        /// then the per-key sequence fence for constructive and ordinary
        /// per-key outcomes. A current-epoch auth refusal or whole-plane
        /// sentinel still closes the plane when that key's fence has moved
        /// ahead. The shared fence never moves backward. Returns true when
        /// a fresh install armed an exposure that should be swept.
        /// </summary>
        private bool Install(
            long seq,
            string scope,
            string experimentKey,
            ShardPilotExperimentOutcome outcome,
            long capturedEpoch,
            DateTime resolvedAtUtc,
            List<KeyValuePair<string, string>> normalizedAttributes,
            bool consentClosed,
            bool freshHostAgeDecision,
            out List<string> condemnedFactPurge,
            out WithdrawnApplications ageWithdrawal)
        {
            condemnedFactPurge = null;
            ageWithdrawal = null;
            lock (_lock)
            {
                // The cache-ownership gate: a DISOWNED client's response —
                // host-triggered fetches included — may still resolve its
                // caller's callback, but it mutates NOTHING here. The facade
                // published a successor over this instance, so the shared
                // record (and the process-wide fence/condemnation planes)
                // are the successor's now; a late old-scope install would
                // overwrite the successor's freshly written record wholesale
                // (a reinit legitimately changes app/env/key scope). The
                // destructive-vs-constructive consent partition does NOT
                // apply: that partition arbitrates which verdicts the OWNER
                // lands — a disowned instance simply no longer owns the
                // file, so its writes are suppressed entirely, withdrawals
                // and installs alike (the successor's own lane receives the
                // server's verdicts).
                if (_automaticLaneDisowned)
                {
                    return false;
                }

                if (capturedEpoch != _authEpoch)
                {
                    return false;
                }

                var subject = CurrentSubjectId();
                if (subject == null || !string.Equals(ScopeFor(subject), scope, StringComparison.Ordinal))
                {
                    return false;
                }

                lock (StaticGate)
                {
                    var fenceKey = FenceKey(scope, experimentKey);
                    var settled = SettledLocked(scope, experimentKey);
                    if (seq <= settled && !outcome.AuthBlocked)
                    {
                        return false;
                    }

                    if (outcome.Authoritative && seq > settled)
                    {
                        Settled[fenceKey] = seq;
                    }
                }

                if (outcome.TerminalAssignment)
                {
                    _terminalAssignments.Add(experimentKey);
                    _revalidateAtByKey.Remove(experimentKey);
                    return false;
                }

                if (outcome.AuthBlocked)
                {
                    // Fail closed: stop serving every cached assignment (the
                    // getters return null), halt revalidation, and open a
                    // new auth epoch so every response still in flight is
                    // discarded — only a fetch started AFTER this moment may
                    // unlatch. The durable record is retained (a re-init may
                    // serve it and re-probe) unless the real-subjects
                    // sentinel says the platform withdrew the assignments
                    // outright.
                    _authBlocked = true;
                    _authEpoch++;
                    var condemnFloor = resolvedAtUtc;
                    foreach (var held in _entries)
                    {
                        if (held.Value.FetchedAtUtc >= condemnFloor)
                        {
                            condemnFloor = RaiseAboveStamp(held.Value.FetchedAtUtc);
                        }
                    }

                    if (outcome.DropAll)
                    {
                        // The floor also rises to the RESOLVED experiment's
                        // raw durable stamps: a generic auth latch may have
                        // emptied memory while the disk retains this key's
                        // record, and after a clock rollback that retained
                        // stamp can sit above this resolution — fencing on
                        // memory alone would let the clear succeed
                        // vacuously, lift the marker, and hand the
                        // withdrawn assignment back to the next launch. The
                        // raise is TO the stamp (the fence keeps strictly
                        // newer) and covers ONLY the key this sentinel
                        // resolved: a sibling's genuinely newer write for
                        // another key is post-condemnation state the
                        // stamp-fence canon deliberately preserves.
                        var storedForFloor = RawDurableEntriesLocked(scope);
                        ShardPilotExperimentEntry storedResolved;
                        if (storedForFloor.TryGetValue(experimentKey, out storedResolved) &&
                            storedResolved.FetchedAtUtc > condemnFloor)
                        {
                            condemnFloor = storedResolved.FetchedAtUtc;
                        }
                    }

                    _entries.Clear();
                    if (outcome.DropAll)
                    {
                        // The sentinel withdraws the assignments AND their
                        // subject-fact keys outright: owed exposure
                        // snapshots carry those keys and go with them —
                        // THIS SCOPE'S snapshots. A retired scope's owed
                        // facts (kept across a subject re-mint) are not this
                        // sentinel's to withdraw and survive. The scope is
                        // condemned process-wide BEFORE the durable clear
                        // is attempted, so a sibling instance's held copy
                        // stops serving and a record surviving a failed
                        // clear is refused on every later load.
                        DiscardCondemnedScopeOwedLocked(scope);
                        lock (StaticGate)
                        {
                            Condemned[CondemnationKey(scope)] = condemnFloor;
                            CondemnationVersion++;
                            _seenCondemnationVersion = CondemnationVersion;
                        }

                        // Facts already ACCEPTED into the analytics pipeline
                        // for this scope — queue-resident, or held in the
                        // worker's retained batch behind backoff — carry the
                        // withdrawn subject-fact keys and must not ship on
                        // the next flush: collect them here; the caller
                        // hands them to the client's purger outside this
                        // lock. Sibling instances sweep their own on their
                        // next tick via the version bump above.
                        condemnedFactPurge = CollectCondemnedFactIdsLocked();

                        // The condemnation is persisted BEFORE the clear is
                        // attempted: if the process dies with the clear owed,
                        // the next launch reads the marker, refuses the
                        // withdrawn record, and re-attempts the clear — the
                        // in-memory owed state alone dies with the process.
                        var markerWritten = WriteCondemnationMarkerLocked(scope, condemnFloor);
                        if (FencedSentinelClearLocked(scope, condemnFloor))
                        {
                            // Only THIS scope's owed intents are moot; a
                            // retired scope's owed drops keep converging.
                            var settled = new List<string>();
                            foreach (var pair in _durablePending)
                            {
                                if (string.Equals(pair.Value.Scope, scope, StringComparison.Ordinal))
                                {
                                    settled.Add(pair.Key);
                                }
                            }

                            for (var i = 0; i < settled.Count; i++)
                            {
                                _durablePending.Remove(settled[i]);
                            }

                            _durableClearPendingAsOfUtc = null;
                            _durableClearPendingScope = null;
                            MaybeRemoveCondemnationMarkerLocked();
                        }
                        else
                        {
                            // The withdrawn assignments are still on disk:
                            // mark the clear OWED — stamped by this
                            // resolution — and retry it every tick until it
                            // lands. The marker (when it wrote) keeps the
                            // condemnation across a crash meanwhile.
                            _durableClearPendingAsOfUtc = condemnFloor;
                            _durableClearPendingScope = scope;
                            Diagnose(markerWritten
                                ? "assignment_cache_clear_failed"
                                : "assignment_condemnation_persist_failed");
                        }
                    }
                    else
                    {
                        // An ORDINARY 401/403 retains the durable record —
                        // and the owed EXPOSURE snapshots: a treatment that
                        // already ran is a fact about the past, and the
                        // latch stops future serving, not the reporting of
                        // what happened. An owed cache WRITE whose entry
                        // this latch just cleared from memory must not decay
                        // into a delete at the next retry — absence in
                        // memory here is fail-closed serving, not a drop
                        // decision — so owed writes are cancelled (their
                        // source data is gone; the disk keeps its
                        // last-known record). Owed DROPS were authoritative
                        // server decisions and still land.
                        var cancelled = new List<string>();
                        foreach (var pair in _durablePending)
                        {
                            if (!pair.Value.Drop)
                            {
                                cancelled.Add(pair.Key);
                            }
                        }

                        for (var i = 0; i < cancelled.Count; i++)
                        {
                            _durablePending.Remove(cancelled[i]);
                        }
                    }

                    if (outcome.DropAll)
                    {
                        // The real-subjects sentinel is the server's answer
                        // to a declaring fetch; an ordinary latch is not (it
                        // retains the durable record for a later unlatch).
                        DecideDeclarationLocked(experimentKey, seq);
                    }

                    return false;
                }

                if (outcome.Authoritative && !outcome.Transient && !consentClosed)
                {
                    // An authoritative, authorized outcome of a post-latch
                    // fetch (the epoch gate above guarantees that) proves
                    // the credential works again: unlatch and let
                    // revalidation resume. The unexpected-status shape
                    // (Authoritative AND Transient: fence settles, cache
                    // serves stale) is excluded — a stray proxy status
                    // proves nothing about the credential, so it neither
                    // latches nor unlatches. And a CLOSED-consent
                    // destructive landing is a withdrawal only: it neither
                    // unlatches nor resets pacing below.
                    _authBlocked = false;
                }

                if (outcome.Transient)
                {
                    return false;
                }

                // Only an applied answer to this launch's host age fetch
                // decides startup eligibility. Cadence, malformed/transient
                // responses and auth latches cannot confirm it.
                if (freshHostAgeDecision && outcome.Authoritative && outcome.Result.Ok && !consentClosed)
                {
                    _launchUndecided.Remove(experimentKey);
                    _launchDecided.Add(experimentKey);
                }

                // A settled authoritative answer resets the
                // consecutive-failure counter. A server-set Retry-After
                // deadline is deliberately NOT cleared here: the pacing is
                // shared by every cached entry, and one admitted request for
                // an unrelated entry does not rescind the server's wait for
                // the plane — the deadline simply expires on its own. A
                // closed-consent destructive landing leaves pacing alone.
                if (!consentClosed)
                {
                    _backoffAttempt = 0;
                }

                if (outcome.DropEntry)
                {
                    // A not-assigned verdict or a permanent drop answers a
                    // declaring fetch: it governs as it always has (below).
                    DecideDeclarationLocked(experimentKey, seq);
                }

                if (outcome.DropEntry && outcome.WithdrawOwedApplications)
                {
                    // The age refusal: the subject is not eligible, so its
                    // owed applications are withdrawn with the entry instead
                    // of surviving as facts (the branch below).
                    NonAdultDeclaration declaration;
                    var declaredKeys = _declaring.TryGetValue(experimentKey, out declaration) && declaration.Seq == seq
                        ? declaration.FactKeys : null;
                    ageWithdrawal = WithdrawRefusedApplicationsLocked(
                        scope, experimentKey, outcome.RefusedSubjectFactKey, resolvedAtUtc, declaredFactKeys: declaredKeys);
                    return false;
                }

                if (outcome.DropEntry)
                {
                    ShardPilotExperimentEntry dropped;
                    _entries.TryGetValue(experimentKey, out dropped);
                    _entries.Remove(experimentKey);
                    // The OWED exposures deliberately survive the drop: an
                    // application that already happened is a fact — the drop
                    // stops future serving, not the record of real
                    // treatment. The durable drop converges keyed on the
                    // DISK state (a latch may have cleared serving while the
                    // record retains the entry) and is retried until it
                    // lands — the kill rule demands the drop reach the disk.
                    // The drop's effective stamp is raised above the entry
                    // it resolves: the wall clock can move backward, and a
                    // drop for X must always outrank X's own stamp.
                    var asOf = resolvedAtUtc;
                    if (dropped != null && dropped.FetchedAtUtc >= asOf)
                    {
                        asOf = RaiseAboveStamp(dropped.FetchedAtUtc);
                    }

                    // A cache drop does not erase applications already recorded
                    // in memory; their captured pins remain available to the apply route.
                    SyncDurableEntryLocked(scope, experimentKey, asOf, false);
                    return false;
                }

                if (outcome.NewEntry != null)
                {
                    _terminalAssignments.Remove(experimentKey);
                    if (consentClosed)
                    {
                        // Unreachable via HandleResponse (constructive
                        // outcomes are refused before Install under closed
                        // consent) — structural insurance: a constructive
                        // install must never land under closed consent.
                        return false;
                    }

                    var entry = outcome.NewEntry;
                    var subjectNow = CurrentSubjectId();
                    entry.SubjectKey = subjectNow;
                    entry.Attributes = normalizedAttributes != null && normalizedAttributes.Count > 0
                        ? normalizedAttributes
                        : null;

                    // Freshness stamps order the records for a scope and the
                    // wall clock can move backward: raise the stamp above
                    // every record this install supersedes — the held entry,
                    // the durable entry, and any sentinel condemnation — so
                    // a later offline fetch cannot roll back and a fresh
                    // authorized assignment lifts the condemned state.
                    var floor = entry.FetchedAtUtc;
                    ShardPilotExperimentEntry held;
                    if (_entries.TryGetValue(experimentKey, out held) && held.FetchedAtUtc >= floor)
                    {
                        floor = RaiseAboveStamp(held.FetchedAtUtc);
                    }

                    var raw = RawDurableEntriesLocked(scope);
                    ShardPilotExperimentEntry storedEntry;
                    if (raw.TryGetValue(experimentKey, out storedEntry) && storedEntry.FetchedAtUtc >= floor)
                    {
                        floor = RaiseAboveStamp(storedEntry.FetchedAtUtc);
                    }

                    DateTime condemnedAt;
                    if (TryGetCondemnation(scope, out condemnedAt) && condemnedAt >= floor)
                    {
                        floor = RaiseAboveStamp(condemnedAt);
                    }

                    entry.FetchedAtUtc = floor;
                    _entries[experimentKey] = entry;

                    // A fresh authorized assignment supersedes any owed
                    // whole-record clear RIGHT NOW, not at the next tick:
                    // waiting would leave the stale clear armed across an
                    // ordinary auth latch (which empties memory while
                    // retaining the durable record), and the retention canon
                    // must never let that clear delete state written after
                    // it.
                    DemoteOwedClearLocked();
                    SyncDurableEntryLocked(scope, experimentKey, entry.FetchedAtUtc, false);

                    // Only THIS entry's deadline re-arms: an install must
                    // never extend an unrelated entry's earlier due time
                    // (a frequently-fetched sibling would starve this
                    // entry's kill/not-assigned updates indefinitely).
                    ArmRevalidationLocked(experimentKey, NowUtc());
                    // Installing a serving entry is not a gameplay application.
                    DecideDeclarationLocked(experimentKey, seq);
                    return true;
                }

                return false;
            }
        }

        private void ArmRevalidationLocked(string experimentKey, DateTime nowUtc)
        {
            var jitter = 1 + (NextJitter() * 2 - 1) * ShardPilotExperimentsLogic.RevalidateJitter;
            _revalidateAtByKey[experimentKey] =
                nowUtc.AddSeconds(ShardPilotExperimentsLogic.RevalidateIntervalSeconds * jitter);
        }

        private void DeferRevalidationLocked(double seconds, DateTime nowUtc)
        {
            if (seconds <= 0)
            {
                return;
            }

            if (seconds > ShardPilotExperimentsLogic.MaxDeferSeconds)
            {
                seconds = ShardPilotExperimentsLogic.MaxDeferSeconds;
            }

            var deadline = nowUtc.AddSeconds(seconds);
            if (!_retryAfterUtc.HasValue || deadline > _retryAfterUtc.Value)
            {
                _retryAfterUtc = deadline;
            }
        }

        /// <summary>Transient-failure pacing for the revalidation cadence: a
        /// server Retry-After wins; otherwise exponential backoff with full
        /// jitter (transport parity with the batch/receipt paths).</summary>
        private void PaceTransientLocked(int retryAfterSeconds, DateTime nowUtc)
        {
            if (retryAfterSeconds > 0)
            {
                DeferRevalidationLocked(retryAfterSeconds, nowUtc);
                return;
            }

            _backoffAttempt++;
            if (_backoffAttempt < 2)
            {
                return;
            }

            var exp = _backoffAttempt - 2;
            if (exp > 16)
            {
                exp = 16;
            }

            double ceiling = ShardPilotExperimentsLogic.BackoffBaseSeconds * Math.Pow(2, exp);
            if (ceiling > ShardPilotExperimentsLogic.BackoffCapSeconds)
            {
                ceiling = ShardPilotExperimentsLogic.BackoffCapSeconds;
            }

            DeferRevalidationLocked(
                ShardPilotExperimentsLogic.BackoffBaseSeconds +
                NextJitter() * (ceiling - ShardPilotExperimentsLogic.BackoffBaseSeconds),
                nowUtc);
        }

        /// <summary>
        /// The revalidation tick, driven from the analytics worker's cycle.
        /// Owed durable writes retry FIRST and regardless of consent (local
        /// disk housekeeping — a kill drop decided under grant must land
        /// durably even if consent flipped); the owed-exposure sweep and the
        /// fetch cadence run only while consent is granted, the plane is not
        /// auth-latched, and at least one assignment is cached. A parked
        /// revalidation never blocks anything — there is no pending state to
        /// drain at shutdown.
        /// </summary>
        internal void Tick()
        {
            lock (_lock)
            {
                if (_tornDown)
                {
                    return;
                }
            }

            RetryDurableSync();
            RetryLateFactPurges();

            // Before the consent gate: withdrawing condemned facts is data
            // REMOVAL and must run under any consent state — a sibling's
            // sentinel may have landed while this instance's plane was
            // closed.
            SweepCondemnedFacts();
            if (ConsentRefusal(_consent()) != null)
            {
                return;
            }

            SweepOwed();
            List<string> keys = null;
            lock (_lock)
            {
                if (_authBlocked || _automaticLaneDisowned || _entries.Count == 0)
                {
                    return;
                }

                var now = NowUtc();
                if (_retryAfterUtc.HasValue)
                {
                    if (now < _retryAfterUtc.Value)
                    {
                        return;
                    }

                    _retryAfterUtc = null;
                }

                // Deadlines are PER ENTRY: entries seen for the first time
                // arm their own clock (and skip this tick, like the shared
                // clock's first sighting used to), entries past their own
                // deadline revalidate and re-arm, and everything else waits
                // untouched — an unrelated entry's install or revalidation
                // never postpones one that is already due. Deadlines of
                // entries no longer cached are pruned so the map tracks the
                // cache.
                if (_revalidateAtByKey.Count > 0)
                {
                    List<string> stale = null;
                    foreach (var pair in _revalidateAtByKey)
                    {
                        if (!_entries.ContainsKey(pair.Key))
                        {
                            if (stale == null)
                            {
                                stale = new List<string>();
                            }

                            stale.Add(pair.Key);
                        }
                    }

                    if (stale != null)
                    {
                        for (var i = 0; i < stale.Count; i++)
                        {
                            _revalidateAtByKey.Remove(stale[i]);
                        }
                    }
                }

                var cached = new List<string>(_entries.Keys);
                for (var i = 0; i < cached.Count; i++)
                {
                    if (_launchUndecided.Contains(cached[i]) && AgeGated(_entries[cached[i]])) continue;
                    if (_terminalAssignments.Contains(cached[i])) continue;
                    DateTime dueAt;
                    if (!_revalidateAtByKey.TryGetValue(cached[i], out dueAt))
                    {
                        ArmRevalidationLocked(cached[i], now);
                        continue;
                    }

                    if (now < dueAt)
                    {
                        continue;
                    }

                    ArmRevalidationLocked(cached[i], now);
                    if (keys == null)
                    {
                        keys = new List<string>();
                    }

                    keys.Add(cached[i]);
                }
            }

            if (keys == null)
            {
                return;
            }

            keys.Sort(StringComparer.Ordinal);
            for (var i = 0; i < keys.Count; i++)
            {
                // Batched per tick: one GET per cached entry, each
                // re-sending its last host-supplied attributes. Outcomes
                // apply at resolution.
                Fetch(keys[i], null, null, true, null);
            }
        }

        /// <summary>Shutdown housekeeping, run by the client after its final
        /// flush freed queue room: owed durable syncs get one last retry (a
        /// kill drop whose cache write failed transiently must not stay
        /// reload truth on disk), and owed exposure facts get one last sweep
        /// so a treatment applied under a FULL queue does not exit without
        /// its fact. Best-effort by design: a still-failing write or emit is
        /// not a teardown blocker.</summary>
        internal void ShutdownHousekeeping()
        {
            SweepCondemnedFacts();
            RetryDurableSync();
            SweepApplications(false);
        }

        /// <summary>The canonical durable-cache path this consumer owns,
        /// or null when it runs memory-only (no resolvable directory). The
        /// lifecycle registry matches it alongside the spool files: a
        /// replacement client with experiments enabled but both spools
        /// disabled/unavailable shares ONLY this file with its
        /// predecessor, and the registration handoff (old-lane disown,
        /// destructive-debt adoption) must still fire for it.</summary>
        internal string CachePathForHandoff
        {
            get { return string.IsNullOrEmpty(_cachePath) ? null : _cachePath; }
        }

        internal string CacheOwnerKey { get { lock (_lock) return _cachePathIdentityToken; } }

        /// <summary>A facade re-initialization replaced this consumer's
        /// client: the AUTOMATIC lane (revalidation fetches) stops
        /// immediately — a detached client must not re-fetch and rewrite
        /// the shared cache with a decision the successor already
        /// superseded — while the owed housekeeping (durable-sync retries,
        /// the exposure sweep) keeps running so the close drain can land
        /// what is owed.</summary>
        internal void DisownAutomaticLane()
        {
            var replacedNow = false;
            lock (_lock)
            {
                replacedNow = !_automaticLaneDisowned;
                _automaticLaneDisowned = true;
                _replacedNoticeOwed |= replacedNow;

                // The ownership rule extends to the WRITE-DEBT machinery,
                // PARTITIONED: a disowned client retries nothing itself —
                // the shared record file is the successor's now, and a debt
                // retried after storage heals would rewrite the assignments
                // section under the OLD app/key scope, erasing the
                // successor's freshly written cache wholesale. But the two
                // debt kinds settle differently. CONSTRUCTIVE write intents
                // cancel outright (their source entries belong to a
                // replaced instance). DESTRUCTIVE debts — authoritative
                // drops and the owed whole-record clear — are server
                // verdicts the kill rule still owes the disk: they PARK
                // here for the facade to hand to the successor, whose own
                // machinery re-arms and converges them (same-scope debts
                // directly, retired scopes via the foreign-drop path) —
                // otherwise a 404/not-assigned/permanent-400 drop would be
                // lost purely because the client was replaced before
                // storage healed, and the successor would restore the stale
                // assignment. The durable condemnation MARKER (when one is
                // owed) stays on disk regardless: serving stays suppressed
                // process-wide, and a launch with no adopting successor
                // re-arms the clear from it. Only the owed-EXPOSURE section
                // keeps flushing on this client (scope-preserving by
                // construction), so the close drain still lands what is
                // owed.
                foreach (var pair in _durablePending)
                {
                    if (pair.Value.Drop)
                    {
                        if (_disownedOwedDrops == null)
                        {
                            _disownedOwedDrops = new List<DurablePendingSync>();
                        }

                        _disownedOwedDrops.Add(pair.Value);
                    }
                }

                _durablePending.Clear();

                // Guarded move, for IDEMPOTENCE: the direct-construction
                // handoff disowns at PrepareOwnershipHandoff AND (as
                // structural insurance) at the successor's registration —
                // an unguarded second pass would overwrite the parked clear
                // with the now-null live fields and lose it before the
                // successor adopts.
                if (_durableClearPendingAsOfUtc.HasValue)
                {
                    _disownedOwedClearAsOfUtc = _durableClearPendingAsOfUtc;
                    _disownedOwedClearScope = _durableClearPendingScope;
                    _durableClearPendingAsOfUtc = null;
                    _durableClearPendingScope = null;
                }
            }

            // Once, at the first disown: from here on this client serves no
            // experiment and records no experiment fact (ServableEntryForRead,
            // RefuseReplacedFact). Its analytics keep working.
        }

        /// <summary>Logs the replaced-client warning once, if a disown owes
        /// it (#159). The step that completes the handoff calls this outside
        /// its gate: the successor's registration once it has adopted this
        /// client's parked debts, or the facade's re-initialize. The warning
        /// says "Close it", and a host logger that does so at once must not
        /// cost the successor those debts, nor wait on a gate the handoff
        /// still holds.</summary>
        internal void EmitReplacedNoticeIfOwed()
        {
            lock (_lock)
            {
                if (!_replacedNoticeOwed)
                {
                    return;
                }

                _replacedNoticeOwed = false;
            }

            ShardPilotHandoffNotices.Emit(() => Diagnose(
                ReplacedCode + ": a newer client over the same cache took the experiments over; " +
                "this client serves no variant and records no exposure or outcome. Close it."));
        }

        /// <summary>The code a replaced client's exposure and outcome calls
        /// are refused with (<see cref="DisownAutomaticLane"/>).</summary>
        internal const string ReplacedCode = "experiments_replaced";

        /// <summary>A replaced client records no experiment fact: its cached
        /// assignments are no longer revalidated, so an age refusal or a
        /// non-adult declaration the successor receives would never reach
        /// them.</summary>
        private bool RefuseReplacedFact(out string code)
        {
            lock (_lock)
            {
                code = _automaticLaneDisowned ? ReplacedCode : null;
                return code != null;
            }
        }

        /// <summary>True only when THIS instance's construction observed the
        /// parked-drops tombstone marker (and so seeded — or fail-closed on
        /// — its entries): the reconcile prunes the marker by mirroring
        /// <see cref="_durablePending"/>, which is sound only for an
        /// instance whose map actually absorbed the marker. A marker
        /// written AFTER this instance constructed (a sibling's flag-off
        /// park) must never be pruned by a map that never carried its
        /// debts. Guarded by <c>_lock</c>.</summary>
        private bool _parkedDropsMarkerSeen;

        /// <summary>The destructive debts a disown parked, drained for the
        /// successor's adoption. Guarded by <c>_lock</c>.</summary>
        private List<DurablePendingSync> _disownedOwedDrops;
        private DateTime? _disownedOwedClearAsOfUtc;
        private string _disownedOwedClearScope;

        /// <summary>Adopts a displaced predecessor's parked DESTRUCTIVE
        /// cache debts into this consumer's own convergence machinery (the
        /// facade calls this right after constructing the successor):
        /// authoritative drops re-arm as per-key intents — kept only when
        /// no newer intent for the key exists, per the stamp canon — and
        /// land through the ordinary retry paths (same-scope directly,
        /// retired scopes via the foreign-scope drop). A drop whose scope
        /// is THIS consumer's current scope also applies to memory now, so
        /// a stale assignment the constructor just restored stops serving
        /// immediately (only entries stamped at or below the drop — a
        /// genuinely newer write survives, as every owed retry yields to
        /// strictly newer state). The predecessor's owed whole-record clear
        /// re-arms latest-wins.</summary>
        internal void AdoptDestructiveDebtsFrom(ShardPilotExperiments predecessor)
        {
            if (predecessor == null || ReferenceEquals(predecessor, this))
            {
                return;
            }

            List<DurablePendingSync> drops;
            List<PendingWithdrawalIntent> withdrawals;
            DateTime? clearAsOf;
            string clearScope;
            lock (predecessor._lock)
            {
                withdrawals = new List<PendingWithdrawalIntent>(predecessor._withdrawalIntents);
                predecessor._withdrawalIntents.Clear();
                drops = predecessor._disownedOwedDrops;
                clearAsOf = predecessor._disownedOwedClearAsOfUtc;
                clearScope = predecessor._disownedOwedClearScope;
                predecessor._disownedOwedDrops = null;
                predecessor._disownedOwedClearAsOfUtc = null;
                predecessor._disownedOwedClearScope = null;
            }

            if (drops == null && !clearAsOf.HasValue && withdrawals.Count == 0)
            {
                return;
            }

            lock (_lock)
            {
                foreach (var pending in withdrawals)
                {
                    var duplicate = _withdrawalIntents.Find(p => p.Scope == pending.Scope && p.Frame.Id == pending.Frame.Id);
                    if (duplicate == null) _withdrawalIntents.Add(pending);
                    WithdrawalKind kind; ShardPilotExperimentEntry held;
                    if (ShardPilotWithdrawalForm.TryKind(pending.Frame, out kind) &&
                        (kind == WithdrawalKind.AgeDrop || kind == WithdrawalKind.Drop) && pending.Scope == CurrentScopeOrNullLocked() &&
                        _entries.TryGetValue(pending.Frame.Experiment, out held) && (!pending.Frame.Fixed || held.FetchedAtUtc.Ticks <= pending.Frame.BoundaryTicks))
                        _entries.Remove(pending.Frame.Experiment);
                }
                if (drops != null)
                {
                    var scopeNow = CurrentScopeOrNullLocked();
                    for (var i = 0; i < drops.Count; i++)
                    {
                        var drop = drops[i];
                        var key = DurableIntentKey(drop.Scope, drop.ExperimentKey);
                        DurablePendingSync existing;
                        if (!_durablePending.TryGetValue(key, out existing) ||
                            existing.AsOfUtc < drop.AsOfUtc)
                        {
                            _durablePending[key] = drop;
                        }

                        if (scopeNow != null &&
                            string.Equals(drop.Scope, scopeNow, StringComparison.Ordinal))
                        {
                            ShardPilotExperimentEntry held;
                            if (_entries.TryGetValue(drop.ExperimentKey, out held) &&
                                held.FetchedAtUtc <= drop.AsOfUtc)
                            {
                                _entries.Remove(drop.ExperimentKey);
                            }
                        }
                    }
                }

                if (clearAsOf.HasValue &&
                    (!_durableClearPendingAsOfUtc.HasValue ||
                     _durableClearPendingAsOfUtc.Value < clearAsOf.Value))
                {
                    _durableClearPendingAsOfUtc = clearAsOf;
                    _durableClearPendingScope = clearScope;
                }
            }
        }

        internal void BeginCloseDrain()
        {
            // Quiesce producers/fetches without transferring cache ownership:
            // constructive write debts and owned applications still drain.
            lock (_lock) _closingApplications = true;
            AbortDeclarationsForClose();
            lock (_egressGate) { }
        }

        /// <summary>Stop fresh assignment work and unsealed applications at
        /// client teardown. An assignment response landing afterwards is
        /// discarded: it neither installs nor rewrites the assignment cache. The
        /// teardown then PARTICIPATES in the egress serialization: setting
        /// the flag alone cannot stop a fetch that already passed its gated
        /// re-read and is handing the request to the transport inside
        /// <see cref="_egressGate"/> — the close would resolve while that
        /// send is still in flight, and "zero egress after CloseAsync
        /// resolves" would be false. Acquiring and releasing the gate AFTER
        /// the flag is set is a barrier: any in-flight gated send completes
        /// before this teardown returns (so before the close proceeds), and
        /// every later send's in-gate re-read observes the flag and refuses.
        /// Callers may already hold the client gate; the sanctioned global
        /// order is client gate → egress gate → consumer lock (the denial
        /// path's own edge), and nothing inside an egress hold ever reaches
        /// back into client locks, so the barrier cannot deadlock.</summary>
        internal void Teardown()
        {
            lock (_lock)
            {
                _tornDown = true;
            }

            lock (_egressGate)
            {
            }
        }

        // ── getters and fact producers ───────────────────────────────────────

        /// <summary>The cached variant key for the experiment, or null.
        /// Never touches the network, and serves null while consent is not
        /// granted: the plane is granted-only, so a non-granted session sees
        /// no variants at all (the cache record is retained, unserved, until
        /// a re-grant).</summary>
        public string GetExperimentVariant(string experimentKey)
        {
            var entry = ServableEntryForRead(experimentKey);
            return entry != null ? entry.VariantKey : null;
        }

        /// <summary>The cached variant payload (a defensive copy), or null.
        /// Same consent posture as <see cref="GetExperimentVariant"/>.</summary>
        public IReadOnlyDictionary<string, object> GetExperimentPayload(string experimentKey)
        {
            var entry = ServableEntryForRead(experimentKey);
            return entry != null ? ShardPilotExperimentsLogic.CopyPayload(entry.VariantPayload) : null;
        }

        private ShardPilotExperimentEntry ServableEntryForRead(string experimentKey)
        {
            if (string.IsNullOrEmpty(experimentKey))
            {
                return null;
            }

            if (ConsentRefusal(_consent()) != null)
            {
                return null;
            }

            var subject = CurrentSubjectId();
            if (subject == null)
            {
                return null;
            }

            lock (_lock)
            {
                // A torn-down consumer serves nothing: CloseAsync resolved
                // (or is resolving) for the owning client, and a caller
                // holding a stale reference must stop applying treatments —
                // the getters mirror the fetch/track paths' closed refusal
                // with their standard miss result. Nor does a replaced one:
                // its lane no longer revalidates, so a refusal its
                // successor receives would never reach what it holds.
                if (_tornDown || _automaticLaneDisowned || _closingApplications)
                {
                    return null;
                }

                return LoadServableEntryLocked(ScopeFor(subject), experimentKey);
            }
        }

        // Explicit host facts are current-session activity. Admit before reading
        // the session marker or reserving an exposure arm, and outside _lock:
        // the client's owed start renews this consumer and may append unlocked.
        // Historical/automatic sweeps deliberately do not call this boundary.
        private bool AdmitCurrentFact(out string code)
        {
            code = ConsentRefusal(_consent());
            if (code != null)
            {
                return false;
            }
            if (_admitCurrentSession != null && !_admitCurrentSession())
            {
                code = "session_start_pending";
                return false;
            }
            HostFactPostAdmissionHookForTesting?.Invoke();
            return true;
        }

        internal Action HostFactPostAdmissionHookForTesting;
        internal Action FactPostCaptureHookForTesting;

        /// <summary>Capture one additional application of the live assignment with a fresh occurrence ID.</summary>
        internal bool TrackExposure(string experimentKey, out string code)
        { ShardPilotExperimentEntry entry; return RecordApplication(experimentKey, true, out entry, out code); }

        /// <summary>Capture a safe-integer outcome against the last explicit application in the current session.</summary>
        internal bool TrackOutcome(string experimentKey, string outcomeKey, double outcomeValue, out string code)
        { return RecordOutcome(experimentKey, outcomeKey, outcomeValue, out code); }

        // ── test accessors ───────────────────────────────────────────────────

        internal bool AutomaticLaneDisownedForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _automaticLaneDisowned;
                }
            }
        }

        internal bool AuthBlockedForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _authBlocked;
                }
            }
        }

        internal int HeldEntryCountForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count;
                }
            }
        }

        /// <summary>The number of owed exposure snapshots across every
        /// experiment — the shutdown housekeeping loop's progress
        /// measure.</summary>
        internal int OwedExposureCount
        {
            get
            {
                lock (_lock)
                {
                    var count = 0;
                    foreach (var pair in _pendingExposure)
                    {
                        count += pair.Value.Count;
                    }

                    return count;
                }
            }
        }

        internal int OwedExposureCountForTesting
        {
            get { return OwedExposureCount; }
        }

        /// <summary>Owed exposure snapshots for ONE experiment.</summary>
        internal void OwedApplicationsForTesting(string experimentKey, out int snapshots)
        {
            lock (_lock)
            {
                List<OwedExposure> owed;
                snapshots = _pendingExposure.TryGetValue(experimentKey, out owed) ? owed.Count : 0;
            }
        }

        /// <summary>The bounded condemnation-index cap, so a test can wrap
        /// the FIFO deterministically without duplicating the residency
        /// derivation.</summary>
        internal int MaxTrackedFactScopesForTesting
        {
            get { return _maxTrackedFactScopes; }
        }

        internal int OwedDurableSyncCountForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _durablePending.Count + _withdrawalIntents.Count;
                }
            }
        }

        internal bool OwedDurableClearForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _durableClearPendingAsOfUtc.HasValue;
                }
            }
        }

        /// <summary>The EARLIEST armed per-entry revalidation deadline, or
        /// null while none is armed.</summary>
        internal DateTime? RevalidateDueForTesting
        {
            get
            {
                lock (_lock)
                {
                    DateTime? earliest = null;
                    foreach (var pair in _revalidateAtByKey)
                    {
                        if (!earliest.HasValue || pair.Value < earliest.Value)
                        {
                            earliest = pair.Value;
                        }
                    }

                    return earliest;
                }
            }
        }

        /// <summary>The armed revalidation deadline for ONE experiment key,
        /// or null while none is armed for it.</summary>
        internal DateTime? RevalidateDueForTesting_For(string experimentKey)
        {
            lock (_lock)
            {
                DateTime dueAt;
                return _revalidateAtByKey.TryGetValue(experimentKey, out dueAt)
                    ? dueAt
                    : (DateTime?)null;
            }
        }

        internal DateTime? RetryAfterDeadlineForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _retryAfterUtc;
                }
            }
        }

        // Checked under the client's gate before a fact can contribute to the
        // current lifecycle's timestamp. A captured marker survives ID reuse;
        // staged-renewal facts still belong to the pre-start history.
        internal bool IsCurrentSessionMarker(string marker)
        {
            lock (_lock)
            {
                return _provisionalRenewals.Count == 0 &&
                    string.Equals(marker, _sessionMarker, StringComparison.Ordinal);
            }
        }

        internal string SessionMarkerForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _sessionMarker;
                }
            }
        }

        /// <summary>The CURRENT scope's fact-key history for one experiment,
        /// sorted.</summary>
        internal List<string> FactKeyHistoryForTesting(string experimentKey)
        {
            lock (_lock)
            {
                var keys = new List<string>();
                var scope = CurrentScopeOrNullLocked();
                Dictionary<string, List<string>> scopeHistory;
                List<string> retained;
                if (scope != null && _factKeyHistory.TryGetValue(scope, out scopeHistory) &&
                    scopeHistory.TryGetValue(experimentKey, out retained))
                {
                    keys.AddRange(retained);
                }

                keys.Sort(StringComparer.Ordinal);
                return keys;
            }
        }

        internal bool RemintedForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _reminted;
                }
            }
        }
    }
}
