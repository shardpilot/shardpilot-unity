#nullable enable annotations
using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>
    /// The typed ad verb: <c>ad_impression_revenue</c>, built from typed
    /// arguments and validated against the canonical schema
    /// (analytics.ad_impression_revenue.v1) before anything is queued.
    ///
    /// Unlike the progression family this verb takes NO extras map: the
    /// schema sets <c>additionalProperties: false</c>, so any key it does not
    /// declare makes the ingest reject the event. A verb that accepted extras
    /// would build events that cannot be accepted.
    ///
    /// Consent has TWO gates here and the SDK owns only one. The analytics
    /// gate remains unchanged for this typed producer. On top of it the ingest
    /// requires the workspace's own <c>ad_revenue</c> consent posture
    /// and suppresses the event per event, inside an ACCEPTED
    /// batch, with status <c>suppressed_ad_revenue_consent</c> when that
    /// grant is missing. This SDK cannot see or set that grant — its consent
    /// wire body carries the analytics decision only. Nor does it hand the
    /// verdict back: the batch result is internal and <see cref="Snapshot"/>
    /// counts a suppression under neither <c>Accepted</c> nor <c>Rejected</c>.
    /// The log carries it — the client writes one line per non-accepted event
    /// to <c>ShardPilotConfig.Logger</c>, naming the status — so a caller who
    /// needs to know whether an impression was ingested watches for that
    /// line.
    /// </summary>
    public sealed partial class ShardPilotClient
    {
        private const string AdImpressionRevenueEventName = "ad_impression_revenue";

        // The schema's own bounds.
        private const int MaxImpressionIdLength = 256;
        private const int MaxNetworkLength = 128;
        private const int CurrencyLength = 3;
        private const int MaxRevenuePrecisionLength = 64;
        private const int MaxAdUnitLength = 256;
        private const int MaxAdFormatLength = 64;
        private const int MaxPlacementLength = 256;

        public bool TrackAdImpressionRevenue(ShardPilotAdImpressionRevenueArgs args)
        {
            string ignored;
            return TrackAdImpressionRevenue(args, out ignored);
        }

        /// <summary>Emits the named impression after validating its required
        /// identifiers and nonnegative revenue. Currency is upper-cased;
        /// empty optional fields are omitted and all fields retain their
        /// canonical schema bounds.</summary>
        public bool TrackAdImpressionRevenue(ShardPilotAdImpressionRevenueArgs args, out string? error)
        {
            error = null;

            var impression = ShardPilotConfig.Trim(args.ImpressionId);
            if (string.IsNullOrEmpty(impression) || CodePointLength(impression) > MaxImpressionIdLength)
            {
                error = "invalid_impression_id";
                return false;
            }
            var adNetwork = ShardPilotConfig.Trim(args.Network);
            if (string.IsNullOrEmpty(adNetwork) || CodePointLength(adNetwork) > MaxNetworkLength)
            {
                error = "invalid_network";
                return false;
            }
            if (args.RevenueMicros < 0)
            {
                error = "invalid_revenue_micros";
                return false;
            }
            // The schema fixes the length at three: currency is an ISO 4217
            // code, and the fact layer groups revenue by the exact string, so
            // it is upper-cased here exactly as the Go SDK's purchase verb does.
            var code = ShardPilotConfig.Trim(args.Currency);
            if (CodePointLength(code) != CurrencyLength)
            {
                error = "invalid_currency";
                return false;
            }
            code = code.ToUpperInvariant();

            var props = new Dictionary<string, object>(8, StringComparer.Ordinal)
            {
                { "impression_id", impression },
                { "network", adNetwork },
                { "revenue_micros", args.RevenueMicros },
                { "currency", code }
            };
            if (!AddBoundedOptional(props, "revenue_precision", args.RevenuePrecision, MaxRevenuePrecisionLength, ref error)
                || !AddBoundedOptional(props, "ad_unit", args.AdUnit, MaxAdUnitLength, ref error)
                || !AddBoundedOptional(props, "ad_format", args.AdFormat, MaxAdFormatLength, ref error)
                || !AddBoundedOptional(props, "placement", args.Placement, MaxPlacementLength, ref error))
            {
                return false;
            }

            return EnqueueTypedEvent(AdImpressionRevenueEventName, props, out error);
        }

        /// <summary>The schema's lengths count Unicode CODE POINTS, while
        /// <c>string.Length</c> counts UTF-16 code units: a non-BMP character
        /// — an emoji in a placement name — is two units and one code point,
        /// so a byte- or unit-based check refuses a value the schema accepts.
        /// All bounded ad fields use this code-point measure.</summary>
        private static int CodePointLength(string value)
        {
            var count = 0;
            for (var index = 0; index < value.Length; index++)
            {
                // A low surrogate is folded into the character before it ONLY
                // when it completes the pair that high surrogate opened. An
                // UNPAIRED surrogate counts as one, because the wire is what
                // the schema measures: this string is UTF-16 in memory and
                // Encoding.UTF8.GetBytes replaces each unpaired unit with one
                // U+FFFD, so a value that skipped them here arrives LONGER
                // than it measured and the ingest rejects the event
                // terminally while the verb returned true. Front-truncating a
                // mediation network's string (Substring past a pair's high
                // half) is the ordinary way to produce one.
                if (char.IsHighSurrogate(value[index])
                    && index + 1 < value.Length
                    && char.IsLowSurrogate(value[index + 1]))
                {
                    index++;
                }

                count++;
            }

            return count;
        }

        /// <summary>Adds an optional string prop when it carries a value,
        /// refusing one longer than the schema allows. An empty value is
        /// absent, never an empty string on the wire.</summary>
        private static bool AddBoundedOptional(IDictionary<string, object> props, string key, string value,
            int maxLength, ref string error)
        {
            var trimmed = ShardPilotConfig.Trim(value);
            if (string.IsNullOrEmpty(trimmed))
            {
                return true;
            }
            if (CodePointLength(trimmed) > maxLength)
            {
                error = "invalid_" + key;
                return false;
            }
            props[key] = trimmed;
            return true;
        }
    }
}
