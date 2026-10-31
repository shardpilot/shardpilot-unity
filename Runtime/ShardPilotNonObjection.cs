using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace ShardPilot
{
    // Captured host data is separate from consent, receipts and admission authority.
    internal static class ShardPilotNonObjection
    {
        internal sealed class SnapshotValue
        {
            private readonly string _json;
            internal SnapshotValue(Dictionary<string, object> fields) { _json = ShardPilotJson.CaptureMap(fields); }
            internal Dictionary<string, object> ToMap()
            {
                Dictionary<string, object> fields;
                if (!ShardPilotJsonParser.TryParseObject(_json, out fields)) throw new InvalidOperationException("Invalid captured non-objection data.");
                return fields;
            }
        }

        private static Func<object, object> Literal(object expected) => value => expected.Equals(value) ? value : null;
        private static Func<object, object> Text(int minimum, int maximum) => value =>
        {
            if (!(value is string text)) return null;
            var count = 0;
            for (var i = 0; i < text.Length; i++)
            {
                if (char.IsHighSurrogate(text[i])) { if (++i >= text.Length || !char.IsLowSurrogate(text[i])) return null; }
                else if (char.IsLowSurrogate(text[i])) return null;
                if (++count > maximum) return null;
            }
            return count >= minimum ? text : null;
        };
        private static Func<object, object> Hex(int length) => value => value is string text &&
            Regex.IsMatch(text, @"\A[a-f0-9]{" + length + @"}\z") ? text : null;
        private static Func<object, object> Object(Dictionary<string, Func<object, object>> shape) => value =>
        {
            var source = value as IDictionary<string, object>;
            if (source == null || source.Count != shape.Count || source.Keys.Any(key => !shape.ContainsKey(key))) return null;
            var copy = new Dictionary<string, object>(StringComparer.Ordinal);
            foreach (var field in shape)
            {
                if (!source.TryGetValue(field.Key, out var raw)) return null;
                var item = field.Value(raw); if (item == null) return null;
                copy.Add(field.Key, item);
            }
            return copy;
        };

        private static readonly Func<object, object> Matrix = Object(new Dictionary<string, Func<object, object>>
        {
            ["docs_commit"] = Hex(40), ["file_sha256"] = Hex(64),
            ["date"] = value => value is string date && ShardPilotConsentPolicy.CalendarDate(date) ? date : null
        });
        private static readonly Func<object, object> Host = Object(new Dictionary<string, Func<object, object>>
        {
            ["advisory"] = Object(new Dictionary<string, Func<object, object>>
            {
                ["matrix"] = Matrix, ["row_id"] = Text(1, 64),
                ["jurisdiction"] = value => value is string text && Regex.IsMatch(text, @"\A[A-Z]{2}\z") ? text : null
            }),
            ["age_evidence_class"] = Literal("eligible_adult"),
            ["attestation"] = Object(new Dictionary<string, Func<object, object>>
            {
                ["notice_provided"] = Literal(true), ["free_simple_objection_provided"] = Literal(true),
                ["did_not_object"] = Literal(true), ["eligible"] = Literal(true)
            }),
            ["notice_version"] = Text(1, 64), ["notice_locale"] = Text(2, 35), ["policy_version"] = Text(1, 64)
        });
        private static readonly Func<object, object> Acknowledgement = Object(new Dictionary<string, Func<object, object>>
        {
            ["recorded"] = Literal(true), ["replayed"] = value => value is bool ? value : null,
            ["admission_basis"] = Literal("advisory_non_objection")
        });

        internal static SnapshotValue Snapshot(object value, out string error)
        {
            error = "non_objection_invalid";
            try
            {
                var fields = Host(value) as Dictionary<string, object>; if (fields == null) return null;
                var notice = new ShardPilotConsentNotice { NoticeVersion = (string)fields["notice_version"],
                    NoticeLocale = (string)fields["notice_locale"], PolicyVersion = (string)fields["policy_version"] };
                if (!ShardPilotConsentNoticeValidator.TrySnapshot(notice, true, out _, out _, out _)) return null;
                var snapshot = new SnapshotValue(fields); error = null; return snapshot;
            }
            catch { return null; }
        }

        // Decoded shape only. Transport, status and credential handling belong to the caller.
        internal static SnapshotValue Response(object value, out string error)
        {
            error = "non_objection_response_invalid";
            try
            {
                var fields = Acknowledgement(value) as Dictionary<string, object>; if (fields == null) return null;
                var snapshot = new SnapshotValue(fields); error = null; return snapshot;
            }
            catch { return null; }
        }
    }
}
