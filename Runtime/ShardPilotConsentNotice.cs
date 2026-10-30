using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>
    /// Notice provenance for one consent decision; never the notice text.
    /// An absent nullable carrier means omission. A supplied empty carrier
    /// is malformed. Setter and receipt integration is a separate step.
    /// </summary>
#nullable enable annotations
    public struct ShardPilotConsentNotice
    {
        public string? NoticeVersion;
        public string? NoticeLocale;
        public string? PolicyVersion;
    }

#nullable restore annotations
    internal sealed class ShardPilotConsentNoticeSnapshot
    {
        internal readonly string NoticeVersion;
        internal readonly string NoticeLocale;
        internal readonly string PolicyVersion;

        internal ShardPilotConsentNoticeSnapshot(string version, string locale, string policy)
        {
            NoticeVersion = version;
            NoticeLocale = locale;
            PolicyVersion = policy;
        }
    }

    internal static class ShardPilotConsentNoticeValidator
    {
        // RFC 5646 section 2.1: this closed grandfathered set never changes.
        // Registry membership, registered prefixes and the stricter extlang
        // validity rules are server concerns; the SDK checks the complete ABNF.
        private static readonly HashSet<string> Grandfathered = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "en-GB-oed", "i-ami", "i-bnn", "i-default", "i-enochian", "i-hak", "i-klingon",
            "i-lux", "i-mingo", "i-navajo", "i-pwn", "i-tao", "i-tay", "i-tsu",
            "sgn-BE-FR", "sgn-BE-NL", "sgn-CH-DE", "art-lojban", "cel-gaulish",
            "no-bok", "no-nyn", "zh-guoyu", "zh-hakka", "zh-min", "zh-min-nan", "zh-xiang"
        };

        /// <summary>
        /// Copies the complete tuple or none. Invalid grants are refused;
        /// invalid denials remain permitted with a notice-loss warning.
        /// This helper itself changes no consent state or receipt.
        /// </summary>
        internal static bool TrySnapshot(ShardPilotConsentNotice? notice, bool granted,
            out ShardPilotConsentNoticeSnapshot snapshot, out string error, out string warning)
        {
            snapshot = null;
            error = null;
            warning = null;
            if (!notice.HasValue) return true;
            var value = notice.Value;
            if (IsToken(value.NoticeVersion, 1, 64, IsVersionCharacter) &&
                IsLocale(value.NoticeLocale) && IsToken(value.PolicyVersion, 1, 64, IsVersionCharacter))
            {
                snapshot = new ShardPilotConsentNoticeSnapshot(value.NoticeVersion, value.NoticeLocale, value.PolicyVersion);
                return true;
            }
            if (granted) { error = "consent_notice_invalid"; return false; }
            warning = "consent_notice_invalid";
            return true;
        }

        private static bool IsLocale(string locale)
        {
            if (locale == null || locale.Length > 35) return false;
            // Reject non-ASCII before case folding, whose behavior varies by runtime.
            foreach (var character in locale) if (character > 0x7f) return false;
            if (Grandfathered.Contains(locale)) return true;
            var parts = locale.Split('-');
            if (string.Equals(parts[0], "x", StringComparison.OrdinalIgnoreCase)) return PrivateUse(parts, 0);
            if (!IsToken(parts[0], 2, 8, IsAlpha)) return false;
            var index = 1;
            if (parts[0].Length <= 3)
            {
                var extlangs = 0;
                while (index < parts.Length && extlangs < 3 && IsToken(parts[index], 3, 3, IsAlpha))
                { index++; extlangs++; }
            }
            if (index < parts.Length && IsToken(parts[index], 4, 4, IsAlpha)) index++;
            if (index < parts.Length && (IsToken(parts[index], 2, 2, IsAlpha) || IsToken(parts[index], 3, 3, IsDigit))) index++;
            var variants = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (index < parts.Length && IsVariant(parts[index]))
            {
                if (!variants.Add(parts[index++])) return false;
            }
            var singletons = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (index < parts.Length && IsToken(parts[index], 1, 1, IsAlphanumeric) &&
                !string.Equals(parts[index], "x", StringComparison.OrdinalIgnoreCase))
            {
                if (!singletons.Add(parts[index++])) return false;
                var start = index;
                while (index < parts.Length && IsToken(parts[index], 2, 8, IsAlphanumeric)) index++;
                if (start == index) return false;
            }
            return index == parts.Length || PrivateUse(parts, index);
        }

        private static bool PrivateUse(string[] parts, int index)
        {
            if (index >= parts.Length - 1 || !string.Equals(parts[index], "x", StringComparison.OrdinalIgnoreCase)) return false;
            for (var i = index + 1; i < parts.Length; i++)
                if (!IsToken(parts[i], 1, 8, IsAlphanumeric)) return false;
            return true;
        }

        private static bool IsVariant(string value)
        {
            return IsToken(value, 5, 8, IsAlphanumeric) ||
                (value.Length == 4 && IsDigit(value[0]) && IsToken(value, 4, 4, IsAlphanumeric));
        }

        private static bool IsToken(string value, int minimum, int maximum, Func<char, bool> allowed)
        {
            // Every accepted character is ASCII, so these lengths are also
            // exact byte lengths. Non-ASCII input is refused, never trimmed.
            if (value == null || value.Length < minimum || value.Length > maximum) return false;
            foreach (var character in value) if (!allowed(character)) return false;
            return true;
        }

        private static bool IsAlpha(char value)
        {
            return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
        }

        private static bool IsDigit(char value)
        {
            return value >= '0' && value <= '9';
        }

        private static bool IsAlphanumeric(char value)
        {
            return IsAlpha(value) || IsDigit(value);
        }

        private static bool IsVersionCharacter(char value)
        {
            return IsAlphanumeric(value) || value == '.' || value == '_' || value == '+' || value == '/' || value == '-';
        }
    }
}
