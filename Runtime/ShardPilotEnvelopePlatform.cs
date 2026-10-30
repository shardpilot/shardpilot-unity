using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>
    /// Normalizes emitted SDK platforms while preserving the existing aliases.
    /// Unknown and empty values use the explicit fallback. Fresh analytics and
    /// crash writers share this closed vocabulary.
    /// </summary>
    internal static class ShardPilotEnvelopePlatform
    {
        private static readonly Dictionary<string, string> Vocabulary =
            new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "tvos",      "tvos" },
            { "ps4",       "ps4" },
            { "ps5",       "ps5" },
            { "xbox",      "xbox" },
            { "switch",    "switch" },
            { "other",     "other" },
            { "android",   "android" },
            { "browser",   "web" },
            { "darwin",    "macos" },
            { "html5",     "web" },
            { "ios",       "ios" },
            { "ipad",      "ios" },
            { "ipados",    "ios" },
            { "iphone",    "ios" },
            { "linux",     "linux" },
            { "mac",       "macos" },
            { "macos",     "macos" },
            { "macosx",    "macos" },
            { "osx",       "macos" },
            { "steamdeck", "linux" },
            { "web",       "web" },
            { "win",       "windows" },
            { "win32",     "windows" },
            { "win64",     "windows" },
            { "windows",   "windows" },
        };

        /// <summary>Returns a canonical platform value, including the fallback.</summary>
        internal static string Normalize(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return "other";
            }

            var folded = value.Trim().ToLowerInvariant();
            string canonical;
            return Vocabulary.TryGetValue(folded, out canonical) ? canonical : "other";
        }

        /// <summary>
        /// True when the host SET a value that this vocabulary does not know --
        /// the case worth reporting. An unset value is the ordinary default path
        /// (detection answers for it) and is deliberately not reported: a warning
        /// there would fire on every correctly configured game, which is how a
        /// diagnostic gets switched off before the day it matters.
        /// </summary>
        internal static bool IsSetButUnmapped(string value)
        {
            return !string.IsNullOrEmpty(value) && !Vocabulary.ContainsKey(value.Trim().ToLowerInvariant());
        }
    }
}
