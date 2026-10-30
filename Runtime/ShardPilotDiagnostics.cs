using System;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    internal static class ShardPilotDiagnostics
    {
        internal static void Log(ShardPilotConfig config, ShardPilotLogLevel level, string message, string requestBearer = null)
        {
            if (config == null) return;
            var threshold = (int)config.LogLevel;
            var severity = (int)level;
            if (threshold < 0 || threshold >= (int)ShardPilotLogLevel.Off ||
                severity < threshold || severity >= (int)ShardPilotLogLevel.Off) return;
            try
            {
                var text = Redact(config, message ?? string.Empty, requestBearer);
                if (config.Logger != null) config.Logger.Log(level, text);
                else if (level == ShardPilotLogLevel.Error) UnityEngine.Debug.LogError(text);
                else if (level == ShardPilotLogLevel.Warn) UnityEngine.Debug.LogWarning(text);
                else UnityEngine.Debug.Log(text);
            }
            catch
            {
                // Diagnostic sinks cannot change SDK delivery semantics.
            }
        }

        internal static string Redact(ShardPilotConfig config, string text, string requestBearer = null)
        {
            if (text == null) return null;
            var secrets = new[] { config?.PublishableKey, config?.CrashKey, requestBearer };
            // A short configured key may be a prefix of another credential.
            Array.Sort(secrets, (left, right) => (right?.Length ?? 0).CompareTo(left?.Length ?? 0));
            foreach (var secret in secrets)
                if (!string.IsNullOrEmpty(secret)) text = text.Replace(secret, "<redacted>");
            // The replacement itself (or an assembled boundary) must not expose
            // a credential. Suppress any diagnostic that still contains one.
            foreach (var secret in secrets)
                if (!string.IsNullOrEmpty(secret) && text.Contains(secret)) return string.Empty;
            return text;
        }

        internal static string WarningCodeKey(string code)
        {
            // Keep the existing bounded distinct-code census without retaining
            // server text that could itself be an echoed bearer.
            using (var hash = SHA256.Create())
                return Convert.ToBase64String(hash.ComputeHash(Encoding.UTF8.GetBytes(code ?? string.Empty)));
        }
    }
}
