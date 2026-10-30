using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace ShardPilot
{
    /// <summary>
    /// Minimal allocation-conscious JSON reader used for the remote-config
    /// payload and its disk cache. Objects parse to Dictionary&lt;string, object&gt;,
    /// arrays to List&lt;object&gt;, numbers to long (integral) or double, plus
    /// string, bool, and null. Pure C#: no Unity dependencies.
    /// </summary>
    internal static class ShardPilotJsonParser
    {
        private const int MaxDepth = 64;

        public static bool TryParse(string text, out object value)
        {
            value = null;
            if (string.IsNullOrEmpty(text))
            {
                return false;
            }

            try
            {
                var index = 0;
                var parsed = ParseValue(text, ref index, 0);
                SkipWhitespace(text, ref index);
                if (index != text.Length)
                {
                    return false;
                }

                value = parsed;
                return true;
            }
            catch (FormatException)
            {
                return false;
            }
        }

        public static bool TryParseObject(string text, out Dictionary<string, object> value)
        {
            value = null;
            object parsed;
            if (!TryParse(text, out parsed))
            {
                return false;
            }

            value = parsed as Dictionary<string, object>;
            return value != null;
        }

        // Opt-in strict reader for signed objects. The ordinary reader retains
        // its compatibility behavior; this path rejects ambiguous decoded keys
        // at every depth and exposes exact root value tokens for forwarding.
        internal static bool TryParseUniqueObject(string text, out Dictionary<string, object> value,
            out Dictionary<string, string> raw)
        {
            value = null;
            raw = null;
            if (string.IsNullOrEmpty(text)) return false;
            try
            {
                var index = 0;
                SkipWhitespace(text, ref index);
                if (Peek(text, index) != '{') return false;
                var tokens = new Dictionary<string, string>(StringComparer.Ordinal);
                var parsed = ParseObject(text, ref index, 0, true, tokens);
                SkipWhitespace(text, ref index);
                if (index != text.Length) return false;
                value = parsed;
                raw = tokens;
                return true;
            }
            catch (FormatException) { return false; }
            catch (EncoderFallbackException) { return false; }
        }

        // Preserve every value byte while renaming only an unambiguous root key.
        internal static bool TryMigrateCrashComponent(string text, out string migrated)
        {
            migrated = text;
            Dictionary<string, object> parsed;
            if (!TryParseObject(text, out parsed)) return false;
            try
            {
                var index = 0;
                SkipWhitespace(text, ref index);
                index++;
                var sourceStart = -1;
                var sourceEnd = -1;
                var componentPresent = false;
                var keys = new HashSet<string>(StringComparer.Ordinal);
                while (true)
                {
                    SkipWhitespace(text, ref index);
                    if (Peek(text, index) == '}') break;
                    var start = index;
                    var key = ParseString(text, ref index);
                    var end = index;
                    if (!keys.Add(key)) return false;
                    SkipWhitespace(text, ref index);
                    index++; // ':'; the complete document was validated above.
                    var value = ParseValue(text, ref index, 1);
                    if (key == "source")
                    {
                        if (!(value is string) || !ShardPilotConfig.IsCrashSourceSlug((string)value)) return false;
                        sourceStart = start;
                        sourceEnd = end;
                    }
                    if (key == "component") componentPresent = true;
                    SkipWhitespace(text, ref index);
                    if (Peek(text, index) == '}') break;
                    index++; // ','
                }
                if (sourceStart < 0) return true;
                if (componentPresent) return false;
                migrated = text.Substring(0, sourceStart) + "\"component\"" + text.Substring(sourceEnd);
                return true;
            }
            catch (FormatException)
            {
                return false;
            }
        }

        private static object ParseValue(string text, ref int index, int depth, bool strict = false)
        {
            if (depth > MaxDepth)
            {
                throw new FormatException("json nesting too deep");
            }

            SkipWhitespace(text, ref index);
            if (index >= text.Length)
            {
                throw new FormatException("unexpected end of json");
            }

            switch (text[index])
            {
                case '{':
                    return ParseObject(text, ref index, depth, strict);
                case '[':
                    return ParseArray(text, ref index, depth, strict);
                case '"':
                    return ParseString(text, ref index, strict);
                case 't':
                    Expect(text, ref index, "true");
                    return true;
                case 'f':
                    Expect(text, ref index, "false");
                    return false;
                case 'n':
                    Expect(text, ref index, "null");
                    return null;
                default:
                    return ParseNumber(text, ref index, strict);
            }
        }

        private static Dictionary<string, object> ParseObject(string text, ref int index, int depth, bool strict = false,
            Dictionary<string, string> raw = null)
        {
            var result = new Dictionary<string, object>(StringComparer.Ordinal);
            index++; // '{'
            SkipWhitespace(text, ref index);
            if (Peek(text, index) == '}')
            {
                index++;
                return result;
            }

            while (true)
            {
                SkipWhitespace(text, ref index);
                if (Peek(text, index) != '"')
                {
                    throw new FormatException("expected object key");
                }

                var key = ParseString(text, ref index, strict);
                if (strict && result.ContainsKey(key)) throw new FormatException("duplicate object key");
                SkipWhitespace(text, ref index);
                if (Peek(text, index) != ':')
                {
                    throw new FormatException("expected ':' after object key");
                }

                index++;
                SkipWhitespace(text, ref index);
                var start = index;
                result[key] = ParseValue(text, ref index, depth + 1, strict);
                if (raw != null) raw.Add(key, text.Substring(start, index - start));
                SkipWhitespace(text, ref index);
                var ch = Peek(text, index);
                if (ch == ',')
                {
                    index++;
                    continue;
                }

                if (ch == '}')
                {
                    index++;
                    return result;
                }

                throw new FormatException("expected ',' or '}' in object");
            }
        }

        private static List<object> ParseArray(string text, ref int index, int depth, bool strict = false)
        {
            var result = new List<object>();
            index++; // '['
            SkipWhitespace(text, ref index);
            if (Peek(text, index) == ']')
            {
                index++;
                return result;
            }

            while (true)
            {
                result.Add(ParseValue(text, ref index, depth + 1, strict));
                SkipWhitespace(text, ref index);
                var ch = Peek(text, index);
                if (ch == ',')
                {
                    index++;
                    continue;
                }

                if (ch == ']')
                {
                    index++;
                    return result;
                }

                throw new FormatException("expected ',' or ']' in array");
            }
        }

        private static string ParseString(string text, ref int index, bool strict = false)
        {
            index++; // opening quote
            var builder = new StringBuilder();
            while (true)
            {
                if (index >= text.Length)
                {
                    throw new FormatException("unterminated string");
                }

                var ch = text[index++];
                if (ch == '"')
                {
                    var value = builder.ToString();
                    if (strict) new UTF8Encoding(false, true).GetByteCount(value);
                    return value;
                }

                if (strict && ch < 0x20) throw new FormatException("unescaped control character");
                if (ch != '\\')
                {
                    builder.Append(ch);
                    continue;
                }

                if (index >= text.Length)
                {
                    throw new FormatException("unterminated escape");
                }

                var escape = text[index++];
                switch (escape)
                {
                    case '"':
                        builder.Append('"');
                        break;
                    case '\\':
                        builder.Append('\\');
                        break;
                    case '/':
                        builder.Append('/');
                        break;
                    case 'b':
                        builder.Append('\b');
                        break;
                    case 'f':
                        builder.Append('\f');
                        break;
                    case 'n':
                        builder.Append('\n');
                        break;
                    case 'r':
                        builder.Append('\r');
                        break;
                    case 't':
                        builder.Append('\t');
                        break;
                    case 'u':
                        if (index + 4 > text.Length)
                        {
                            throw new FormatException("truncated unicode escape");
                        }

                        if (strict && !Regex.IsMatch(text.Substring(index, 4), @"\A[0-9a-fA-F]{4}\z"))
                            throw new FormatException("invalid unicode escape");
                        int code;
                        if (!int.TryParse(
                                text.Substring(index, 4),
                                NumberStyles.HexNumber,
                                CultureInfo.InvariantCulture,
                                out code))
                        {
                            throw new FormatException("invalid unicode escape");
                        }

                        index += 4;
                        builder.Append((char)code);
                        break;
                    default:
                        throw new FormatException("invalid escape '\\" + escape + "'");
                }
            }
        }

        private static object ParseNumber(string text, ref int index, bool strict = false)
        {
            var start = index;
            while (index < text.Length && IsNumberChar(text[index]))
            {
                index++;
            }

            if (index == start)
            {
                throw new FormatException("unexpected character '" + text[start] + "'");
            }

            var token = text.Substring(start, index - start);
            if (strict && !Regex.IsMatch(token, @"\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\z"))
                throw new FormatException("invalid number token");
            if (token.IndexOf('.') < 0 && token.IndexOf('e') < 0 && token.IndexOf('E') < 0)
            {
                long integral;
                if (long.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out integral))
                {
                    return integral;
                }
            }

            double number;
            if (!double.TryParse(token, NumberStyles.Float, CultureInfo.InvariantCulture, out number))
            {
                throw new FormatException("invalid number '" + token + "'");
            }

            if (strict && (double.IsNaN(number) || double.IsInfinity(number)))
                throw new FormatException("non-finite number");
            return number;
        }

        private static bool IsNumberChar(char ch)
        {
            return ch >= '0' && ch <= '9' || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E';
        }

        private static void Expect(string text, ref int index, string literal)
        {
            if (index + literal.Length > text.Length ||
                string.CompareOrdinal(text, index, literal, 0, literal.Length) != 0)
            {
                throw new FormatException("invalid literal");
            }

            index += literal.Length;
        }

        private static char Peek(string text, int index)
        {
            return index < text.Length ? text[index] : '\0';
        }

        private static void SkipWhitespace(string text, ref int index)
        {
            while (index < text.Length)
            {
                var ch = text[index];
                if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n')
                {
                    return;
                }

                index++;
            }
        }
    }
}
