using System;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    internal static class ShardPilotIds
    {
        /// <summary>
        /// UUIDv5 of the UTF-8 exit-record key in the fixed namespace
        /// cea0c4a2-91da-4d42-afc5-e3c913e97c43. Replay preserves this identity.
        /// </summary>
        public static string FromExitRecordKey(string recordKey)
        {
            // RFC network order, rather than Guid.ToByteArray's mixed order.
            var namespaceBytes = new byte[]
            {
                0xce, 0xa0, 0xc4, 0xa2, 0x91, 0xda, 0x4d, 0x42,
                0xaf, 0xc5, 0xe3, 0xc9, 0x13, 0xe9, 0x7c, 0x43
            };
            var nameBytes = Encoding.UTF8.GetBytes(recordKey);
            var input = new byte[namespaceBytes.Length + nameBytes.Length];
            namespaceBytes.CopyTo(input, 0);
            nameBytes.CopyTo(input, namespaceBytes.Length);

            using (var sha = SHA1.Create())
            {
                var digest = sha.ComputeHash(input);
                digest[6] = (byte)((digest[6] & 0x0f) | 0x50);
                digest[8] = (byte)((digest[8] & 0x3f) | 0x80);
                var builder = new StringBuilder(36);
                for (var i = 0; i < 16; i++)
                {
                    if (i == 4 || i == 6 || i == 8 || i == 10)
                    {
                        builder.Append('-');
                    }

                    builder.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }

                return builder.ToString();
            }
        }

        public static string NewUuidV7()
        {
            return NewUuidV7(DateTimeOffset.UtcNow);
        }

        public static string NewUuidV7(DateTimeOffset now)
        {
            var bytes = new byte[16];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(bytes);
            }

            var millis = now.ToUnixTimeMilliseconds();
            bytes[0] = (byte)(millis >> 40);
            bytes[1] = (byte)(millis >> 32);
            bytes[2] = (byte)(millis >> 24);
            bytes[3] = (byte)(millis >> 16);
            bytes[4] = (byte)(millis >> 8);
            bytes[5] = (byte)millis;
            bytes[6] = (byte)((bytes[6] & 0x0f) | 0x70);
            bytes[8] = (byte)((bytes[8] & 0x3f) | 0x80);

            return string.Format(CultureInfo.InvariantCulture,
                "{0:x2}{1:x2}{2:x2}{3:x2}-{4:x2}{5:x2}-{6:x2}{7:x2}-{8:x2}{9:x2}-{10:x2}{11:x2}{12:x2}{13:x2}{14:x2}{15:x2}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
        }

        /// <summary>
        /// The largest Unix millisecond timestamp <see cref="DateTimeOffset"/>
        /// can represent (9999-12-31T23:59:59.999Z). A UUIDv7's 48 timestamp
        /// bits can encode values past this, and feeding one to
        /// <see cref="DateTimeOffset.FromUnixTimeMilliseconds"/> throws.
        /// </summary>
        private static readonly long MaxUnixTimeMilliseconds = DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();

        /// <summary>
        /// Extracts the millisecond Unix timestamp a UUIDv7 was minted with
        /// (its first 48 bits). Returns false — never throws — for anything
        /// that is not a well-formed UUIDv7, or whose encoded timestamp is
        /// out of <see cref="DateTimeOffset"/> range; the caller decides what
        /// an undatable id means (the crash spool's retention window treats
        /// it as expired: retention must be provable).
        /// </summary>
        public static bool TryGetUuidV7TimestampUtc(string value, out DateTime timestampUtc)
        {
            timestampUtc = default(DateTime);
            if (!IsUuidV7(value))
            {
                return false;
            }

            long millis = 0;
            // The first 48 bits are the hex chars at positions 0-7 and 9-12
            // ("xxxxxxxx-xxxx-..."), big-endian milliseconds since the epoch.
            for (var i = 0; i < 13; i++)
            {
                if (i == 8)
                {
                    continue;
                }

                millis = (millis << 4) | (long)Uri.FromHex(value[i]);
            }

            // 48 bits can encode ~year 10889; DateTimeOffset stops at year
            // 9999. Out of range = undatable, NOT an exception: this runs
            // inside the crash spool's load path, where a throw would take
            // the whole pending backlog down with one corrupt line.
            if (millis > MaxUnixTimeMilliseconds)
            {
                return false;
            }

            timestampUtc = DateTimeOffset.FromUnixTimeMilliseconds(millis).UtcDateTime;
            return true;
        }

        public static bool IsUuidV7(string value)
        {
            if (value == null || value.Length != 36)
            {
                return false;
            }

            for (var i = 0; i < value.Length; i++)
            {
                if (i == 8 || i == 13 || i == 18 || i == 23)
                {
                    if (value[i] != '-')
                    {
                        return false;
                    }
                    continue;
                }

                if (!Uri.IsHexDigit(value[i]))
                {
                    return false;
                }
            }

            return value[14] == '7' &&
                   (value[19] == '8' || value[19] == '9' || value[19] == 'a' ||
                    value[19] == 'A' || value[19] == 'b' || value[19] == 'B');
        }
    }
}
