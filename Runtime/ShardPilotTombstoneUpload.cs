using System;
using System.Globalization;
using System.Text;

namespace ShardPilot
{
    /// <summary>
    /// Wire constants and the multipart body builder for the Android
    /// tombstone lane:
    /// <c>POST {crashBase}/api/v1/crashes/ingest-tombstone</c> with the
    /// <c>crash:write</c> Bearer and a two-part <c>multipart/form-data</c>
    /// body — a <c>metadata</c> envelope JSON part (the SAME envelope
    /// fields as the JSON crash lane, minus the stack-bearing fields the
    /// server refuses on this lane) plus exactly one <c>tombstone</c> file
    /// part carrying the RAW AOSP <c>Tombstone</c> protobuf bytes,
    /// UNMODIFIED. The server owns parsing, allowlist projection, and
    /// privacy: the SDK never decodes, never strips,
    /// and NEVER truncates the proto — a truncated protobuf is unparseable,
    /// so an over-cap tombstone is not uploaded at all.
    /// </summary>
    internal static class ShardPilotTombstoneUpload
    {
        /// <summary>
        /// The lane's route, appended to the configured crash base URL —
        /// the exact-path sibling of <c>/api/v1/crashes/ingest</c>.
        /// </summary>
        public const string IngestPath = "/api/v1/crashes/ingest-tombstone";

        /// <summary>
        /// The client-side HARD cap on the raw tombstone part — the
        /// server's 4 MiB <c>TombstonePayloadMaxBytes</c> lane cap
        /// (deliberate headroom, tombstones are typically
        /// &lt; 1 MiB). A stream past this cap is never read into a wire
        /// body and never truncated: the record degrades to the
        /// metadata-only JSON report instead.
        /// </summary>
        public const int MaxTombstoneBytes = 4 * 1024 * 1024;

        /// <summary>The metadata envelope part name the server requires.</summary>
        public const string MetadataPartName = "metadata";

        /// <summary>The raw tombstone file part name the server requires.</summary>
        public const string TombstonePartName = "tombstone";

        /// <summary>
        /// Builds the <c>Content-Type</c> header value declaring
        /// <paramref name="boundary"/>.
        /// </summary>
        public static string ContentTypeFor(string boundary)
        {
            return "multipart/form-data; boundary=" + boundary;
        }

        /// <summary>
        /// A fresh multipart boundary: a fixed marker plus 32 random hex
        /// digits. The alphabet is RFC 2046 bchars-safe, and the entropy
        /// makes a collision with the metadata JSON or the binary protobuf
        /// bytes negligible (the standard multipart posture).
        /// </summary>
        public static string NewBoundary()
        {
            var random = Guid.NewGuid().ToByteArray();
            var builder = new StringBuilder("shardpilot-tombstone-", 21 + 32);
            for (var i = 0; i < random.Length; i++)
            {
                builder.Append(random[i].ToString("x2", CultureInfo.InvariantCulture));
            }

            return builder.ToString();
        }

        /// <summary>
        /// Assembles the two-part body: the <c>metadata</c> part as a plain
        /// form FIELD (no filename — the server accepts the envelope as a
        /// form value or a file part; a field keeps the framing minimal)
        /// with an explicit <c>application/json</c> content type, then the
        /// single <c>tombstone</c> FILE part (a filename is what makes it a
        /// file part server-side) as <c>application/octet-stream</c>. The
        /// tombstone bytes are copied VERBATIM — byte-for-byte, no
        /// re-encoding, no transformation — per the server-parse contract.
        /// </summary>
        public static byte[] BuildMultipartBody(string metadataJson, byte[] tombstoneBytes, string boundary)
        {
            if (metadataJson == null)
            {
                throw new ArgumentNullException("metadataJson");
            }

            if (tombstoneBytes == null || tombstoneBytes.Length == 0)
            {
                throw new ArgumentException("tombstone bytes are required", "tombstoneBytes");
            }

            if (string.IsNullOrEmpty(boundary))
            {
                throw new ArgumentException("boundary is required", "boundary");
            }

            var head = new StringBuilder();
            head.Append("--").Append(boundary).Append("\r\n");
            head.Append("Content-Disposition: form-data; name=\"").Append(MetadataPartName).Append("\"\r\n");
            head.Append("Content-Type: application/json; charset=utf-8\r\n");
            head.Append("\r\n");
            head.Append(metadataJson);
            head.Append("\r\n");
            head.Append("--").Append(boundary).Append("\r\n");
            head.Append("Content-Disposition: form-data; name=\"").Append(TombstonePartName)
                .Append("\"; filename=\"tombstone.pb\"\r\n");
            head.Append("Content-Type: application/octet-stream\r\n");
            head.Append("\r\n");

            var tail = "\r\n--" + boundary + "--\r\n";

            var headBytes = Encoding.UTF8.GetBytes(head.ToString());
            var tailBytes = Encoding.UTF8.GetBytes(tail);
            var body = new byte[headBytes.Length + tombstoneBytes.Length + tailBytes.Length];
            Buffer.BlockCopy(headBytes, 0, body, 0, headBytes.Length);
            Buffer.BlockCopy(tombstoneBytes, 0, body, headBytes.Length, tombstoneBytes.Length);
            Buffer.BlockCopy(tailBytes, 0, body, headBytes.Length + tombstoneBytes.Length, tailBytes.Length);
            return body;
        }
    }
}
