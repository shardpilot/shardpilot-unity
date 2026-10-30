using System;
using System.IO;
using System.IO.Compression;

namespace ShardPilot
{
    /// <summary>
    /// Request-body gzip for the analytics batch route.
    ///
    /// Scoped to the batch publish ON PURPOSE. That is where the bytes are — a
    /// batch body is the same envelope keys repeated per event, close to the
    /// best case deflate has — while the other routes this SDK uses are a
    /// remote-config GET (no body), a single consent decision (a few hundred
    /// bytes, under the threshold anyway) and the crash upload (multipart
    /// binary to a different service, already compressed). Compressing those
    /// would add risk for no measurable bytes.
    /// </summary>
    internal static class ShardPilotRequestCompression
    {
        /// <summary>
        /// Body size below which a publish is sent uncompressed.
        ///
        /// gzip costs 18 bytes of framing (10-byte header, 8-byte trailer)
        /// before deflate's own block overhead, so a small body can come out
        /// LARGER compressed — and the CPU is spent either way. A single-event
        /// batch is a few hundred bytes; a full batch is tens of kilobytes.
        /// The threshold sits between them.
        /// </summary>
        internal const int MinimumBytes = 1024;

        /// <summary>The coding this SDK sends, and the only one ingest accepts.</summary>
        internal const string GzipCoding = "gzip";

        internal const string ContentEncodingHeaderName = "Content-Encoding";

        /// <summary>
        /// Ingest detail codes that mean "this server cannot read the coding
        /// you sent". Matched instead of the bare 400: a status-only match
        /// would let an ordinary validation failure — a bad event name, a
        /// scope mismatch — silently switch compression off for the rest of
        /// the session, a transport change nobody asked for made in response
        /// to an unrelated defect.
        /// </summary>
        internal const string UnsupportedCodingDetail = "unsupported_content_encoding";

        internal const string InvalidCodingDetail = "invalid_content_encoding";

        /// <summary>
        /// Compresses <paramref name="payload"/> and reports whether the
        /// result is worth sending. Returns false — leaving the caller on the
        /// uncompressed path — when the body is under the threshold, when
        /// compression fails, or when it did not actually shrink the body. The
        /// last case is not theoretical: a high-entropy body comes back
        /// larger, and sending it would spend CPU to make the request worse.
        /// </summary>
        internal static bool TryCompress(byte[] payload, out byte[] compressed)
        {
            compressed = null;
            if (payload == null || payload.Length < MinimumBytes)
            {
                return false;
            }

            try
            {
                using (var buffer = new MemoryStream(payload.Length / 2))
                {
                    // CompressionLevel.Fastest, not Optimal. On batch JSON —
                    // dominated by repeated envelope keys, which deflate
                    // catches at any level — the two land within a couple of
                    // percent of each other, and an SDK inside a game's frame
                    // budget takes the cheaper one.
                    using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, true))
                    {
                        gzip.Write(payload, 0, payload.Length);
                    }

                    // The GZipStream is disposed BEFORE the buffer is read:
                    // disposal is what writes the CRC and length trailer, and
                    // a body missing it is refused by the server as an
                    // unreadable gzip stream. Reading inside the using block
                    // is the single easiest way to get this wrong.
                    var encoded = buffer.ToArray();
                    if (encoded.Length >= payload.Length)
                    {
                        return false;
                    }

                    compressed = encoded;
                    return true;
                }
            }
            catch (Exception)
            {
                // Compression is an optimization. Anything that goes wrong
                // here falls back to the uncompressed body rather than
                // failing the publish.
                return false;
            }
        }

        /// <summary>
        /// Reports whether a failed publish carried the server saying it could
        /// not read the request's content coding.
        /// </summary>
        internal static bool IsEncodingRefusal(Exception error)
        {
            var status = error as ShardPilotHttpStatusException;
            if (status == null || string.IsNullOrEmpty(status.DetailCodes))
            {
                return false;
            }

            return status.DetailCodes.IndexOf(UnsupportedCodingDetail, StringComparison.Ordinal) >= 0 ||
                   status.DetailCodes.IndexOf(InvalidCodingDetail, StringComparison.Ordinal) >= 0;
        }
    }

    /// <summary>
    /// Per-client state deciding whether the next batch publish is compressed.
    ///
    /// Two failure shapes have to be survivable, because this SDK ships to
    /// customers who point it at their own ingest deployment and their own
    /// edge:
    ///
    /// 1. The server refuses the coding with a detail code. Unambiguous:
    ///    compression is latched off for the session and the batch is re-sent
    ///    uncompressed.
    ///
    /// 2. On WebGL the request never reaches the server at all. A browser
    ///    preflights Content-Encoding — it is not CORS-safelisted — and an
    ///    edge whose Access-Control-Allow-Headers omits it fails the request
    ///    as a NETWORK error, indistinguishable from an ordinary blip. There
    ///    is no status, no detail code, nothing to match on. Left alone, a
    ///    WebGL build behind such an edge would retry compressed forever and
    ///    deliver nothing.
    ///
    /// So the first compressed publish is PROBATIONARY: until one has
    /// succeeded, a transport error also triggers one uncompressed retry, and
    /// compression latches off only if that retry succeeds — which is the
    /// evidence that the coding, not the network, was the problem. Once a
    /// compressed publish has succeeded the coding is proven for this endpoint
    /// and transport errors are treated as what they usually are.
    /// </summary>
    internal sealed class ShardPilotCompressionPolicy
    {
        private readonly object _gate = new object();
        private readonly bool _optOut;
        private bool _refused;
        private bool _proven;

        internal ShardPilotCompressionPolicy(bool enabled)
        {
            _optOut = !enabled;
        }

        /// <summary>Whether the next publish may compress its body.</summary>
        internal bool ShouldCompress
        {
            get
            {
                lock (_gate)
                {
                    return !_optOut && !_refused;
                }
            }
        }

        /// <summary>Whether a compressed publish has ever succeeded.</summary>
        internal bool IsProven
        {
            get
            {
                lock (_gate)
                {
                    return _proven;
                }
            }
        }

        internal void RecordCompressedSuccess()
        {
            lock (_gate)
            {
                _proven = true;
            }
        }

        internal void Disable()
        {
            lock (_gate)
            {
                _refused = true;
            }
        }

        /// <summary>
        /// Decides what to do with a failed COMPRESSED publish.
        /// Returns true when the caller should retry the same body
        /// uncompressed.
        /// </summary>
        internal bool ShouldRetryUncompressed(Exception error)
        {
            if (ShardPilotRequestCompression.IsEncodingRefusal(error))
            {
                Disable();
                return true;
            }

            // Probation: an unproven coding that failed at the transport is
            // the WebGL preflight shape. Retry once uncompressed; the caller
            // reports back whether that retry worked.
            var transportError = error is ShardPilotTransportException;
            return transportError && !IsProven;
        }
    }
}
