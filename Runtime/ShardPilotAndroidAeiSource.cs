#if UNITY_ANDROID
using System;
using System.Collections.Generic;
using UnityEngine;

namespace ShardPilot
{
    /// <summary>
    /// The production <see cref="IShardPilotAeiSource"/>: reads
    /// <c>ActivityManager.getHistoricalProcessExitReasons</c> for the OWN
    /// package over Unity's <c>AndroidJavaObject</c> JNI bridge — 100%
    /// managed C#, no native plugin. Compiled only into
    /// Android builds; the headless harness tests the lane through the
    /// source seam with fakes, and this thin JNI projection is exercised on
    /// devices via the smoke runbook.
    ///
    /// Defensive throughout: any JNI failure yields an EMPTY read — the
    /// ANR lane must never cost SDK initialization anything on a broken or
    /// pre-API-30 device.
    /// </summary>
    internal sealed class ShardPilotAndroidAeiSource : IShardPilotAeiSource
    {
        private const int MinSupportedSdkInt = 30;

        public List<ShardPilotAeiExitRecord> ReadExitRecords()
        {
            var result = new List<ShardPilotAeiExitRecord>();
            try
            {
                using (var version = new AndroidJavaClass("android.os.Build$VERSION"))
                {
                    if (version.GetStatic<int>("SDK_INT") < MinSupportedSdkInt)
                    {
                        return result;
                    }
                }

                using (var unityPlayer = new AndroidJavaClass("com.unity3d.player.UnityPlayer"))
                using (var activity = unityPlayer.GetStatic<AndroidJavaObject>("currentActivity"))
                {
                    if (activity == null)
                    {
                        return result;
                    }

                    var packageName = activity.Call<string>("getPackageName");
                    using (var activityManager = activity.Call<AndroidJavaObject>("getSystemService", "activity"))
                    {
                        if (activityManager == null)
                        {
                            return result;
                        }

                        // maxNum 0 = every record the platform still holds
                        // for this package; the OS's own small AEI ring
                        // buffer is the bound. The reporter needs the FULL
                        // enumeration for its no-skip cursor rule — its
                        // per-pass processing cap applies AFTER enumeration
                        // (a capped read would return only the newest
                        // records and settling over them would strand the
                        // older ones below the cursor forever). Each record
                        // is identity fields + two lazy providers, so the
                        // full enumeration stays cheap.
                        using (var list = activityManager.Call<AndroidJavaObject>(
                                   "getHistoricalProcessExitReasons", packageName, 0, 0))
                        {
                            if (list == null)
                            {
                                return result;
                            }

                            var size = list.Call<int>("size");
                            for (var i = 0; i < size; i++)
                            {
                                var record = ReadRecord(list, i);
                                if (record != null)
                                {
                                    result.Add(record);
                                }
                            }
                        }
                    }
                }
            }
            catch
            {
                // A failed batch read is an empty read.
            }

            return result;
        }

        private static ShardPilotAeiExitRecord ReadRecord(AndroidJavaObject list, int index)
        {
            try
            {
                // NOT disposed here: the ApplicationExitInfo object is
                // captured by the record's lazy DESCRIPTION and TRACE
                // readers, which the reporter invokes only for records it
                // actually reports — enumeration itself materializes the
                // identity tuple ONLY, so a suppressed (opted-out/denied)
                // launch never pulls getDescription() free text or a trace
                // stream into managed memory. The platform's own AEI ring
                // bounds how many of these small JNI references exist per
                // launch; they are released when the records go out of
                // scope after the pass.
                var info = list.Call<AndroidJavaObject>("get", index);
                if (info == null)
                {
                    return null;
                }

                var record = new ShardPilotAeiExitRecord
                {
                    TimestampMs = info.Call<long>("getTimestamp"),
                    Reason = info.Call<int>("getReason"),
                    Pid = info.Call<int>("getPid"),
                    DefiningUid = info.Call<int>("getDefiningUid"),
                    DescriptionReader = () => info.Call<string>("getDescription"),
                    TraceTextReader = maxBytes => ReadTraceText(info, maxBytes)
                };

                if (record.Reason == ShardPilotAeiReasons.ReasonCrashNative)
                {
                    // The binary tombstone reader exists ONLY on native-crash
                    // records: an ANR record's
                    // stream is the text thread dump, never read as bytes.
                    record.TraceBytesReader = maxBytes => ReadTraceBytes(info, maxBytes);
                }

                return record;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Bounded BINARY read of a REASON_CRASH_NATIVE record's tombstone
        /// stream (API 31+ — the raw AOSP <c>Tombstone</c> protobuf; the
        /// server parses it). The read is CHUNKED
        /// and CAPPED by <see cref="ShardPilotAnrIds.ReadBoundedTombstoneBytes"/>
        /// — the shared loop the headless suite drives with fakes: every
        /// chunk request is budgeted to the remaining allowance, so AT MOST
        /// maxBytes + 1 bytes are ever pulled from the stream and buffered,
        /// and the FIRST byte past the cap classifies the dump OverCap and
        /// stops — an over-cap tombstone is never drained onto the Java
        /// heap just to be size-checked (a multi-megabyte dump must not OOM
        /// or stall the next-launch pass), never <c>toByteArray()</c>-ed,
        /// and never marshals a byte across the bridge: skipped whole,
        /// never truncated. Each chunk moves BETWEEN two Java objects
        /// (<c>InputStream.read(byte[],int,int)</c> into a reflectively
        /// created Java <c>byte[]</c>, <c>ByteArrayOutputStream.write</c>
        /// out of it) with only the count crossing JNI per iteration — the
        /// same no-chunked-copy-back portability stance that keeps the ANR
        /// reader line-based; an IN-cap stream's bytes marshal to C# ONCE,
        /// as <c>toByteArray()</c>'s return value, exactly as before. Null
        /// on a NULL stream (the API-31+ overwritten-buffer shape; API-30
        /// records never construct this reader) or any failure — the
        /// reporter then emits the metadata-only degraded report.
        /// </summary>
        private static ShardPilotAeiTombstoneRead ReadTraceBytes(AndroidJavaObject info, int maxBytes)
        {
            AndroidJavaObject stream = null;
            try
            {
                stream = info.Call<AndroidJavaObject>("getTraceInputStream");
                if (stream == null)
                {
                    return null;
                }

                using (var buffer = new AndroidJavaObject("java.io.ByteArrayOutputStream"))
                using (var byteClass = new AndroidJavaClass("java.lang.Byte"))
                using (var byteType = byteClass.GetStatic<AndroidJavaObject>("TYPE"))
                using (var arrayClass = new AndroidJavaClass("java.lang.reflect.Array"))
                // A Java-side byte[] transfer buffer (reflectively created —
                // Unity's high-level bridge cannot new a primitive array
                // directly, and Array.newInstance returns Object, which it
                // CAN hold as a reference and pass back as a byte[] arg).
                using (var chunk = arrayClass.CallStatic<AndroidJavaObject>(
                           "newInstance", byteType, ShardPilotAnrIds.TombstoneReadChunkBytes))
                {
                    var boundedStream = stream;
                    var boundedBuffer = buffer;
                    var boundedChunk = chunk;
                    return ShardPilotAnrIds.ReadBoundedTombstoneBytes(
                        request =>
                        {
                            var read = boundedStream.Call<int>("read", boundedChunk, 0, request);
                            if (read > 0)
                            {
                                boundedBuffer.Call("write", boundedChunk, 0, read);
                            }

                            return read;
                        },
                        () => boundedBuffer.Call<byte[]>("toByteArray"),
                        maxBytes);
                }
            }
            catch
            {
                return null;
            }
            finally
            {
                if (stream != null)
                {
                    try
                    {
                        stream.Call("close");
                    }
                    catch
                    {
                        // Best-effort close.
                    }

                    stream.Dispose();
                }
            }
        }

        /// <summary>
        /// Bounded read of the record's ANR trace: for REASON_ANR the
        /// stream is the text thread dump, read line-by-line through
        /// <c>BufferedReader.readLine()</c> — strings marshal cleanly over
        /// the JNI bridge, avoiding byte[] copy-back semantics that vary
        /// across Unity versions. Returns null when the platform returns a
        /// NULL stream (the bounded global buffer was overwritten — the
        /// degraded shape) or on any read failure; the
        /// reporter then emits the metadata-only degraded report. The
        /// tombstone (binary protobuf) read is the SEPARATE
        /// <see cref="ReadTraceBytes"/> above, behind the same seam — never
        /// a change to this one.
        /// </summary>
        private static string ReadTraceText(AndroidJavaObject info, int maxBytes)
        {
            AndroidJavaObject stream = null;
            AndroidJavaObject reader = null;
            try
            {
                stream = info.Call<AndroidJavaObject>("getTraceInputStream");
                if (stream == null)
                {
                    return null;
                }

                using (var streamReader = new AndroidJavaObject("java.io.InputStreamReader", stream, "UTF-8"))
                {
                    reader = new AndroidJavaObject("java.io.BufferedReader", streamReader);
                    var boundedReader = reader;

                    // Each line is capped to the REMAINING budget BEFORE it
                    // is appended (the shared bounded accumulator), so one
                    // pathological multi-megabyte dump line can never be
                    // RETAINED past maxBytes: the accumulator keeps the
                    // in-budget prefix plus the one-char truncation signal
                    // and discards the rest of the stream unread. Honest
                    // bound, stated: the platform's readLine() still
                    // materializes the crossing line TRANSIENTLY before the
                    // trim — bounding that would need char[] chunk reads,
                    // whose JNI copy-back semantics vary across Unity
                    // versions (the same reason this reader is line-based
                    // at all); the retained-memory bound is what protects
                    // the next-launch pass.
                    return ShardPilotAnrIds.ReadBoundedTraceText(
                        () => boundedReader.Call<string>("readLine"), maxBytes);
                }
            }
            catch
            {
                return null;
            }
            finally
            {
                // AndroidJavaObject.Dispose releases the JNI reference but
                // does NOT close the Java stream — close explicitly.
                if (reader != null)
                {
                    try
                    {
                        reader.Call("close");
                    }
                    catch
                    {
                        // Best-effort close.
                    }

                    reader.Dispose();
                }

                if (stream != null)
                {
                    try
                    {
                        stream.Call("close");
                    }
                    catch
                    {
                        // Best-effort close.
                    }

                    stream.Dispose();
                }
            }
        }
    }
}
#endif
