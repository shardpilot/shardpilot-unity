using UnityEngine.Networking;

namespace ShardPilot
{
    internal static class ShardPilotSdkIdentity
    {
        internal const string Version = "0.14.0-alpha";
        internal const string HeaderName = "X-ShardPilot-SDK";
        internal const string HeaderValue = "unity/" + Version;

        internal static void Apply(UnityWebRequest request)
        {
            request.SetRequestHeader(HeaderName, HeaderValue);
        }
    }
}
