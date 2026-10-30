using UnityEditor;

namespace ShardPilot.Editor
{
    [InitializeOnLoad]
    internal static class ShardPilotEditorLifecycle
    {
        static ShardPilotEditorLifecycle() { Install(); }
        internal static void Install()
        {
            EditorApplication.playModeStateChanged -= OnPlayModeChanged;
            EditorApplication.playModeStateChanged += OnPlayModeChanged;
            AssemblyReloadEvents.beforeAssemblyReload -= ShardPilotHostLifecycle.Exit;
            AssemblyReloadEvents.beforeAssemblyReload += ShardPilotHostLifecycle.Exit;
        }
        private static void OnPlayModeChanged(PlayModeStateChange state)
        {
            if (state == PlayModeStateChange.ExitingPlayMode) ShardPilotHostLifecycle.Exit();
            if (state == PlayModeStateChange.ExitingEditMode) ShardPilotHostLifecycle.Enter();
        }
    }
}
