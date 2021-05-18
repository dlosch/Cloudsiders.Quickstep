using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Cloudsiders.Orleans.SimpleGrains")]
[assembly: InternalsVisibleTo("Cloudsiders.Quickstep.Tests")]

namespace Cloudsiders.Quickstep {
    internal static class FasterLogMessageConstants {
        internal const int MessageHeaderSizeMin = 31;
        internal const int MaxMessageParts = 8;
    }
}