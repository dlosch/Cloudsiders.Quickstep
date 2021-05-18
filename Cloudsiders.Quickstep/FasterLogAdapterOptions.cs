using System;
using System.Text.Json.Serialization;

namespace Cloudsiders.Quickstep {
    public class FasterLogAdapterOptions {
        public string LogFile { get; set; }
        public bool PreallocateLogFile { get; set; }
        public bool UsePersistentLog { get; set; }
        public bool RecoverDevice { get; set; }
        public int LogSegmentSizeBits { get; set; }
        public int LogMemorySizeBits { get; set; }
        public int LogPageSizeBits { get; set; }

        // todo timespan offenbar still not json serializable system.text meh
        [JsonIgnore]
        public TimeSpan CommitPeriod => TimeSpan.FromMilliseconds(CommitPeriodMillis);

        public long CommitPeriodMillis { get; set; }

        public int ChannelBacklogSize { get; set; } = 1024;
        
        public int? QueueCount { get; set; }
        
        public bool UseWatermarkForCommits { get; set; }
    }

    public static class FasterLogAdapterOptionsExtensions {
        public static void Validate(this FasterLogAdapterOptions options) {
            if (null == options) throw new NullReferenceException(nameof(options));
        }
    }
}