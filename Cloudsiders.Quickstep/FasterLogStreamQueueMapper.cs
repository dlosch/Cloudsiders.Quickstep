using Orleans.Configuration;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    internal sealed class FasterLogStreamQueueMapper : HashRingBasedStreamQueueMapper {
        public FasterLogStreamQueueMapper(HashRingStreamQueueMapperOptions options, string queueNamePrefix) : base(options, queueNamePrefix) { }
    }
}