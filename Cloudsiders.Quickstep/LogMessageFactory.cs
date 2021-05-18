using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    internal static class LogMessageFactory {
        internal static LogMessage ToMessage<T>(FasterLogSerializationManager serializationManager, Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext, StreamSequenceToken sequenceToken)
            => new LogMessage {
                Events = events?.Cast<object>(),
                // todo do I need to clone here?
                RequestContext = requestContext,

                SequenceToken = sequenceToken,
                StreamGuid = streamGuid,
                StreamNamespace = streamNamespace,
            };
    }
}