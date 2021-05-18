using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Cloudsiders.Quickstep {
    [Serializable]
    public class LogMessage : IBatchContainer {
        public LogMessage() { }

        public LogMessage(StreamSequenceToken sequenceToken) => _sequenceToken = sequenceToken;

        public int Id { get; set; }

        internal IEnumerable<object> Events { get; set; }

        public Dictionary<string, object> RequestContext { get; set; }

        internal uint Sequence => SequenceToken != null ? unchecked((uint)SequenceToken.SequenceNumber) : 0u;

        [field: NonSerialized]
        public string StreamNamespace { get; set; }

        [field: NonSerialized]
        public Guid StreamGuid { get; set; }

        [Obsolete]
        // todo 
        public uint StreamId { get; set; }

        [field: NonSerialized]
        internal long FasterLogAddress { get; set; }

        [field: NonSerialized]
        internal int FasterLogLength { get; set; }

        [NonSerialized]
        private StreamSequenceToken _sequenceToken;

        public StreamSequenceToken SequenceToken {
            get {
                if (_sequenceToken == null) {
                    // if more than one event in batch, need to index each event with a dedicated StreamSequenceToken (sequence + idx of event)
                    var evtToken = new EventSequenceTokenV2(0u);
                    _sequenceToken = evtToken;
                }

                return _sequenceToken;
            }
            // init
            internal set => _sequenceToken = value;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() {
            // todo high what about them SequenceToken here? Semantics?
            //return Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, EvtSequenceTokenV2.CreateSequenceTokenForEvent(i)));

            if (Events?.Count() == 1) Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, SequenceToken));

            var baseToken = _sequenceToken as EventSequenceTokenV2 ?? new EventSequenceTokenV2(Sequence);
            return Events?.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, baseToken.CreateSequenceTokenForEvent(i)));
            // todo 20200217 implementierung fehlt
            throw new NotImplementedException();
        }

        public bool ImportRequestContext() {
            if (null != RequestContext) {
                // todo 20210413 implementierung fehlt
                RequestContextExtensions.Import(RequestContext);
            }

            return true;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc) {
            if (null == Events) return false;
            foreach (var item in Events) {
                if (shouldReceiveFunc(stream, filterData, item)) {
                    return true; // There is something in this batch that the consumer is intereted in, so we should send it.
                }
            }

            return false; // Consumer is not interested in any of these events, so don't send.
        }
    }
}