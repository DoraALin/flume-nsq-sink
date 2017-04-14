# flume-nsq-sink

[![GitHub release](https://img.shields.io/github/release/doraalin/flume-nsq-sink.svg)](https://github.com/doraalin/flume-nsq-sink/releases/latest)

**flume-nsq-sink** is apache flume sink for port flume events to NSQ, with a local backup file tool to backup event body which failed in publish.
 Install as plugin and follow usecase to configure sink in conf file.
  
## Installation and Usecase
1. Run '_mvn package'_ to package plugin in path _./nsq-sink_. place folder in your ${FLUME_HOME}/plugin.d/
2. Configure your agent conf file with nsq-sink, you can get started with following example, which has a sequence source:
   <pre>
   # example.conf: A single-node Flume configuration
  
   # Name the components on this agent
   a1.sources = sequence
   a1.sinks = nsq
   a1.channels = c1
  
   # Describe/configure the source
   a1.sources.sequence.type = seq
  
   # Describe the sink, type, topic, lookupAddresses are mandatory
   a1.sinks.nsq.type = com.youzan.flume.sink.nsq.NSQSink
   a1.sinks.nsq.topic = flume_sink
   a1.sinks.nsq.lookupdAddresses = 127.0.0.1:4161
  
   # Use a channel which buffers events in memory
   a1.channels.c1.type = memory
   a1.channels.c1.capacity = 1000
   a1.channels.c1.transactionCapacity = 100
  
   # Bind the source and sink to the channel
   a1.sources.sequence.channels = c1
   a1.sinks.nsq.channel = c1
   </pre>
3. Start your agent, nsq sink carries number sequence to NSQ.