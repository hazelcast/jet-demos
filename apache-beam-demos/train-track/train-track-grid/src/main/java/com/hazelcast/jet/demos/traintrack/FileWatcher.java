package com.hazelcast.jet.demos.traintrack;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

/**
 * <p>The Beam job writes to the filesystem rather than Jet, as the Beam job
 * runs in Jet but does not know about Jet. It could run in another Beam runner.
 * </p>
 * <p>So, we define here another job that reads the output file of the Beam
 * job, and publishes the lines into a Hazelcast topic. Any subscriber to the
 * topic gets the current location sent to it.
 * </p>
 */
public class FileWatcher {

	static Pipeline build() {
		Pipeline pipeline = Pipeline.create();
		
		StreamStage<String> source = pipeline
				.readFrom(FileWatcher.buildFileWatcherSource()).withoutTimestamps().setName("fileSource");

		source.writeTo(FileWatcher.buildTopicSink());
		source.writeTo(Sinks.logger()).setName("loggerSink");

		return pipeline;
	}

	
	/**
	 * <p>A custom source, a filewatcher that produces a continuous stream
	 * of lines in files in the "{@code beam-output}" directory.
	 * </p>
	 * <p>As the Beam job writes the lines, this job reads the lines.
	 * </p>
	 *
	 * @return
	 */
	protected static StreamSource<String> buildFileWatcherSource() {
		return Sources.filesBuilder(".").glob("beam-output-*").buildWatcher();
	}
	
	/**
	 * <p>A custom sink, this one takes a stream of strings as input (which happen to be JSON)
	 * and publishes them to a Hazelcast topic as {@link HazelcastJsonValue} type.
	 * </p>
	 * <p>The topic name is "{@code treno}". This should match the topic that the
	 * Web UI is looking for.
	 * </p>
	 *
	 * @return
	 */
    protected static Sink<? super String> buildTopicSink() {
        return SinkBuilder.sinkBuilder(
                        "topicSink", 
                        context -> context.jetInstance().getHazelcastInstance().getTopic("treno")
                        )
                        .receiveFn((iTopic, item) -> iTopic.publish(new HazelcastJsonValue(item.toString())))
                        .build();
    }
	
}
