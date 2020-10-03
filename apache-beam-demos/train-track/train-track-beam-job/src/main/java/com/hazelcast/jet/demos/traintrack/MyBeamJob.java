package com.hazelcast.jet.demos.traintrack;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

/**
 * <p>Define a Beam pipeline that reads a CSV file, reformats it
 * to JSON, and writes it to another file. Reformatting uses the
 * start time of the job adjust the timestamp on the input data
 * so that it looks like it is happening real-time.
 * </p>
 * <p>This is not a particularly sophisticated task, but this
 * example isn't about the business logic.
 * </p>
 */
public class MyBeamJob {

	static final long QUARTER_OF_A_SECOND_MS = 250L;
	static final Duration ONE_SECOND = Duration.standardSeconds(1);
	
	public static Pipeline build(PipelineOptions pipelineOptions) {
		
	    Pipeline pipeline = Pipeline.create(pipelineOptions);

		pipeline
		.apply("unbounded-source", 
				Read.from(new MyUnboundedSource("beam-input")))
	    .apply("reformat-and-timestamp", 
	    		ParDo.of(new MyEnrichAndReformatFn()))
		.apply("window",
				 Window.<String>into(FixedWindows.of(ONE_SECOND))
				 .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
				 .discardingFiredPanes()
				 .withAllowedLateness(ONE_SECOND)
				)
	    .apply("sink",
	    		FileIO.<String>write()
	    		.via(TextIO.sink())
	            .to(".")
	            .withPrefix("beam-output")
	            .withNumShards(1)
	    		)
		;

	    return pipeline;
	}

}
