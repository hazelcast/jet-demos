package com.hazelcast.jet.demos.traintrack;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * <p>Create an unbounded (ie. infinite) source wrapping an
 * unbounded reader.
 * </p>
 * <p>See <a href="https://beam.apache.org/releases/javadoc/2.5.0/org/apache/beam/sdk/io/UnboundedSource.html">
 * here</a><./p>	
 */
@SuppressWarnings("serial")
public class MyUnboundedSource extends UnboundedSource<String, UnboundedSource.CheckpointMark>
	implements Serializable {
	
	private final String fileName;
	
	public MyUnboundedSource(String arg0) {
		this.fileName = arg0;
	}
	
	/**
	 * <p>Create a reader that reads indefinitely.</p>
	 *
	 * @param arg0 Pipeline options, ignored
	 * @param arg1 Starting checkpoint, ignored
	 */
	@Override
	public UnboundedReader<String> createReader(PipelineOptions arg0, CheckpointMark arg1) throws IOException {
	    return new MyUnboundedReader(this, this.fileName);
	}

	/**
	 * <p>Checkpoints are not used.</p>
	 */
	@Override
	public Coder<CheckpointMark> getCheckpointMarkCoder() {
		return null;
	}

	/**
	 * <p>Returns strings.</p>
	 */
	@Override
	public Coder<String> getOutputCoder() {
		return StringUtf8Coder.of();
	}

	/**
	 * <p>Source list, only one as ignoring splits.</p>
	 *
	 * @param arg0 Number of splits, ignored
	 * @param arg1 Pipeline options, ignored2
	 */
	@Override
	public List<? extends UnboundedSource<String, CheckpointMark>> split(int arg0, PipelineOptions arg1)
			throws Exception {
		List<UnboundedSource<String, CheckpointMark>> result = new ArrayList<>(Arrays.asList(this));
		return Collections.unmodifiableList(result);
	}

}
