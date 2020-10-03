package com.hazelcast.jet.demos.traintrack;

import org.apache.beam.runners.jet.JetPipelineOptions;
import org.apache.beam.runners.jet.JetRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * <p>Take a standard Beam pipeline, and submit it to Hazelcast
 * Jet for execution.
 * </p>
 * <p>This mainly involves setting up six parameters, hardcoded
 * here for simplicity but could be better externalised. These
 * are:
 * </p>
 * <ul>
 * <li><b>CodeJarPathName</b>
 * <p>The path to the Jar file that contains the pipeline code,
 * so we can stream this Jar file to Jet to run.</p>
 * <p>Note this is a "shaded" jar, including the pipeline and the
 * Beam Runner code.
 * </p>
 * </li>
 * <li><b>JetDefaultParallelism</b>
 * <p>Jet is usually highly parallel, which in this case would normally
 * result in the input file being processed in parallel and producing
 * multiple output files with some of the processed content in each.
 * Ordinarily that would be useful, but here to keep it simple we
 * drop to single execution so only one output file is produced.</p>
 * </li>
 * <li><b>JetGroupName</b>
 * <p>The Jet cluster name, "{@code dev}", "{@code test}", "{@code prod}", or
 * in this case "{@code frecciarossa}"</p>
 * </li>
 * <li><b>JetServers</b>
 * <p>Where to find the Jet cluster processes. You don't have to list them
 * all but more than one entry point would be a good idea to specify.</p>
 * </li>
 * <li><b>JobName</b>
 * <p>Obvious, hopefully</p>
 * </li>
 * <li><b>Runner</b>
 * <p>The implementation class that runs the job, Jet of course</p>
 * </li>
 * </ul>
 */
public class Application {

	public static void main(String[] args) {
		
		JetPipelineOptions jetPipelineOptions
			= PipelineOptionsFactory.create().as(JetPipelineOptions.class);
		
	    jetPipelineOptions.setCodeJarPathname("train-track-beam-runner/target/train-track-beam-runner-shaded.jar");
	    jetPipelineOptions.setJetDefaultParallelism(1);
	    jetPipelineOptions.setClusterName("frecciarossa");
		jetPipelineOptions.setJetServers("127.0.0.1:8701");
	    jetPipelineOptions.setJobName(MyBeamJob.class.getSimpleName()); // Needs Beam 2.15
	    jetPipelineOptions.setRunner(JetRunner.class);

	    Pipeline pipeline = MyBeamJob.build(jetPipelineOptions);

	    try {
		    pipeline.run();
	    } catch (Exception e) {
		// If submit twice, can get JobAlreadyExistsException if first still running
		e.printStackTrace();
	    } finally {
		    // Don't wait for Pipeline, unbounded source!
		    System.exit(0);
	    }

	}
}
