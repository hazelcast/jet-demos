package com.hazelcast.jet.demos.traintrack;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;

/**
 * <p>Ensure one copy of the file watcher job is running.
 * This looks for output from the beam job and uploads it
 * from a file publishes to a Hazelcast topic.
 * </p>
 */
@Configuration
public class ApplicationRunner implements CommandLineRunner {

	@Autowired
	private JetInstance jetInstance;

    @Override
    public void run(String... args) throws Exception {
    	Pipeline pipeline = FileWatcher.build();
    	
    	JobConfig jobConfig = new JobConfig();
    	jobConfig.setName(FileWatcher.class.getSimpleName());

    	this.jetInstance.newJobIfAbsent(pipeline, jobConfig);
    }

}
