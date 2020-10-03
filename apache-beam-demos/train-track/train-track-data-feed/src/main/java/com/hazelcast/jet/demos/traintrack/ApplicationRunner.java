package com.hazelcast.jet.demos.traintrack;

import java.io.File;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Write the GPS data to a file named "{@code beam-input}",
 * as if real-time.
 * </p>
 * <p>The input points have an offset "{@code 50}", "{@code 60}"
 * which is actually the number of seconds since the start. We
 * delay by a proportionate amount to make the input arrive at
 * a more interesting rate. Being four times. So the delay
 * between consecutive points with offset "{@code 50}" and "{@code 60}"
 * is 2.5 seconds.
 * </p>
 */
@Configuration
@Slf4j
public class ApplicationRunner implements CommandLineRunner {

    private static final String FILE_NAME = "beam-input";
    private static final long QUARTER_OF_A_SECOND_MS = 250L;
    private long previousOffset = -1;

    @Override
    public void run(String... args) throws Exception {

    	File file = new File(FILE_NAME);
        try (PrintWriter printWriter = new PrintWriter(file);) {
        	
            Stream.of(FrecciaRossa.POINTS)
            .forEach(trio -> {
            	
            	// Deduce time difference from previous point
            	long currentOffset = Long.valueOf(trio[2]);
            	long offsetDiff = currentOffset - this.previousOffset;
            	if (this.previousOffset < 0 || offsetDiff < 0) {
            		offsetDiff = 0;
            	}
        		this.previousOffset = currentOffset;

        		// Simulate delay based on input
                try {
                	if (offsetDiff > 10) {
                		log.info("... (Gap: {}) ...", offsetDiff);
                	}
					TimeUnit.MILLISECONDS.sleep(offsetDiff * QUARTER_OF_A_SECOND_MS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
                
                String csv = trio[0] + "," + trio[1] + "," + trio[2]; 
                log.info(csv);
                printWriter.println(csv);
                printWriter.flush();
            });
        }
 	   
    }

}
