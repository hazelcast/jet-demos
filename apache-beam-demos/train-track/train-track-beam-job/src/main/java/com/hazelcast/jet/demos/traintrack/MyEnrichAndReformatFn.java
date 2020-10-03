package com.hazelcast.jet.demos.traintrack;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/**
 * <p>The input element of the process context is a CSV string.
 * Convert this into JSON.
 * </p>
 * <p>The third part of the CSV is a time offset, in milliseconds.
 * Enrich this with the current time to make the timestamps in the
 * output JSON appear to be happening right now.
 * </p>
 */
@SuppressWarnings("serial")
public class MyEnrichAndReformatFn extends DoFn<String, String> {

	private static final SimpleDateFormat simpleDateFormat 
		= new SimpleDateFormat("HH:mm:ss");
	
	private long count;
	private long startTimestamp;
	
	public MyEnrichAndReformatFn() {
		this(0);
	}
	public MyEnrichAndReformatFn(long arg0) {
		this.startTimestamp = arg0;
	}

	/**
	 * <p>Use helper functions to take the input from the
	 * execution context to make the output.
	 * </p>
	 *
	 * @param element String from the ProcessContext
	 * @param receiver Where to send the processed input
	 */
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) { 
    	if (this.startTimestamp==0) {
    		this.startTimestamp = System.currentTimeMillis();
    	}
    	this.count++;
    	
    	long offset = _getOffset(element) * MyBeamJob.QUARTER_OF_A_SECOND_MS;
    	
    	long timestamp = _calculateTimestamp(offset);

    	String result = _makeJson(element, timestamp);

        receiver.outputWithTimestamp(result, new Instant(timestamp));
	}

    
    /**
     * <p>Find field 3 in CSV. Very slim need for this to be a
     * separate method.
     * </p>
     * 
     * @param input Something comma something comma offset
     * @return The offset
     */
    public long _getOffset(String input) {
    	String[] trio = input.split(",");

    	return Long.parseLong(trio[2]);
    }
    
    /**
     * <p>Add the offset of the time in the original input data to
     * the start time, to make the effective time.
     * </p>
     * 
     * @param offset From GPS
     * @return
     */
    public long _calculateTimestamp(long offset) {
    	return this.startTimestamp + offset;
    }
    
    /**
     * <p>Reformat the input into Json.
     * </p>
     *
     * @param input String in CSV form
     * @param timestamp Timestamp to use to appear it is happening now
     * @return String in JSON form
     */
    public String _makeJson(String input, long timestamp) {
    	String[] trio = input.split(",");
    	
        StringBuilder sb = new StringBuilder("{");
        
        sb.append(" \"input-point\": \"" + this.count + "\",");
        
        sb.append(" \"latitude\": \"" + trio[0] + "\",");
        sb.append(" \"longitude\": \"" + trio[1] + "\",");
        sb.append(" \"timestamp\": \"" + timestamp + "\",");
        sb.append(" \"timeprint\": \"" + simpleDateFormat.format(new Date(timestamp)) + "\"");

        sb.append(" }");
        
        return sb.toString();
    }
}
