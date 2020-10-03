package com.hazelcast.jet.demos.traintrack;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyEnrichAndReformatFnTest {
	
	/* Beam 2.14.0 is first version for Jet BeamRunner
	 * on http://repo1.maven.org/maven2/org/apache/beam/beam-runners-core-java/
	 */
	static long BEAM_2_14_RELEASE;

	static MyEnrichAndReformatFn myEnrichAndReformatFn;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Date beamReleaseDate
			= new SimpleDateFormat("yyyy-MM-dd HH:mm").parse("2019-07-24 22:06");

		BEAM_2_14_RELEASE = beamReleaseDate.getTime();

		myEnrichAndReformatFn = new MyEnrichAndReformatFn(BEAM_2_14_RELEASE);
	}

	@Test
	public void json_parse() throws JSONException {
		long offset = 789;
		long timestampToUse = 987;
		String input = "123,456," + offset;
		
		String output = myEnrichAndReformatFn._makeJson(input, timestampToUse);
		
		log.info("{} -> {}", input, output);
		
		assertThat("output", output, not(nullValue()));
		
		JSONObject jsonObject = new JSONObject(output);
		
		String latitudeExtracted = jsonObject.getString("latitude");
		assertThat("latitude", latitudeExtracted, not(nullValue()));
		assertThat("latitude", latitudeExtracted, is(equalTo("123")));

		String longitudeExtracted = jsonObject.getString("longitude");
		assertThat("longitude", longitudeExtracted, not(nullValue()));
		assertThat("longitude", longitudeExtracted, is(equalTo("456")));
		
		String timestampExtracted = jsonObject.getString("timestamp");
		assertThat("timestamp", timestampExtracted, not(nullValue()));
		assertThat("timestamp", timestampExtracted, is(equalTo(""+timestampToUse)));
		
		assertThat("timestampToUse", timestampToUse, not(equalTo(offset)));
	}

	@Test
	public void calculate_timestamp() {
		long offset = 789;

		long timestampExpected = BEAM_2_14_RELEASE + offset;

		long timestampCalculated = myEnrichAndReformatFn._calculateTimestamp(offset);

		assertThat("timestampCalculated", timestampCalculated, is(equalTo(timestampExpected)));
	}
}
