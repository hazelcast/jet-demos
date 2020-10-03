package com.hazelcast.jet.demos.traintrack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.joda.time.Instant;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Create an infinite stream of lines from our input file.
 * </p>
 * <p>
 * See <a href=
 * "https://beam.apache.org/releases/javadoc/2.5.0/org/apache/beam/sdk/io/UnboundedSource.UnboundedReader.html">
 * here</a><./p>
 * <p><a href="https://beam.apache.org/releases/javadoc/2.2.0/org/apache/beam/sdk/io/TextIO.html">
 * TextIO</a> has a filewatcher but only for the appearance of new files, not new lines in
 * existing files.
 * </p>
 */
@Slf4j
@SuppressWarnings("serial")
public class MyUnboundedReader extends UnboundedReader<String> implements Serializable {

	private static final String BASE_DIR = ".";

	private final UnboundedSource<String, UnboundedSource.CheckpointMark> unboundedSource;

	private BufferedReader bufferedReader;
	private int count;
	private final String fileName;
	private String lineRead;
	private final Queue<String> queue = new LinkedBlockingQueue<>(10000);
	private WatchService watchService;
	private Instant watermark;

	public MyUnboundedReader(UnboundedSource<String, UnboundedSource.CheckpointMark> arg0, String arg1)
		throws IOException {
		this.unboundedSource = arg0;
		this.fileName = arg1;
		this.watchService = FileSystems.getDefault().newWatchService();
	}

	/**
	 * <p>The "{@code advance()}" method is a true/false for whether data exists.
	 * </p>
	 * <p>We are watching for changes to the given input file ("{@code beam-input}").
	 * However, mostly the {@link java.nio.file.WatchService} will not spot each line
	 * added and instead give us a block. We wish the lines one at a time. So what
	 * the filewatcher actually finds we add to a queue and only release the head
	 * item.
	 * </p>
	 * 
	 * @return True if something read by filewatcher
	 */
	@Override
	public boolean advance() throws IOException {

		/* Part 1. Filewatch for lines being added to the requested
		 * file. Filewatch won't necessarily spot each line, so add
		 * all lines noticed to a buffer.
		 */
		WatchKey watchKey = this.watchService.poll();
		if (watchKey!=null) {
			for (WatchEvent<?> watchEvent : watchKey.pollEvents()) {
				if (watchEvent.context() instanceof Path) {
					Path path = (Path) watchEvent.context();
					if (path.getFileName().toString().equals(this.fileName)) {
						// Only open the file once
						if (bufferedReader==null) {
							bufferedReader = Files.newBufferedReader(path, Charset.forName(
									StandardCharsets.UTF_8.toString()));
						}

						String line = null;
						int i = 0;
						try {
							while((line = bufferedReader.readLine()) != null){
								this.queue.add(line);
								i++;
							}
							log.info("Read {} lines from '{}'", i, path);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
			watchKey.reset();
		}

		/* Part 2. Filewatch may add a block of lines to the queue,
		 * release only one.
		 */
		this.lineRead = this.queue.poll();
		if (this.lineRead != null) {
			try {
				Thread.sleep(MyBeamJob.QUARTER_OF_A_SECOND_MS);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			count++;
			this.watermark = new Instant(count * MyBeamJob.QUARTER_OF_A_SECOND_MS);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * <p>No checkpointing.</p>
	 */
	@Override
	public CheckpointMark getCheckpointMark() {
		return null;
	}

	/**
	 * <p>Link back to parent.</p>
	 */
	@Override
	public UnboundedSource<String, ?> getCurrentSource() {
		return this.unboundedSource;
	}

	/**
	 * <p>Last read point.</p>
	 */
	@Override
	public Instant getWatermark() {
		return this.watermark;
	}

	/**
	 * <p>Start watching for file changes.</p>
	 *
	 * @return False, starting doesn't do any polling so nothing read
	 */
	@Override
	public boolean start() throws IOException {
		log.info("Started to watch directory: '{}'", BASE_DIR);
		Paths.get(BASE_DIR).register(this.watchService, StandardWatchEventKinds.ENTRY_MODIFY);

		this.watermark=new Instant(0);
		return false;
	}

	/**
	 * <p>Can't close an infinite reader.</p>
	 */
	@Override
	public void close() throws IOException {
	}

	/**
	 * <p>Set by {@link #advance} if returns "{@code true}".</p>
	 */
	@Override
	public String getCurrent() throws NoSuchElementException {
		return this.lineRead;
	}

	/**
	 * <p>Set by {@link #advance} if returns "{@code true}".</p>
	 */
	@Override
	public Instant getCurrentTimestamp() throws NoSuchElementException {
		return this.watermark;
	}

}
