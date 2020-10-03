# Train Track

[Screenshot1]: src/site/markdown/images/Screenshot1.png "Image screenshot1.png"
[Screenshot2]: src/site/markdown/images/Screenshot2.png "Image screenshot2.png"

In June 2019, we <a href="https://hazelcast.com/blog/hazelcast-jet-and-apache-beam/">announced</a> the inclusion of Hazelcast Jet as a runner for <a href="https://beam.apache.org/">Apache Beam</a>.

Now it's time for an example showing how it's done. As a bonus, it's not "<em>Word Count."</em>

![Image of points plotted on a map of Milan, Italy][Screenshot1] 

<h2>IoT Data</h2>
The data we will use is a series of 2,000 GPS points and time offsets:
<pre># Latitude, Longitude, Time-Offset
45.417,8.179,1629
45.417,8.178,1630
45.416,8.178,1631
45.416,8.177,1632
45.415,8.176,1633
</pre>
These points are real data. They come from a trip on Italy's famous <a href="https://www.italiarail.com/frecciarossa">Freccia Rossa (Red Arrow)</a> train from Milan to Turin.

Being real data, it gives us some real problems to solve. Since trains move, they are connected wirelessly. Delivering the position over wireless networks may mean some locations arrive twice or out of order. In some cases, such as when a train is in a tunnel, the positions may not arrive at all.
<h2>The Example Application</h2>
This example application mainly does two things.

It uses a Beam pipeline to enrich the stream of GPS points. They are parsed and reformatted from CSV to JSON, and then windowing is used to drop some out of sequence points. This is not the most sophisticated Beam example, but that's not the objective.

These points are plotted dynamically on a map to make things more visual using JavaScript and a WebSocket. This is done as they arrive, stream processing not batch processing.
<h2>Beam and the Jet Runner</h2>
The basic concept of Apache Beam is that the definition of a processing pipeline is independent and agnostic of the execution platform.

We define a Beam pipeline and choose to run it on Hazelcast Jet.

We could choose to run it with something other than Hazelcast Jet, but we won't.

Take care that although Beam is platform-independent, not all platforms implement all features. For more information on feature compatibility, please refer to the <a href="https://beam.apache.org/documentation/runners/capability-matrix/">Apache Beam Capability Matrix</a>.
<h2>Running the Example</h2>
This example consists of four processes running simultaneously. As it uses file watching logic, we also want to run them all from the same directory to keep things simple.

<b>Note:</b> Using the same directory is crucial to making this demo work. Step 4 writes to the directory that steps 2 and 5 are expecting to keep the example simple. You can change this in the code if you want, but if you don't wish to, be sure to run all commands from the same directory.
<h3>1. Build</h3>
Download <a href="https://github.com/hazelcast/hazelcast-jet-demos">hazelcast-jet-demos</a>.

From the top level of the "<code>train-track</code>" folder run this command to build:
<pre>mvn clean install
</pre>
<h3>2. Run The Jet Grid</h3>
Run this command to start one Jet grid process, and leave it running:
<pre>java -jar train-track-grid/target/train-track-grid.jar
</pre>
This will create a Jet grid running on port 8701 on your machine. The grid is named <em>frecciarossa</em>.

If you have access to either or both of <a href="https://hazelcast.org/download/#management-center">Hazelcast IMDG Management Center</a> and <a href="https://jet.hazelcast.org/download/#management-center">Hazelcast Jet Management Center</a>, you have additional ways to monitor the activity.

The Jet grid is where the Beam job will run. Although we won't do so here, the Jet grid could contain several Jet processes, spread across multiple machines, making full use of all available CPUs.
<h3>3. Run The Web UI</h3>
Start the web UI using this command, and leave it running:
<pre>java -jar train-track-web-ui/target/train-track-web-ui.jar
</pre>
Bring up this page in a browser:
<pre>http://localhost:8085/
</pre>
Assuming you have an internet connection, the browser will download a map of Milan to display on the screen.

This process connects to the Hazelcast Jet cluster to find the location of the train. We won't see the position of the train yet as we haven't started the data feed, which should appear after Step 5 runs.
<h3>4. Create The Data Feed</h3>
Next, run this command:
<pre>java -jar train-track-data-feed/target/train-track-data-feed.jar
</pre>
The command will output four GPS points per second to the screen, and into a file named "<code>beam-input</code>" in the current directory.

There are over 2,000 points recorded, so this job will take more than fifteen minutes to complete. The actual train trip from Milan to Turin takes 65 minutes.

Don't wait for it to complete, as soon as output appears, proceed to Step 5.
<h3>5. Run The Beam Job</h3>
Finally, run this command:
<pre>java -jar train-track-beam-runner/target/train-track-beam-runner.jar
</pre>
Be sure to select "<code>train-track-beam-runner/target/train-track-beam-runner.jar</code>" not "<code>train-track-beam-runner/target/train-track-beam-runner-shaded.jar</code>".

This step will submit a Beam job to run in Hazelcast Jet process started in Step 2.

It will read the file "<code>beam-input</code>" being produced by the process started in Step 4.

It will produce files named "<code>beam-output*</code>" with the enriched GPS information. In addition to the latitude and longitude, we should also have the time in a more human readable form.
<h3>6. Success?</h3>
If you look at the "<code>beam-input</code>" file you should see a sequence of lines for the GPS points:
<pre>45.464,8.438,1364
45.464,8.437,1365
</pre>
If you look at the "<code>beam-output*</code>" files you should see the enriched output for these GPS points:
<pre>{ "input-point": "953", "latitude": "45.464", "longitude": "8.438", "timestamp": "1565644594246", "timeprint": "22:16:34" }
{ "input-point": "954", "latitude": "45.464", "longitude": "8.437", "timestamp": "1565644594496", "timeprint": "22:16:34" }
</pre>
Looking at the browser, you should see it updating as the points are plotted on the map. Something like this:

![Image of points plotted on a map west of Milan, Italy][Screenshot2] 

If you don't get these three, something went wrong. It's worth finding out where and why.
<h2>Show Me the Magic</h2>
How does this work?

The cleverness is in the "<code>train-track-beam-runner</code>" module, and specifically this coding:
<pre>public static void main(String[] args) {
		
    JetPipelineOptions jetPipelineOptions
        = PipelineOptionsFactory.create().as(JetPipelineOptions.class);
	
    jetPipelineOptions.setCodeJarPathname("train-track-beam-runner/target/train-track-beam-runner-shaded.jar");
    jetPipelineOptions.setJetDefaultParallelism(1);
    jetPipelineOptions.setJetGroupName("frecciarossa");
    jetPipelineOptions.setJetServers("127.0.0.1:8701");
    jetPipelineOptions.setJobName(MyBeamJob.class.getSimpleName());
    jetPipelineOptions.setRunner(JetRunner.class);

    Pipeline pipeline = MyBeamJob.build(jetPipelineOptions);
	    
    pipeline.run();
}
</pre>
This does three things.

It creates the object "<code>jetPipelineOptions</code>" which contains the specifics for running a Beam job in Hazelcast Jet. The essential parts being the Jar file that contains the code to run, and "<code>JetRunner</code>" as the thing that runs it.

It then creates the Beam processing pipeline, passing in the "<code>jetPipelineOptions</code>" as a parameter to be held within the processing job.

Lastly, it uses the "<code>pipeline.run()</code>" command to send the job to the execution engine for processing. We don't wait on the job to complete, fire and forget.
<h2>Understanding the Example</h2>
How does this <em>really</em> work?

This next section gives a more detailed explanation for the curious. Skip on to the summary if you prefer. If not, here goes explaining how the five modules in this demo interact,
<h3>Data Feed</h3>
The first job in the application is <code>train-track-data-feed</code>.

This has nothing to do with Hazelcast Jet or Apache Beam.

All it does is write a series of data points, from the code file "<code>FrecciaRossa.java</code>" to a file named "<code>beam-input</code>" in the top-level directory.

The GPS points are already captured, but we're trying to simulate a real feed coming in.

The filename "<code>beam-input</code>" is meant to imply it is the input file for Beam processing. Of course, it's also the output file from this module.

In the instructions, this is Step 4. But it doesn't have a dependency on steps 2 or 3.
<h3>Beam Job, <code>train-track-beam-job</code></h3>
This module is the whole point of the example; it defines processing logic using the Apache Beam Java SDK. There is no reference to Hazelcast Jet in this module.

The processing reads from a file called "<code>beam-input</code>" and writes to "<code>beam-output</code>". Between this read and write, the data is enriched.
<h4>Beam job Maven dependency</h4>
The Maven dependency for this module is:
<pre>&lt;dependency&gt;
    &lt;groupId&gt;org.apache.beam&lt;/groupId&gt;
    &lt;artifactId&gt;beam-sdks-java-core&lt;/artifactId&gt;
&lt;/dependency&gt;
</pre>
It has no reliance on Hazelcast Jet; everything is standard Apache Beam.
<h4>Beam job data enrichment</h4>
The input is in CSV format, for the latitude, longitude and time offset.

The time offset is in relation to when the train left Milan, the units are seconds.

The first point is "<code>45.487,9.205,0000</code>". The latitude is 45.487 North, the longitude is 9.205 West, <a href="https://goo.gl/maps/5TQKFHtdNuw5uWaH7">approximately here in Milan</a>. The time offset is zero as this is the first point.

The last point is "<code>45.059,7.675,3762</code>". The latitude is 45.059 North, the longitude is 7.675 West, <a href="https://goo.gl/maps/8vPfpbFG2JGAzykw8">approximately here in Turin</a>. The time offset is 3762 seconds, 62 minutes after recording began.
<h3>Beam Runner, <code>train-track-beam-runner</code></h3>
The Beam runner job is pretty trivial. As shown earlier, just one class. What that class does is send a Jar file to the Jet grid.

It has a dependency on the Beam pipeline that it submits, "<code>train-track-beam-job</code>":
<pre>    &lt;dependency&gt;
        &lt;groupId&gt;${project.groupId}&lt;/groupId&gt;
        &lt;artifactId&gt;train-track-beam-job&lt;/artifactId&gt;
        &lt;version&gt;${project.version}&lt;/version&gt;
    &lt;/dependency&gt;
</pre>
<h4>Shaded Jar</h4>
In the "<code>pom.xml</code>" file, you'll see that we use "<code>maven-shade-plugin</code>".

This takes this module ("<code>train-track-beam-runner</code>") and all its dependencies (especially "<code>train-track-beam-runner</code>"), expands them out and then re-bundles them as a single unit.

The result "<code>train-track-beam-runner-shaded.jar</code>" is more of a Jar that contains "<code>.class</code>" files than a Jar that contains "<code>.jar</code>" files, making it easier for Beam to find the classes.

We take a simplistic approach here. One of the bundled dependencies is Hazelcast Jet itself. When we submit this bundled Jar to the Hazelcast Jet grid, it will already have these classes. As the versions are the same, it does no harm, but we could make the configuration more selective at the expense of simplicity.
<h4>Jet Pipeline Options</h4>
In the main class, six options are used to configure the deployment:
<ul>
 	<li>
<pre>jetPipelineOptions.setCodeJarPathname("train-track-beam-runner/target/train-track-beam-runner-shaded.jar");</pre>
This is the Jar file we wish to send to the Jet cluster to execute and has been assembled using the "<code>maven-shade-plugin</code>".

It needs to include the pipeline job classes, plus some of the dependent classes used, like "<code>JetRunner</code>". Due to the basic configuration used for shading, this Jar also contains some unnecessary classes.</li>
 	<li>
<pre>jetPipelineOptions.setJetDefaultParallelism(1);</pre>
For Jet to achieve high performance, a job will generally run multiple instances in each JVM, and a Jet cluster will have multiple JVMs. So for each, the processing will be highly parallel.

But we only have one input file to process, so we reduce down the parallelism to make things simpler to understand.</li>
 	<li>
<pre>jetPipelineOptions.setJetGroupName("frecciarossa");</pre>
This is the cluster name for the Jet cluster we wish to connect. No password credentials are needed in this example as security is not enabled.</li>
 	<li>
<pre>jetPipelineOptions.setJetServers("127.0.0.1:8701");</pre>
This option lists *some* of the Jet servers in the cluster to attempt a connection. The first reply includes the addresses of all the others, and then connections are opened to them, too.

Generally, 2 or 3 are enough places to try, but since this example only uses one server, that's all we should specify.</li>
 	<li>
<pre>jetPipelineOptions.setJobName(MyBeamJob.class.getSimpleName());</pre>
For visualization on the <a href="https://jet.hazelcast.org/download/#management-center">Hazelcast Jet Management Center</a>, it is useful if the job has a name.</li>
 	<li>
<pre>jetPipelineOptions.setRunner(JetRunner.class);</pre>
This is the class that will run the Beam pipeline against the Jet grid. For now, it will always be set to "<code>JetRunner</code>".</li>
</ul>
<h3>Jet Grid, <code>train-track-grid</code></h3>
The Jet grid for this example is relatively trivial. A single node is all we need. The group name for this grid (of 1) is <em>frecciarossa</em>. It runs on port 8701 to avoid a clash with any other grid on the default port of 5701 which may also be present.

When this grid starts, it doesn't know of the Beam job and therefore won't be running it. Indeed, it doesn't have any of the Beam classes in its classpath (check the "<code>pom.xml</code>"), until the "<code>train-track-beam-runner</code>" send these classes to the grid.

The one non-standard piece of the grid is a Jet job (not a Beam job) configured to run at start-up, called <em>FileWatcher</em>.
<h4>FileWatcher Job</h4>
The class "<code>FileWatcher.java</code>" defines a Jet job to run when the grid starts. It only contains two effective lines.
<pre>pipeline.drawFrom(FileWatcher.buildFileWatcherSource()).withoutTimestamps()		 
        .drainTo(FileWatcher.buildTopicSink());
</pre>
What this job does is look for lines in files named "<code>beam-output*</code>" and publish those lines to a Hazelcast topic named <em>treno</em>.

This is just a bit of plumbing.

The Jet job detects each line written by the Beam job, and that line is written to a Hazelcast topic for the Web UI. The Beam job can't do this itself, as the Beam job doesn't know it is running in Hazelcast Jet.
<h3>Web UI, <code>train-track-web-ui</code></h3>
The last module, <code>train-track-web-ui</code>, does some cool stuff, but it's not much to do with Hazelcast.

The Java part of the coding is again just a bit of plumbing. What it does is subscribe to the Hazelcast topic "<em>treno</em>" that "<code>train-track-grid</code>" is publishing to, and passing the message payload received through a Web Socket to JavaScript running in your browser.

The Web UI runs on port 8085, to avoid potential clashes with <a href="https://hazelcast.org/download/#management-center">Hazelcast IMDG Management Center</a> on 8080 or <a href="https://jet.hazelcast.org/download/#management-center">Hazelcast Jet Management Center</a> on 8081.

All you need do is point your browser at:
<pre>http://localhost:8085/
</pre>
In the browser, <a href="http://openstreetmap.org">OpenStreetMap</a> is used to do the plotting. The code is mostly just this:
<pre>function onMessage(message) {
	var json = JSON.parse(message);
	var latitude = json.latitude;
	var longitude = json.longitude;
	var timeprint = json.timeprint;
	
	var myMarker = L.marker([latitude, longitude]);
	myMarker.bindPopup('&lt;p&gt;' + timeprint + '&lt;/p&gt;');
	myMarker.addTo(map);
	
	myMarker.openPopup();
}
</pre>
When a message is received from the Web Socket, it is parsed as JSON and the fields with the text names <em>latitude</em>, <em>longitude, </em>and <em>timeprint</em> are extracted.

A marker point is plotted on the map using the latitude and longitude, and a text field is added to this marker containing the time of day.

Data arrives every up to four points a second, with the time of day shown on the most recent. Old points are still present, so we can see where the train has been.

Gaps in the points shown might be a tunnel, be when there was no signal to deliver the message or some other fault.
<h3>Tracing the data flow</h3>
So, if all four processes are running, what will happen is roughly this:

"<code>train-track-data-feed</code>" will write one CSV line per second to a file named "<code>beam-input</code>". eg.
<pre>45.059,7.675,3762
</pre>
"<code>train-track-beam-job</code>" running in the Jet grid will read that one CSV line from "<code>beam-input</code>" and write JSON to "<code>beam-output*</code>":
<pre>{ "input-point": "2057", "latitude": "45.059", "longitude": "7.675", "timestamp": "1565645192996", "timeprint": "22:26:32" }
</pre>
The <em>FileWatcher</em> job running in the Jet grid will read the one JSON line from "<code>beam-output*</code>" and publish it unchanged to the topic "<code>treno</code>":
<pre>{ "input-point": "2057", "latitude": "45.059", "longitude": "7.675", "timestamp": "1565645192996", "timeprint": "22:26:32" }
</pre>
The "<code>train-track-web-ui</code>" subscribes to the "<code>treno</code>" topic, and passes this JSON on through a web socket to your browser:
<pre>{ "input-point": "2057", "latitude": "45.059", "longitude": "7.675", "timestamp": "1565645192996", "timeprint": "22:26:32" }
</pre>
Code in the browser plots the JSON on a map!

You'll note that only Beam really does any proper processing. Everything else is just moving data from place to place without enrichment, aggregation, or the like.
<h2>Summary</h2>
This example shows how to create and execute an Apache Beam processing job in Hazelcast Jet. It also subliminally teaches you the location of two cities in northern Italy.

You can define a Beam processing job in Java just as before.

If you have Apache Beam 2.14 or later, the new "<code>JetRunner</code>" allows you to submit this to Hazelcast Jet for execution. We do it here with ten lines of code, though you could trim that a bit.

And that's it! You can run an Apache Beam job in Hazelcast Jet or elsewhere. The trade-off is you don't get to use all the power of Jet specifics, the sacrifice necessary to go with implementation-independent processing.

