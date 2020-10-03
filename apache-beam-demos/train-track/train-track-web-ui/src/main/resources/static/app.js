/* ------------------------------
 * Variables, "L" is leaflet
 * ------------------------------
 */

// Create a map in the "map" div on the page, initial top-left near Milan latitude-longitude
var map = L.map('map').setView([45.4, 9.35], 10);

// Use of Open Street Map requires this text
var openStreetMap = '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors,'
                + '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>';

/* ------------------------------
 * Functions
 * ------------------------------
 */

/* Show a point received on the map.
 */
function onMessage(message) {
	// Extract fields from the JSON message object
	var json = JSON.parse(message);
	var latitude = json.latitude;
	var longitude = json.longitude;
	var timeprint = json.timeprint;
	
	// Create a point and add to the map. Add pop-up text if clicked
	var myMarker = L.marker([latitude, longitude]);
	myMarker.bindPopup('<p>' + timeprint + '</p>');
	myMarker.addTo(map);
	
	// Open force closes the previous
	myMarker.openPopup();
}

/* This function is invoked when the page loads. It connects
 * to the web-socket, subscribes to the named queue, and
 * invokes the above function for each message received
 * on the queue.
 */
$(function () {
	// Connect to the "hazelcast" socket
    var stompClient = Stomp.over(new SockJS('/hazelcast'));
    
    // Listen on the queue
    stompClient.connect({}, function (frame) {
        stompClient.subscribe('/queue/location', function (message) {
            onMessage(message.body);
        });
    });
});

/* Add the chart to the map box on the page
 */
L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        minZoom: 7,
        attribution: openStreetMap,
        }
).addTo(map);