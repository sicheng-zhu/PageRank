// Register controller
var app = angular.module("pageRankApp", ['ngSanitize']);
app.controller("contentControl", function($scope) {
	$scope.container = "<svg width=\"1024\" height=\"500\"></svg>";

	$scope.notes = 
		"<ol>" +
			"<li>Black node represents the web page you just selected. The thickness of edge represents the value of PageRank.</li>" +
			"<li>Click a node to display the connected graph of how this node connects to other nodes.</li>" +
		"</ol>";
});