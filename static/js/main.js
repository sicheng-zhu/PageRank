/*
This file converts page ranking information to force-directed graph by D3.js, and display on screen.
*/
// Get HTML elements on index.html page, and create SVG container.
var svg = d3.select("svg"),    // Graph is enclosed by svg tags.
    width = +svg.attr("width"),
    height = +svg.attr("height");

// Some settings.
var color = d3.scaleOrdinal(d3.schemeCategory20);
var queryID = getParameterByName('query');
var visible_num = 1000;  // Each node is only connected to 1000 nodes on screen for better view.

// Parse the page id in URL.
function getParameterByName(name, url) {
    if (!url) {
        url = window.location.href;
    }

    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
	
    if (!results) {
		return null;		
	}

    if (!results[2]) {
		return '';
	}
	
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

// Remove duplicate nodes.
function unique(arr) {
    var u = {}, a = [];
	
    for (var i = 0, l = arr.length; i < l; ++i) {
        if (!u.hasOwnProperty(arr[i])) {
            a.push(arr[i]);
            u[arr[i]] = 1;
        }
    }
	
    return a;
}

// Remove duplicate data, and limit the number connected to each node for better view.
function cleanData(json) {
    var DATA = {
        "nodes": [],
        "links": []
    };
    var nodes = [];
	
    for (var i = 0; i < json.links.length; ++i) {
        var leftRange = parseInt(queryID) - visible_num;
        var rightRange = parseInt(queryID) + visible_num;
		
        if ((leftRange <= json.links[i].source && rightRange >= json.links[i].source)
            || (leftRange <= json.links[i].target && rightRange >= json.links[i].target)) {
            nodes.push(json.links[i].source);
            nodes.push(json.links[i].target);
            DATA.links.push(json.links[i]);
        }
    }
	
    nodes = unique(nodes);
    for (var j = 0; j < nodes.length; ++j) {
        DATA.nodes.push({
            "id": nodes[j],
            "group": parseInt(nodes[j]) % 3
        });
    }
	
    return DATA;
}

// Restart the simulation when drag event starts on graph. Also reset alpha decay rate to 0.3.
function dragStarted(d) {
    if (!d3.event.active) {
		simulation.alphaTarget(0.3).restart();
	}
	// Assign current position to fixed position variables.
    d.fx = d.x;
    d.fy = d.y;
}

// Assign mouse position to fixed position(next position of this node after drag event) when dragging.
// After the drag, d.x value will be assigned to d.fx, and d.y value will be assigned to d.fy.
function dragged(d) {
    d.fx = d3.event.x;
    d.fy = d3.event.y;
}

// Clear fixed position after drag ends.
function dragEnded(d) {
    if (!d3.event.active) {
		simulation.alphaTarget(0); // Reset strength of forces this node give or receive to 0.
	}
	
    d.fx = null;
    d.fy = null;
}

// Create force-directed graph simulation, and add some forces.
var simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(function (d) { // Creates a new link force(force of attraction)
        return d.id;
    }))  
    .force("charge", d3.forceManyBody()) // Creates a new many-body force(Repulsive force)
    .force("center", d3.forceCenter(width / 2, height / 2)); // Creates a new centering force

// read json data, and convert data to SVG graph.
// d3.json("data/result.json", function (error, jsonData) {
d3.json("/jsonData", function (error, jsonData) {
    if (error) {
		throw error;
	}

    var graph = cleanData(jsonData);
    if (graph.nodes.length <= 1) {
        return;
    }
	
	// Create edges.
    var link = svg.append("g")
        .attr("class", "links")
        .selectAll("line") 
        .data(graph.links)
        .enter()
	    .append("line")
        .attr("stroke-width", function (d) {
            return Math.sqrt(d.value * 2);
        });

	// Show mouseover text when mouse hover over a node.
    var tooltip = d3.select("body")
        .append("div")
        .style("position", "absolute")
        .style("font-size", "small")
        .style("font-weight", "600")
        .style("margin-left", "10px")
        .style("z-index", "10")
        .style("visibility", "hidden")
        .text("xxx");

	// Create nodes.
    var node = svg.append("g")
        .attr("class", "nodes")
        .selectAll("circle")  
        .data(graph.nodes)
        .enter()
	    .append("circle")
        .attr("class", "node")
        .attr("r", function (d) {
            if (d.id == queryID) {
                return 10; // If the node is selected, make size 10.
            } else {
                return 5;  // If the node is not selected, make size 5.
            }
        })
        .attr("fill", function (d) {
            if (d.id == queryID) {
                return 'black'; // If the node is selected, make node black.
            } else {
                return color(d.group);
            }
        })
        .on("mouseover", function () {
            tooltip.text("<- " + d3.select(this).text());
            return tooltip.style("visibility", "visible");
        })
        .on("mousemove", function () {
            return tooltip.style("top", (event.pageY - 10) + "px").style("left", (event.pageX + 10) + "px");
        })
        .on("mouseout", function () {
            return tooltip.style("visibility", "hidden");
        })
        .on("mousedown", function () {
            location.href = '?query=' + d3.select(this).text();
        })
        .call(d3.drag() // Each node on graph can be dragged.
            .on("start", dragStarted)
            .on("drag", dragged)
            .on("end", dragEnded));

	// Display page id on node
    node.append("title")
        .text(function (d) {
            return d.id;
        });
	
	// Add graph nodes to graph.
    simulation.nodes(graph.nodes)
		      .on("tick", ticked);

	// Add link force(force of attraction) to graph links.
    simulation.force("link")
		      .links(graph.links);

	// The tick handler is the function that get the state of the layout when 
	// it has changed, and act on it. In particular, redraw the nodes and links where they 
	// currently are in the simulation.
    function ticked() {
        link.attr("x1", function (d) {
                return d.source.x;
            })
            .attr("y1", function (d) {
                return d.source.y;
            })
            .attr("x2", function (d) {
                return d.target.x;
            })
            .attr("y2", function (d) {
                return d.target.y;
            });

        node.attr("cx", function (d) {
                return d.x;
            })
            .attr("cy", function (d) {
                return d.y;
            });
    }
});

// Add query ID (page id) to search bar.
$(".searchbar").val(queryID);