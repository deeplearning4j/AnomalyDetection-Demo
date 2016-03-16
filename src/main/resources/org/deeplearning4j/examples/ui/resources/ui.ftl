<!DOCTYPE html>
<html>
<head>
    <style type="text/css">
        html, body {
            width: 100%;
            height: 100%;
        }

        .bgcolor {
            background-color: #FFFFFF;
        }

        .hd {
            background-color: #000000;
            font-size: 18px;
            color: #FFFFFF;
        }

        .sectionheader {
            background-color: #888888;
            font-size: 16px;
            font-style: bold;
            color: #FFFFFF;
            padding-left: 40px;
            padding-right: 8px;
            padding-top: 2px;
            padding-bottom: 2px;

        }

        .subsectiontop {
            background-color: #F5F5FF;
            height: 300px;
        }

        .subsectionbottom {
            background-color: #F5F5FF;
            height: 540px;
        }

        h1 {
            font-family: Georgia, Times, 'Times New Roman', serif;
            font-size: 28px;
            font-style: bold;
            font-variant: normal;
            font-weight: 500;
            line-height: 26.4px;
        }

        h3 {
            font-family: Georgia, Times, 'Times New Roman', serif;
            font-size: 16px;
            font-style: normal;
            font-variant: normal;
            font-weight: 500;
            line-height: 26.4px;
        }

        div.outerelements {
            padding-bottom: 30px;
        }

        /** Line charts */
        path {
            stroke: steelblue;
            stroke-width: 2;
            fill: none;
        }

        .axis path, .axis line {
            fill: none;
            stroke: #000;
            shape-rendering: crispEdges;
        }

        .tick line {
            opacity: 0.2;
            shape-rendering: crispEdges;
        }

        /** Bar charts */
        .bar {
            fill: steelblue;
        }

        .legend rect {
            fill:white;
            stroke:black;
            opacity:0.8;
        }

    </style>
    <title>Network Intrusion Detection</title>
</head>
<body class="bgcolor">

<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">

<script src="//ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
<link rel="stylesheet" href="//code.jquery.com/ui/1.11.4/themes/smoothness/jquery-ui.css">
<script src="//code.jquery.com/jquery-1.10.2.js"></script>
<script src="//code.jquery.com/ui/1.11.4/jquery-ui.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3.min.js"></script>
<script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<script src="/assets/d3.legend.js"></script>

<script>
    //Store last update times:
    var lastStatusUpdateTime = -1;
    var lastSettingsUpdateTime = -1;
    var lastResultsUpdateTime = -1;

    var resultTableSortIndex = 0;
    var resultTableSortOrder = "ascending";
    var resultsTableContent;

    var expandedRowsCandidateIDs = [];

    //Interval function to do updates:
    setInterval(function () {

        //Connections/sec chart:
        $.get("/charts/connection", function (data) {
            var jsonObj = JSON.parse(JSON.stringify(data));
            //console.log(jsonObj);

            //Expect a line chart here...
            var connectionRateDiv = $('#connectionChartDiv');
            connectionRateDiv.html('');

            createAndAddComponent(jsonObj, connectionRateDiv, 460, 260);
        });

        //Bytes/sec chart:
        $.get("/charts/bytes", function (data) {
            var jsonObj = JSON.parse(JSON.stringify(data));
            //console.log(jsonObj);

            //Expect a line chart here...
            var byteRateDiv = $('#bytesChartDiv');
            byteRateDiv.html('');

            createAndAddComponent(jsonObj, byteRateDiv, 460, 260);
        });

        //Types of flows area chart:
        $.get("/areachart/", function (data) {
            var jsonObj = JSON.parse(JSON.stringify(data));
//            console.log(jsonObj);

            //Expect a line chart here...
            var areaDiv = $('#flowsAreaChartDiv');
            areaDiv.html('');

            createAndAddComponent(jsonObj, areaDiv, 460, 260);
        });

        //Summary of network attacks chart
        $.get("/table", function (data) {
            var jsonObj = JSON.parse(JSON.stringify(data));
            var tableDiv = $('#attackTableDiv');

            tableDiv.html('');
            createAndAddComponent(jsonObj, tableDiv, 0, 0);

        });



    }, 1000);

    //Intercept click events on table rows:
    $(function () {
        $('#attackTableDiv').delegate("td", "click", function (e) {
//            console.log("Row clicked on: " + $(e.currentTarget).index() + " - " + $(e.currentTarget).html());
//            var rowIdx = $(this).parent().index();
//            var colIdx = $(this).index();

            //Get the details for the row clicked on...
            var flowNumber = $(this).parent().children('td').eq(0).text();
            var path = "/flow/" + flowNumber;

            $.get(path, function (data) {
                var jsonObj = JSON.parse(JSON.stringify(data));
                var detailsDiv = $('#attackInfoDiv');

                var components = jsonObj['renderableComponents'];

                detailsDiv.html('');
                var nComponents = components.length;
                for (var i = 0; i < nComponents; i++) {
                    createAndAddComponent(components[i], detailsDiv, 660, 215);
                }
            });
        });
    });


    function createAndAddComponent(renderableComponent, appendTo, width, height) {
        var key = Object.keys(renderableComponent)[0];
        var type = renderableComponent[key]['componentType'];

        switch (type) {
            case "string":
                var s = renderableComponent[key]['string'];
                appendTo.append(s);
                break;
            case "simpletable":
                createTable(renderableComponent[key], null, appendTo);
                break;
            case "linechart":
                createLineChart(renderableComponent[key], appendTo, width, height);
                break;
            case "scatterplot":
                createScatterPlot(renderableComponent[key], appendTo);
                break;
            case "accordion":
                createAccordion(renderableComponent[key], appendTo);
                break;
            case "horizontalbarchart":
                createHorizontalBarChart(renderableComponent[key], appendTo, width, height);
                break;
            case "stackedareachart":
                createStackedAreaChart(renderableComponent[key], appendTo, width, height);
                break;
            default:
                return "(Error rendering component: Unknown object)";
        }
    }

    function createTable(tableObj, tableId, appendTo) {
        //Expect RenderableComponentTable
        var header = tableObj['header'];
        var values = tableObj['table'];
        var title = tableObj['title'];
        var border = tableObj['border'];
        var padLeft = tableObj['padLeftPx'];
        var padRight = tableObj['padRightPx'];
        var padTop = tableObj['padTopPx'];
        var padBottom = tableObj['padBottomPx'];
        var colWidths = tableObj['colWidthsPercent'];
        var nRows = (values ? values.length : 0);
        var backgroundColor = tableObj['backgroundColor'];
        var headerColor = tableObj['headerColor'];


        var tbl = document.createElement('table');
        tbl.style.width = '100%';
        tbl.style.height = '100%';
        tbl.setAttribute('border', border);
        if(backgroundColor) tbl.style.backgroundColor = backgroundColor;

        if (colWidths) {
            for (var i = 0; i < colWidths.length; i++) {
                var col = document.createElement('col');
                col.setAttribute('width', colWidths[i] + '%');
                tbl.appendChild(col);
            }
        }

        if (header) {
            var theader = document.createElement('thead');
            var headerRow = document.createElement('tr');

            if(headerColor) headerRow.style.backgroundColor = headerColor;

            for (var i = 0; i < header.length; i++) {
                var headerd = document.createElement('th');
                headerd.style.padding = padTop + 'px ' + padRight + 'px ' + padBottom + 'px ' + padLeft + 'px';
                headerd.appendChild(document.createTextNode(header[i]));
                headerRow.appendChild(headerd);
            }
            tbl.appendChild(headerRow);
        }

        //Add content:
        if (values) {

            var tbdy = document.createElement('tbody');
            for (var i = 0; i < values.length; i++) {
                var tr = document.createElement('tr');

                for (var j = 0; j < values[i].length; j++) {
                    var td = document.createElement('td');
                    td.style.padding = padTop + 'px ' + padRight + 'px ' + padBottom + 'px ' + padLeft + 'px';
                    td.appendChild(document.createTextNode(values[i][j]));
                    tr.appendChild(td);
                }

                tbdy.appendChild(tr);
            }
            tbl.appendChild(tbdy);
        }

        appendTo.append(tbl);
    }

    /** Create + add line chart with multiple lines, (optional) title, (optional) series names.
     * appendTo: jquery selector of object to append to. MUST HAVE ID
     * */
    function createLineChart(chartObj, appendTo, chartWidth, chartHeight) {
        //Expect: RenderableComponentLineChart
        var title = chartObj['title'];
        var xData = chartObj['x'];
        var yData = chartObj['y'];
        var mTop = chartObj['marginTop'];
        var mBottom = chartObj['marginBottom'];
        var mLeft = chartObj['marginLeft'];
        var mRight = chartObj['marginRight'];
        var removeAxisHorizontal = chartObj['removeAxisHorizontal'];
        var seriesNames = chartObj['seriesNames'];
        var withLegend = chartObj['legend'];
        var nSeries = (!xData ? 0 : xData.length);

        // Set the dimensions of the canvas / graph
        var margin = {top: mTop, right: mRight, bottom: mBottom, left: mLeft},
                width = chartWidth - margin.left - margin.right,
                height = chartHeight - margin.top - margin.bottom;

        // Set the ranges
        var xScale = d3.scale.linear().range([0, width]);
        var yScale = d3.scale.linear().range([height, 0]);

        // Define the axes
        var xAxis = d3.svg.axis().scale(xScale)
                .innerTickSize(-height)     //used as grid line
                .orient("bottom").ticks(5);

        if(removeAxisHorizontal == true){
            xAxis.tickValues([]);
        }

        var yAxis = d3.svg.axis().scale(yScale)
                .innerTickSize(-width)      //used as grid line
                .orient("left").ticks(5);

        // Define the line
        var valueline = d3.svg.line()
                .x(function (d) {
                    return xScale(d.xPos);
                })
                .y(function (d) {
                    return yScale(d.yPos);
                });

        // Adds the svg canvas
        var svg = d3.select("#" + appendTo.attr("id"))
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .attr("padding", "20px")
                .append("g")
                .attr("transform",
                        "translate(" + margin.left + "," + margin.top + ")");

        // Scale the range of the chart
        var xMin = Number.MAX_VALUE;
        var xMax = -Number.MAX_VALUE;
        var yMax = -Number.MAX_VALUE;
        var yMin = Number.MAX_VALUE;
        for (var i = 0; i < nSeries; i++) {
            var xV = xData[i];
            var yV = yData[i];
            var thisMin = d3.min(xV);
            var thisMax = d3.max(xV);
            var thisMaxY = d3.max(yV);
            var thisMinY = d3.min(yV);
            if (thisMin < xMin) xMin = thisMin;
            if (thisMax > xMax) xMax = thisMax;
            if (thisMaxY > yMax) yMax = thisMaxY;
            if (thisMinY < yMin) yMin = thisMinY;
        }
        if (yMin > 0) yMin = 0;
        xScale.domain([xMin, xMax]);
        yScale.domain([yMin, yMax]);

        // Add the valueline path.
        var color = d3.scale.category10();
        for (var i = 0; i < nSeries; i++) {
            var xVals = xData[i];
            var yVals = yData[i];

            var data = xVals.map(function (d, i) {
                return {'xPos': xVals[i], 'yPos': yVals[i]};
            });
            svg.append("path")
                    .attr("class", "line")
                    .style("stroke", color(i))
                    .attr("d", valueline(data));
        }

        // Add the X Axis
        svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

        // Add the Y Axis
        svg.append("g")
                .attr("class", "y axis")
                .call(yAxis);

        //Add legend (if present)
        if (seriesNames && withLegend == true) {
            var legendSpace = width / i;
            for (var i = 0; i < nSeries; i++) {
                var values = xData[i];
                var yValues = yData[i];
                var lastX = values[values.length - 1];
                var lastY = yValues[yValues.length - 1];
                var toDisplay;
                if (!lastX || !lastY) toDisplay = seriesNames[i] + " (no data)";
                else toDisplay = seriesNames[i] + " (" + lastX.toPrecision(5) + "," + lastY.toPrecision(5) + ")";
                svg.append("text")
                        .attr("x", (legendSpace / 2) + i * legendSpace) // spacing
                        .attr("y", height + (margin.bottom / 2) + 5)
                        .attr("class", "legend")    // style the legend
                        .style("fill", color(i))
                        .text(toDisplay);

            }
        }

        //Add title (if present)
        if (title) {
            svg.append("text")
                    .attr("x", (width / 2))
                    .attr("y", 0 - ((margin.top - 30) / 2))
                    .attr("text-anchor", "middle")
                    .style("font-size", "13px")
                    .style("text-decoration", "underline")
                    .text(title);
        }
    }

    /** Create + add scatter plot chart with multiple different types of points, (optional) title, (optional) series names.
     * appendTo: jquery selector of object to append to. MUST HAVE ID
     * */
    function createScatterPlot(chartObj, appendTo) {
        //TODO modify this to do scatter plot, not line chart
        //Expect: RenderableComponentLineChart
        var title = chartObj['title'];
        var xData = chartObj['x'];
        var yData = chartObj['y'];
        var seriesNames = chartObj['seriesNames'];
        var nSeries = (!xData ? 0 : xData.length);
        var title = chartObj['title'];

        // Set the dimensions of the canvas / graph
        var margin = {top: 60, right: 20, bottom: 60, left: 50},
                width = 650 - margin.left - margin.right,
                height = 350 - margin.top - margin.bottom;

        // Set the ranges
        var xScale = d3.scale.linear().range([0, width]);
        var yScale = d3.scale.linear().range([height, 0]);

        // Define the axes
        var xAxis = d3.svg.axis().scale(xScale)
                .innerTickSize(-height)     //used as grid line
                .orient("bottom").ticks(5);

        var yAxis = d3.svg.axis().scale(yScale)
                .innerTickSize(-width)      //used as grid line
                .orient("left").ticks(5);

        // Define the line
        var valueline = d3.svg.line()
                .x(function (d) {
                    return xScale(d.xPos);
                })
                .y(function (d) {
                    return yScale(d.yPos);
                });

        // Adds the svg canvas
        var svg = d3.select("#" + appendTo.attr("id"))
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .attr("padding", "20px")
                .append("g")
                .attr("transform",
                        "translate(" + margin.left + "," + margin.top + ")");

        // Scale the range of the chart
        var xMax = -Number.MAX_VALUE;
        var yMax = -Number.MAX_VALUE;
        var yMin = Number.MAX_VALUE;
        for (var i = 0; i < nSeries; i++) {
            var xV = xData[i];
            var yV = yData[i];
            var thisMax = d3.max(xV);
            var thisMaxY = d3.max(yV);
            var thisMinY = d3.min(yV);
            if (thisMax > xMax) xMax = thisMax;
            if (thisMaxY > yMax) yMax = thisMaxY;
            if (thisMinY < yMin) yMin = thisMinY;
        }
        if (yMin > 0) yMin = 0;
        xScale.domain([0, xMax]);
        yScale.domain([yMin, yMax]);

        // Add the valueline path.
        var color = d3.scale.category10();
        for (var i = 0; i < nSeries; i++) {
            var xVals = xData[i];
            var yVals = yData[i];

            var data = xVals.map(function (d, i) {
                return {'xPos': xVals[i], 'yPos': yVals[i]};
            });

            svg.selectAll("circle")
                    .data(data)
                    .enter()
                    .append("circle")
                    .style("fill", function (d) {
                        return color(i)
                    })
                    .attr("r", 3.0)
                    .attr("cx", function (d) {
                        return xScale(d['xPos']);
                    })
                    .attr("cy", function (d) {
                        return yScale(d['yPos']);
                    });
        }

        // Add the X Axis
        svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

        // Add the Y Axis
        svg.append("g")
                .attr("class", "y axis")
                .call(yAxis);

        //Add legend (if present)
        if (seriesNames) {
            var legendSpace = width / i;
            for (var i = 0; i < nSeries; i++) {
                var values = xData[i];
                var yValues = yData[i];
                var lastX = values[values.length - 1];
                var lastY = yValues[yValues.length - 1];
                var toDisplay;
                if (!lastX || !lastY) toDisplay = seriesNames[i] + " (no data)";
                else toDisplay = seriesNames[i] + " (" + lastX.toPrecision(5) + "," + lastY.toPrecision(5) + ")";
                svg.append("text")
                        .attr("x", (legendSpace / 2) + i * legendSpace) // spacing
                        .attr("y", height + (margin.bottom / 2) + 5)
                        .attr("class", "legend")    // style the legend
                        .style("fill", color(i))
                        .text(toDisplay);

            }
        }

        //Add title (if present)
        if (title) {
            svg.append("text")
                    .attr("x", (width / 2))
                    .attr("y", 0 - ((margin.top - 30) / 2))
                    .attr("text-anchor", "middle")
                    .style("font-size", "13px")
                    .style("text-decoration", "underline")
                    .text(title);
        }
    }

    function createHorizontalBarChart(chartObj, appendTo, chartWidth, chartHeight) {

        var title = chartObj['title'];
        var labels = chartObj['labels'];
        var values = chartObj['values'];
        var mTop = chartObj['marginTop'];
        var mBottom = chartObj['marginBottom'];
        var mLeft = chartObj['marginLeft'];
        var mRight = chartObj['marginRight'];
        var xMin = chartObj['xmin'];
        var xMax = chartObj['xmax'];

        var margin = {top: mTop, right: mRight, bottom: mBottom, left: mLeft},
                width = chartWidth - margin.left - margin.right,
                height = chartHeight - margin.top - margin.bottom;

        var x = d3.scale.linear()
                .range([0, width]);

        var y = d3.scale.ordinal()
                .rangeRoundBands([0, height], 0.1);

        var xAxis = d3.svg.axis()
                .scale(x)
                .orient("bottom");

        var yAxis = d3.svg.axis()
                .scale(y)
                .orient("left")
                .tickSize(0)
                .tickPadding(6);

        var svg = d3.select("#" + appendTo.attr("id"))
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var data = labels.map(function (d, i) {
            return {'name': labels[i], 'value': values[i]};
        });

        var chartXMin;
        if(xMin) chartXMin = xMin;
        else chartXMin = d3.min(values);

        var chartXMax;
        if(xMax) chartXMax = xMax;
        else chartXMax = d3.max(values);

        x.domain([chartXMin,chartXMax]);
        y.domain(data.map(function (d) {
            return d.name;
        }));

        svg.selectAll(".bar")
                .data(data)
                .enter().append("rect")
                .attr("class","bar")
                .attr("x", function (d) {
                    return x(Math.min(0, d.value));
                })
                .attr("y", function (d) {
                    return y(d.name);
                })
                .attr("width", function (d) {
                    return Math.abs(x(d.value) - x(0));
                })
                .attr("height", y.rangeBand());

        svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

        svg.append("g")
                .attr("class", "y axis")
                .attr("transform", "translate(" + x(0) + ",0)")
                .call(yAxis);

        //Add title (if present)
        if (title) {
            svg.append("text")
                    .attr("x", (width / 2))
                    .attr("y", 0 - ((margin.top - 30) / 2))
                    .attr("text-anchor", "middle")
                    .style("font-size", "13px")
                    .style("text-decoration", "underline")
                    .text(title);
        }
    }

    function createStackedAreaChart(chartObj, appendTo, chartWidth, chartHeight) {

        var title = chartObj['title'];
        var labels = chartObj['labels'];
        var mTop = chartObj['marginTop'];
        var mBottom = chartObj['marginBottom'];
        var mLeft = chartObj['marginLeft'];
        var mRight = chartObj['marginRight'];
        var removeAxisHorizontal = chartObj['removeAxisHorizontal'];

        var xValues = chartObj['x'];
        var yValuesList = chartObj['y'];

        //Convert data:
        var data = [];
        for(var i=0; i<xValues.length; i++ ){
            var obj = {};
            for( var j=0; j<labels.length; j++ ){
                obj[labels[j]] = yValuesList[j][i];
                obj['xValue'] = xValues[i];
            }
            data.push(obj);
        }

        var margin = {top: mTop, right: mRight, bottom: mBottom, left: mLeft},
                width = chartWidth - margin.left - margin.right,
                height = chartHeight - margin.top - margin.bottom;

        var x = d3.scale.linear()
                .range([0, width]);

        var y = d3.scale.linear()
                .range([height, 0]);

        var color = d3.scale.category20();

        var xAxis = d3.svg.axis()
                .scale(x);

        if(removeAxisHorizontal == true){
            xAxis.tickValues([]);
        }

        var yAxis = d3.svg.axis()
                .scale(y)
                .orient("left")


        var area = d3.svg.area()
                .x(function(d) { return x(d.xValue); })
                .y0(function(d) { return y(d.y0); })
                .y1(function(d) { return y(d.y0 + d.y); });

        var stack = d3.layout.stack()
                .values(function(d) { return d.values; });

        var svg = d3.select("#" + appendTo.attr("id")).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        color.domain(d3.keys(data[0]).filter(function (key) {
            return key !== "xValue";
        }));

        var browsers = stack(color.domain().map(function (name) {
            return {
                name: name,
                values: data.map(function (d) {
                    return {xValue: d.xValue, y: d[name] * 1};
                })
            };
        }));

        // Find the value of the day with highest total value
        var maxX = d3.max(data, function (d) {
            var vals = d3.keys(d).map(function (key) {
                return key !== "xValue" ? d[key] : 0
            });
            return d3.sum(vals);
        });

        // Set domains for axes
        x.domain(d3.extent(data, function (d) {
            return d.xValue;
        }));

        y.domain([0, maxX]);

        var browser = svg.selectAll(".browser")
                .data(browsers)
                .enter().append("g")
                .attr("class", "browser");

        browser.append("path")
                .attr("class", "area")
                .attr("data-legend",function(d) { return d.name})
                .attr("d", function (d) {
                    return area(d.values);
                })
                .style("fill", function (d) {
                    return color(d.name);
                })
                .style({"stroke-width": "0px"});

        //This appends the text labels to the right of the chart. Don't need this in addition to the legend
//        browser.append("text")
//                .datum(function (d) {
//                    return {name: d.name, value: d.values[d.values.length - 1]};
//                })
//                .attr("transform", function (d) {
//                    return "translate(" + x(d.value.xValue) + "," + y(d.value.y0 + d.value.y / 2) + ")";
//                })
//                .attr("x", -6)
//                .attr("dy", ".35em")
//                .text(function (d) {
//                    return d.name;
//                });

        svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

        //Vertical axis label
        svg.append("g")
                .attr("class", "y axis")
                .call(yAxis);

        legend = svg.append("g")
                .attr("class","legend")
                .attr("transform","translate(20,20)")
                .style("font-size","12px")
                .call(d3.legend);

    }
</script>


<table style="width: 100%; padding: 5px;" class="hd">
    <tbody>
    <tr>
        <td style="width:143px; height:35px; padding-left:15px; padding-right:15px; padding-top:4px; padding-bottom:4px">
            <a href="/"><img src="/assets/skymind_w.png" border="0"/></a></td>
        <td> Network Intrusion Detection Demo</td>
    </tr>
    </tbody>
</table>

<div style="width:1400px; margin:0 auto;" id="outerdiv">
    <div style="width:100%; padding-top:20px">
        <div style="width:33.333%; float:left;" class="subsectiontop" id="connectionChartOuter">
            <div style="width:100%;" class="sectionheader">
                Network Utilization: Connections/sec
            </div>
            <div style="width:100%; height:100%; float:left;" id="connectionChartDiv">
            </div>
        </div>
        <div style="width:33.333%; float:left;" class="subsectiontop" id="bytesChartOuter">
            <div style="width:100%;" class="sectionheader">
                Network Utilization: kBytes/sec
            </div>
            <div style="width:100%; height:100%; float:left;" id="bytesChartDiv">
            </div>
        </div>
        <div style="width:33.333%; float:right;" class="subsectiontop">
            <div style="width:100%;" class="sectionheader" id="flowsAreaDiv">
                Network Connections by Service
            </div>
            <div style="width:100%; height:100%; float:left;" id="flowsAreaChartDiv">
            </div>

        </div>

        <div style="width:50%; float:left;" class="subsectionbottom">
            <div style="width:100%;" class="sectionheader">
                Summary: Network Attacks
            </div>
        <#--<div style="width:100%; height:100%" id="attackTableDiv">-->
            <div style="padding:10px" id="attackTableDiv">

            </div>
        </div>
        <div style="width:50%; float:right;" class="subsectionbottom">
            <div style="width:100%;" class="sectionheader">
                Connection/Attack Information
            </div>
            <div style="padding:10px" id="attackInfoDiv">

            </div>
        </div>
    </div>
</div>

</body>
</html>