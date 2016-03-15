<!DOCTYPE html>
<html>
<head>
    <style type="text/css">
        html, body {
            width: 100%;
            height: 100%;
            /*padding-top: 20px;*/
            /*padding-left: 20px;*/
            /*padding-right: 20px;*/
            /*padding-bottom: 20px;*/
        }

        .bgcolor {
            /*background-color: #DCEBF2;*/
            background-color: #FFFFFF;
        }

        .hd {
            background-color: #000000;
            font-size: 18px;
            color: #FFFFFF;
        }

        .sectionheader {
            background-color: #BBBBBB;
            font-size: 16px;
            font-style: bold;
            color: #FFFFFF;
            padding-left: 8px;
            padding-right: 8px;
            padding-top: 2px;
            padding-bottom: 2px;

        }

        .subsectiontop {
            background-color: #F5F5FF;
            height:320px;
        }
        .subsectionbottom {
            background-color: #F5F5FF;
            height: 500px;
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
    setInterval(function(){

        $.get("/charts/connection",function(data){
            var jsonObj = JSON.parse(JSON.stringify(data));
            //console.log(jsonObj);

            //Expect a line chart here...
            var connectionRateDiv = $('#connectionChartDiv');
            connectionRateDiv.html('');

            createAndAddComponent(jsonObj,connectionRateDiv, 460, 300);
        })

        $.get("/charts/bytes",function(data){
            var jsonObj = JSON.parse(JSON.stringify(data));
            //console.log(jsonObj);

            //Expect a line chart here...
            var byteRateDiv = $('#bytesChartDiv');
            byteRateDiv.html('');

            createAndAddComponent(jsonObj,byteRateDiv, 460, 300);
        })


    },1000);


    function createAndAddComponent(renderableComponent, appendTo, width, height){
        var key = Object.keys(renderableComponent)[0];
        var type = renderableComponent[key]['componentType'];

        switch(type){
            case "string":
                var s = renderableComponent[key]['string'];
                appendTo.append(s);
                break;
            case "simpletable":
                createTable(renderableComponent[key],null,appendTo);
                break;
            case "linechart":
                createLineChart(renderableComponent[key],appendTo, width, height);
                break;
            case "scatterplot":
                createScatterPlot(renderableComponent[key],appendTo);
                break;
            case "accordion":
                createAccordion(renderableComponent[key],appendTo);
                break;
            default:
                return "(Error rendering component: Unknown object)";
        }
    }

    function createTable(tableObj,tableId,appendTo){
        //Expect RenderableComponentTable
        var header = tableObj['header'];
        var values = tableObj['table'];
        var title = tableObj['title'];
        var nRows = (values ? values.length : 0);

        if(title){
            appendTo.append("<h5>"+title+"</h5>");
        }

        var table;
        if(tableId) table = $("<table id=\"" + tableId + "\" class=\"renderableComponentTable\">");
        else table = $("<table class=\"renderableComponentTable\">");
        if(header){
            var headerRow = $("<tr>");
            var len = header.length;
            for( var i=0; i<len; i++ ){
                headerRow.append($("<th>" + header[i] + "</th>"));
            }
            headerRow.append($("</tr>"));
            table.append(headerRow);
        }

        if(values){
            for( var i=0; i<nRows; i++ ){
                var row = $("<tr>");
                var rowValues = values[i];
                var len = rowValues.length;
                for( var j=0; j<len; j++ ){
                    row.append($('<td>'+rowValues[j]+'</td>'));
                }
                row.append($("</tr>"));
                table.append(row);
            }
        }

        table.append($("</table>"));
        appendTo.append(table);
    }

    /** Create + add line chart with multiple lines, (optional) title, (optional) series names.
     * appendTo: jquery selector of object to append to. MUST HAVE ID
     * */
    function createLineChart(chartObj, appendTo, chartWidth, chartHeight){
        //Expect: RenderableComponentLineChart
        var title = chartObj['title'];
        var xData = chartObj['x'];
        var yData = chartObj['y'];
        var seriesNames = chartObj['seriesNames'];
        var nSeries = (!xData ? 0 : xData.length);
        var title = chartObj['title'];

        // Set the dimensions of the canvas / graph
        var margin = {top: 60, right: 20, bottom: 60, left: 60},
                width = chartWidth - margin.left - margin.right,
                height = chartHeight - margin.top - margin.bottom;

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
                .x(function(d) { return xScale(d.xPos); })
                .y(function(d) { return yScale(d.yPos); });

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
        for( var i=0; i<nSeries; i++){
            var xV = xData[i];
            var yV = yData[i];
            var thisMin = d3.min(xV);
            var thisMax = d3.max(xV);
            var thisMaxY = d3.max(yV);
            var thisMinY = d3.min(yV);
            if(thisMin < xMin) xMin = thisMin;
            if(thisMax > xMax) xMax = thisMax;
            if(thisMaxY > yMax) yMax = thisMaxY;
            if(thisMinY < yMin) yMin = thisMinY;
        }
        if(yMin > 0) yMin = 0;
        xScale.domain([xMin, xMax]);
        yScale.domain([yMin, yMax]);

        // Add the valueline path.
        var color = d3.scale.category10();
        for( var i=0; i<nSeries; i++){
            var xVals = xData[i];
            var yVals = yData[i];

            var data = xVals.map(function(d, i){
                return { 'xPos' : xVals[i], 'yPos' : yVals[i] };
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
        if(seriesNames) {
            var legendSpace = width / i;
            for (var i = 0; i < nSeries; i++) {
                var values = xData[i];
                var yValues = yData[i];
                var lastX = values[values.length - 1];
                var lastY = yValues[yValues.length - 1];
                var toDisplay;
                if(!lastX || !lastY) toDisplay = seriesNames[i] + " (no data)";
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
        if(title){
            svg.append("text")
                    .attr("x", (width / 2))
                    .attr("y", 0 - ((margin.top-30) / 2))
                    .attr("text-anchor", "middle")
                    .style("font-size", "13px")
                    .style("text-decoration", "underline")
                    .text(title);
        }
    }

    /** Create + add scatter plot chart with multiple different types of points, (optional) title, (optional) series names.
     * appendTo: jquery selector of object to append to. MUST HAVE ID
     * */
    function createScatterPlot(chartObj, appendTo){
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
                .x(function(d) { return xScale(d.xPos); })
                .y(function(d) { return yScale(d.yPos); });

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
        for( var i=0; i<nSeries; i++){
            var xV = xData[i];
            var yV = yData[i];
            var thisMax = d3.max(xV);
            var thisMaxY = d3.max(yV);
            var thisMinY = d3.min(yV);
            if(thisMax > xMax) xMax = thisMax;
            if(thisMaxY > yMax) yMax = thisMaxY;
            if(thisMinY < yMin) yMin = thisMinY;
        }
        if(yMin > 0) yMin = 0;
        xScale.domain([0, xMax]);
        yScale.domain([yMin, yMax]);

        // Add the valueline path.
        var color = d3.scale.category10();
        for( var i=0; i<nSeries; i++){
            var xVals = xData[i];
            var yVals = yData[i];

            var data = xVals.map(function(d, i){
                return { 'xPos' : xVals[i], 'yPos' : yVals[i] };
            });

            svg.selectAll("circle")
                    .data(data)
                    .enter()
                    .append("circle")
                    .style("fill", function(d){ return color(i)})
                    .attr("r",3.0)
                    .attr("cx", function(d){ return xScale(d['xPos']); })
                    .attr("cy", function(d){ return yScale(d['yPos']); });
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
        if(seriesNames) {
            var legendSpace = width / i;
            for (var i = 0; i < nSeries; i++) {
                var values = xData[i];
                var yValues = yData[i];
                var lastX = values[values.length - 1];
                var lastY = yValues[yValues.length - 1];
                var toDisplay;
                if(!lastX || !lastY) toDisplay = seriesNames[i] + " (no data)";
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
        if(title){
            svg.append("text")
                    .attr("x", (width / 2))
                    .attr("y", 0 - ((margin.top-30) / 2))
                    .attr("text-anchor", "middle")
                    .style("font-size", "13px")
                    .style("text-decoration", "underline")
                    .text(title);
        }
    }

    function createAccordion(accordionObj, appendTo) {
        var title = accordionObj['title'];
        var defaultCollapsed = accordionObj['defaultCollapsed'];

        var tempDivOuter = $('<div><h3>' + title + '</h3></div>');
        tempDivOuter.uniqueId();
        var generatedIDOuter = tempDivOuter.attr('id');
        var tempDivInner = $('<div></div>');
        tempDivInner.uniqueId();
        var generatedIDInner = tempDivInner.attr('id');
        tempDivOuter.append(tempDivInner);
        appendTo.append(tempDivOuter);

        if (defaultCollapsed == true) {
            $("#" + generatedIDOuter).accordion({collapsible: true, heightStyle: "content", active: false});
        } else {
            $("#" + generatedIDOuter).accordion({collapsible: true, heightStyle: "content"});
        }

        //Add the inner components:
        var innerComponents = accordionObj['innerComponents'];
        var len = (!innerComponents ? 0 : innerComponents.length);
        for( var i=0; i<len; i++ ){
            var component = innerComponents[i];
            createAndAddComponent(component,$("#"+generatedIDInner));
        }
    }
</script>



<table style="width: 100%; padding: 5px;" class="hd">
    <tbody>
    <tr>
        <td style="width:143px; height:35px; padding-left:15px; padding-right:15px; padding-top:4px; padding-bottom:4px">
            <a href="/"><img src="/assets/skymind_w.png"  border="0"/></a></td>
        <td>  Network Intrusion Detection Demo</td>
        <td style="width: 256px;" class="hd-small">&nbsp;Updated at: <b><span id="updatetime">-</span></b>&nbsp;</td>
    </tr>
    </tbody>
</table>

<div style="width:1400px; margin:0 auto;" id="outerdiv">
    <div style="width:100%; padding-top:20px">
        <div style="width:33.333%; float:left;" class="subsectiontop" id="connectionChartOuter">
            <div style="width:100%;" class="sectionheader">
                Connections/sec
            </div>
            <div style="width:100%; height:100%; float:left;" id="connectionChartDiv">
            </div>
        </div>
        <div style="width:33.333%; float:left;" class="subsectiontop" id="bytesChartOuter">
            <div style="width:100%;" class="sectionheader">
                Bytes/sec
            </div>
            <div style="width:100%; height:100%; float:left;" id="bytesChartDiv">
            </div>
        </div>
        <div style="width:33.333%; float:right;"  class="subsectiontop">
            <div style="width:100%;" class="sectionheader">
                Chart 3
            </div>

        </div>

        <div style="width:50%; float:left;" class="subsectionbottom">
            <div style="width:100%;" class="sectionheader">
                Summary: Network Attacks
            </div>
        </div>
        <div style="width:50%; float:right;" class="subsectionbottom">
            <div style="width:100%;" class="sectionheader">
                Connection/Attack Information
            </div>
        </div>
    </div>
</div>

</body>
</html>