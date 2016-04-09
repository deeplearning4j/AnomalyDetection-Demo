<!DOCTYPE html>
<html>
<head>
    <title>Fraud/Anomaly Detection UI</title>
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
<script src="/assets/dl4j-ui.js"></script>

<script>
    //Store last update times:
    var lastUpdateTime = -1;

    //Interval function to do updates:
    setInterval(function () {

        //Get the last update time

        //If necessary: get the new elements, and render them

        $.get("/ui/components", function(data){
            var componentArr = JSON.parse(JSON.stringify(data));

//            console.log(jsonObj);
//            console.log("testing");

            var mainDiv = $('#outerdiv');
            mainDiv.html('');
//            var componentArray = Component.getComponent(JSON.stringify(data));
            for(var i=0; i<componentArr.length; i++ ){
                var temp = componentArr[i];
                var c = Component.getComponent(JSON.stringify(temp));
                c.render(mainDiv);
//                r.render(componentArr[i])
            }

        });

    }, 1000);

</script>


<table style="width: 100%; padding: 5px;" class="hd">
    <tbody>
    <tr>
        <td style="width:143px; height:35px; padding-left:15px; padding-right:15px; padding-top:4px; padding-bottom:4px">
            <a href="/"><img src="/assets/skymind_w.png" border="0"/></a></td>
        <td> Fraud/Anomaly Detection Demo</td>
    </tr>
    </tbody>
</table>

<div style="width:1400px; margin:0 auto;" id="outerdiv">

</div>

</body>
</html>