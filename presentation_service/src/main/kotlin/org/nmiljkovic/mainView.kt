package org.nmiljkovic

import org.json.JSONObject

fun output(data: JSONObject): String {
    return """<html>
        <head>
            <script src="https://code.jquery.com/jquery-3.2.1.min.js"
              integrity="sha256-hwg4gsxgFZhOsEEamdOYGBf13FyQuiTwlAQgxVSNgt4="
              crossorigin="anonymous">
            </script>

            <script src="http://sigmajs.org/assets/js/sigma.min.js"></script>
            <script src="http://rawgit.com/jacomyal/sigma.js/master/plugins/sigma.parsers.json/sigma.parsers.json.js"></script>
            <style>
                #network-graph {
                  top: 150px;
                  bottom: 150px;
                  left: 150px;
                  right: 150px;
                  position: absolute;
                }
            </style>
        </head>
        <body>
            <div id="network-graph"></div>
            <script>
                var graphData = $data;

                var settings = new sigma.classes.configurable({
                  maxNodeSize: 20,
                  minNodeSize: 10,
                  defaultLabelSize: 18
                });

                var s = new sigma({
                  graph: graphData,
                  container: 'network-graph',
                  settings: {
                    defaultNodeColor: '#ec5148'
                  }
                });

                s.bind("clickNode", function (evt) {
                    var lang = evt.data.node.id;
                    $.get("http://localhost:8003/path/" + encodeURIComponent(lang), function(data) {
                        graphData["edges"] = JSON.parse(data);
                        s.graph.clear();
                        s.graph.read(graphData);
                        s.refresh();
                    });
                });
                s.refresh();
            </script>
        </body>
    </html>
    """
}