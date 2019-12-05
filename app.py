from flask import Flask, render_template, jsonify
# import pandas as pd
import csv
import json

# Declare application
app = Flask(__name__)


@app.route("/jsonData", methods=["GET", "POST"])
def returnProdData():
    DATA = {
        "nodes": [],
        "links": []
    }
    nodes = set()
    with open('static/data/result.csv', 'r') as csvfile:
        fieldnames = ("source", "target", "value")
        reader = csv.DictReader(csvfile)
        for line in reader:
            source = line["source"]
            target = line["target"]
            value = line["value"]
            nodes.add(source)
            nodes.add(target)
            DATA['links'].append(line)

    for node in nodes:
        DATA['nodes'].append({"id": node, "group": int(node) % 3})

    return jsonify(DATA)


@app.route("/", methods=["GET", "POST"])
def returnData():
    returnProdData()

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)