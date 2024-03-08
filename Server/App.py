from flask import Flask
from flask import jsonify, request,make_response
import os
from flask_sqlalchemy import SQLAlchemy
import mysql.connector
app = Flask(__name__)


app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:user12@localhost/STUDENT'
db = SQLAlchemy(app)



#this endpoint will configure the table schema of a data
@app.route("/config",methods = ["POST"])

def config():
    try:
        # Extract schema and shards information from the payload
        payload = request.json
        schema = payload.get("schema")
        shards = payload.get("shards")
        columns = schema["columns"]
        print(schema, flush=True)
        print(shards, flush=True)
        print(columns, flush=True)
        db
        # Dummy response indicating successful initialization
        response = {"message": "Shards initialized successfully", "shards": shards}

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route("/heartbeat",methods = ["GET"])

def heartbeat():
    msg = {
        "message": "",
        "status" : "Successful"
    }
    return make_response(jsonify(msg),200)


@app.route("/home/<server_id>",methods = ["GET"])
def home(server_id):
    msg = {
        "message": f"Hello, From Server {server_id}",
        "status" : "Successful"
    }
    return make_response(jsonify(msg),200)
@app.errorhandler(404)

def errorPage(k):
    msg = {
            "message":"<Error> No servers present..(s)",
            "status" : "Faliure"
            }
    return make_response(jsonify(msg),404)

if __name__ == "__main__":
    
    app.run(host = "0.0.0.0",port=5000 , debug = True)
