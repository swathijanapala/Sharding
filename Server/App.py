from flask import Flask
from flask import jsonify, request,make_response
import os
app = Flask(__name__)


# @app.route("/home",methods = ["GET"])

# def hello_world():
    
#     server_id = os.environ.get('server_id')
#     no_of_servers = os.environ["no_of_servers"]
#     msg = {
#         "message": f"Hello, From Server{server_id}",
#         "no_of_servers" : no_of_servers,
#         "status" : "Successful"
#     }
#     return make_response(jsonify(msg),200)

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