from flask import Flask
from flask import jsonify, request,redirect,url_for,make_response
import os 
import ast
import requests
import subprocess
import LoadBalancer as lb
import uuid
import random

app = Flask(__name__)
app.config.from_object('config.Config')
obj = lb.ConsistentHashing()


# resposes all the replicas of the servers


@app.route("/rep",methods = ["GET"])
def rep():
    try:
            result = subprocess.run(["python","Helper.py",],stdout=subprocess.PIPE, text=True, check=True)
            replicas = result.stdout.splitlines()
            
    except:
        msg = {
        "message":"<Error>  Unable to process your request",
        "status" : "Faliure"
        }
        return jsonify(msg),400
    
    app.config['REPLICAS'] = replicas
    app.config["N"] = len(replicas)
    msg = {
        "message":
        {
            
            "N" : app.config["N"],
            "replicas" :  app.config["REPLICAS"]
        },
        "status" : "Successful"
    }
    return make_response(jsonify(msg),200)

#add servers based on the request

@app.route("/add",methods = ["POST"])
def add():
    try:
        servers = ast.literal_eval(request.args['replicas'])
        n = int(request.args['n'])
    except:
        msg = {
                "message":"<Error> Unable to create some container(s)",
                "status" : "Faliure"
                }
        return make_response(jsonify(msg),400)
    
    
    if(n<len(servers)):
        msg = {
        "message":"<Error> Length of hostname list is more than newly added instances",
        "status" : "Faliure"
        }
        return make_response(jsonify(msg),400)
    # when n is greater than servers instances given then random server containers are created
    elif(n>len(servers)):
        k = n-len(servers)
        for i in range(k):
            servers.append("Sa1wr2"+str(obj.N+1+i))

    for i in servers:
        try:
            result = subprocess.run(["python","Helper.py",str(i),"distributedsystems_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
            if(obj.dic.get(i)==None):
                obj.N+=1
                obj.dic[i] = obj.N
            obj.add_server(obj.dic[i])
            #implement hashing
        except:
            msg = {
                "message":"<Error> Unable to create some container(s)",
                "status" : "Faliure"
                }
            return make_response(jsonify(msg),400)
            
    #add here
    return redirect(url_for("rep"))

 
# removes servers specified in the request
@app.route("/rem",methods = ["POST"])

def rem():
    try:
        servers = ast.literal_eval(request.args['replicas'])
        n = int(request.args['n'])
    except:
        msg = {
            "message":"<Error>  Unable to remove some container(s)",
            "status" : "Faliure"
            }
        return make_response(jsonify(msg),400) 
    
    if(n<len(servers)):
        msg = {
        "message":"<Error>  Length of hostname list is more than removable instances",
        "status" : "Faliure"
        }
        return make_response(jsonify(msg),400)
    elif(n>=len(servers)):
        result = subprocess.run(["python","Helper.py",],stdout=subprocess.PIPE, text=True, check=True)
        replicas = result.stdout.splitlines()
        #print(len(replicas),n, flush=True)
        if(len(replicas)-n<3):
            msg = {
            "message":"<Error>  Cannot remove servers as the available server count after this operation will be less than 3.",
            "status" : "Faliure"
            }
            return make_response(jsonify(msg),400)
        k = n-len(servers)
        if(k>0):
            try:
                for i in servers:
                    replicas.remove(i)
            except:
                pass
            
            servers = servers+random.sample(replicas, k)

    for i in servers:
        try:
            result = subprocess.run(["python","Helper.py",str(i),"remove"],stdout=subprocess.PIPE, text=True, check=True)
            #implement hashing
            obj.remove_server(obj.dic[i])
            
        except:
            msg = {
            "message":"<Error>  Unable to remove some container(s)",
            "status" : "Faliure"
            }
            return make_response(jsonify(msg),400)
    return redirect(url_for("rep"))

# routes requests to one of the avaliable servers

@app.route("/<path>",methods = ["GET"])
def pathRoute1(path):
    if(path!="home"):
        msg = {
            "message":"<Error> Page Not Found",
            "status" : "Faliure"
            }
        return make_response(jsonify(msg),400)
    #assigning uuid to each request
    max_value = 10**(6) - 1
    request_id = uuid.uuid4().int % max_value
    temp = 512
    while(temp>=0):
        server_id = obj.req_server(request_id)
        # if all the servers goes down then 3 new servers will spawn
        if (server_id == None):
            obj.N+=1
            server_name = "Sa1wasd"+str(obj.N)
            obj.dic[server_name] = obj.N
            result = subprocess.run(["python","Helper.py",server_name,"distributedsystems_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
            obj.add_server(obj.dic[server_name])
            continue
        
        for i,j in obj.dic.items():
            if(j==server_id):
                server_name = i
                break
        
        #checkheartbeat
        
        try:
            res = requests.get(f'http://{server_name}:5000/heartbeat')
            if(res.status_code==200):
                break 
        except Exception as errh:
           
            result = subprocess.run(["python","Helper.py",server_name,"remove"],stdout=subprocess.PIPE, text=True, check=True)
            obj.remove_server(obj.dic[server_name])
            obj.N+=1
            server_name = "Sa1wasd"+str(obj.N)
            obj.dic[server_name] = obj.N
            result = subprocess.run(["python","Helper.py",server_name,"distributedsystems_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
            obj.add_server(obj.dic[server_name])
            

        temp-=1
    response = requests.get(f'http://{server_name}:5000/home/{server_name}')
    return make_response(jsonify(response.json()),200)


@app.errorhandler(404)

def errorPage(k):
    return "Page not found"

if __name__ == "__main__":
    # 3 replicas of server are maintained
    for i in ["server1","server2","server3"]:
        try:
            result = subprocess.run(["python","Helper.py",str(i),"distributedsystems_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
        except Exception as e:
            pass
        if(obj.dic.get(i)==None):
            obj.N+=1
            obj.dic[i] = obj.N
        obj.add_server(obj.dic[i])
            #implement hashing
        
    
    app.run(host = "0.0.0.0",debug = True)
