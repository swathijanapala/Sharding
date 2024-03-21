from flask import Flask
from flask import jsonify, request,redirect,url_for,make_response
import os 
import ast
import requests
import subprocess
import LoadBalancer as lb
import Helper as hp
import uuid
import random
import threading
import mysql.connector
import threading
import time



db_config = {
    
    'host': 'lbserver1',
    'user': 'root',
    'password': 'user12',
    'database': 'STUDENT',
    'port' : 3306
}


app = Flask(__name__)
app.config.from_object('config.Config')
obj = lb.ConsistentHashing()
list_of_servers = []

shard_locks = {}

def intialize_locks():
    connection = mysql.connector.connect(**db_config) 
    shard_ids=hp.get_shard_ids(connection)  #Getting all current shard_ids
    for shard in shard_ids:
        shard_locks[shard] = threading.Lock()
    connection.close()

def acquire_lock(shard_id):
    if shard_id in shard_locks:
        shard_locks[shard_id].acquire()

def release_lock(shard_id):
    if shard_id in shard_locks:
        shard_locks[shard_id].release()

def config_shards(servers):
    global schema
    print(schema)
    config_responses={}
    print(servers,flush=True)
    config_responses = {}
    for server, server_shards in servers.items():
        config_payload = {
            "schema": schema,
            "shards": server_shards
        }
        time.sleep(50)
        print('configging',flush=True)
        config_response = requests.post(f"http://{server}:5000/config/{server}", json=config_payload).json()
        print('over_configging',flush=True)
        #if(config_response.status_code==500):
        #break
        config_responses[server] = config_response
    return jsonify({"message": "Configured Database", "status": "success", "config_responses": config_responses}), 200

@app.route("/init", methods=["POST"])
def init():
    try:
        req_payload = request.json

        if 'N' in req_payload and 'schema' in req_payload and 'shards' in req_payload and 'servers' in req_payload:
            global config,schema
            config=req_payload
            N = req_payload.get('N')
            schema = req_payload.get('schema', {})
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            shards = req_payload.get('shards', [])
            servers = req_payload.get('servers', {})

            connection = mysql.connector.connect(**db_config)
            
            initialize_result = hp.initialize_tables(connection)
            if 'error' in initialize_result:
                return jsonify({"error": f"An error occurred during initialization: {initialize_result['error']}"}), 500

            
            shard_insert_result = hp.insert_shard_info(connection,shards)
            if 'error' in shard_insert_result:
                return jsonify({"error": f"An error occurred during shard info insertion: {shard_insert_result['error']}"}), 500

            mapping_insert_result = hp.insert_server_shard_mapping(connection,servers)
            if 'error' in mapping_insert_result:
                return jsonify({"error": f"An error occurred during server-shard mapping insertion: {mapping_insert_result['error']}"}), 500

            connection.close()
            intialize_locks()
            return config_shards(servers)

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/status', methods=['GET'])
def get_status():
    global config
    
    if config is None:
        return jsonify({"error": "Database configuration not set"}), 500

    return jsonify(config), 200
        
#add servers based on the request

def copy_shard_data_to_given_server(connection,server_id,shard_id,write_server):
    try:
        config_payload = {
            "shards": [shard_id]
        }
        config_response = requests.get(f"http://{server_id}:5000/copy", json=config_payload).json()
        
        data_entries = config_response.get(f'{shard_id}', [])
        print("LB copy_shard_data_to_given_server",data_entries,flush=True)
        valid_idx=hp.get_valididx_given_shardid(connection,shard_id)
        print(valid_idx,flush=True)
        acquire_lock(shard_id)
        config_payload2 = {
            "shard": shard_id,
            "curr_idx" : valid_idx,
            "data": data_entries
        }
        config_response2 = requests.post(f"http://{write_server}:5000/write", json=config_payload2).json()
        release_lock(shard_id)
        if config_response2.get("status") == "success":
            return True, "Data copied successfully"
        else:
            return False, "Failed to copy data to the given server"

    except Exception as e:
        return False, f"An error occurred while copying data to the given server: {str(e)}"
    

@app.route('/add', methods=['POST'])
def add_servers():
    req_payload = request.json

    if 'n' in req_payload and 'new_shards' in req_payload and 'servers' in req_payload:

        connection = mysql.connector.connect(**db_config)
        n = req_payload.get('n')
        new_shards = req_payload.get('new_shards', [])
        servers = req_payload.get('servers', {})

        if n > len(servers):
            return jsonify({"message": "Number of new servers (n) is greater than newly added instances", "status": "failure"}), 400
        
        k=n-len(servers)
        new_server_ids=list(servers.keys())
        print(new_server_ids,flush=True)

        for i in new_server_ids:
            try:
                result = subprocess.run(["python3","Helper.py",str(i),"sharding_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
                '''
                if(obj.dic.get(i)==None):
                    obj.N+=1
                    obj.dic[i] = obj.N
                    obj.add_server(obj.dic[i])
                '''
                # add the server to the list_of_servers
                list_of_servers.append(str(i))
            except Exception as e:
                msg = {
                    "message":"<Error> Unable to create some container(s),"+str(e),
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)
        try:
            print('entered',flush=True)
            try:
                config_shards(servers)
            except:
                msg = {
                "message":"<Error> Unable to create shards in new servers(s)",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)
            print('finished',flush=True)

            cur_shards=hp.get_shard_ids(connection)
            for ser,shards in servers.items():
                for i in shards:
                    if i in cur_shards:
                        #servers_list=hp.servers_given_shard(i,connection)
                        #server_id=lb.get_servers_list(servers_list)
                        server_id='server1'
                        print(ser,flush=True)
                        copy_shard_data_to_given_server(connection,server_id,i,ser)

            shard_insert_result = hp.insert_shard_info(connection,new_shards)
            if 'error' in shard_insert_result:
                return jsonify({"error": f"An error occurred during shard info insertion: {shard_insert_result['error']}"}), 500

            mapping_insert_result = hp.insert_server_shard_mapping(connection,servers)
            if 'error' in mapping_insert_result:
                return jsonify({"error": f"An error occurred during server-shard mapping insertion: {mapping_insert_result['error']}"}), 500

            connection.close()   ####look at once
            intialize_locks()
            
            return jsonify({
                    "N": n,
                    "message": f"Add Server:{', '.join(new_server_ids)}",
                    "status": "success"
                }), 200

        except:
            msg = {
                "message":"<Error> Unable to create database(s)",
                    "status" : "Faliure"
            }
            return make_response(jsonify(msg),400)
        

    return jsonify({"error": "Invalid payload structure"}), 400

#return redirect(url_for("rep"))

@app.route("/rm", methods=["DELETE"])
def remove_servers():
    try:
        req_payload = request.json

        if 'n' in req_payload and 'servers' in req_payload:
            n = req_payload['n']
            servers_to_remove = req_payload['servers']

            connection = mysql.connector.connect(**db_config)
            result = subprocess.run(["python3","Helper.py",],stdout=subprocess.PIPE, text=True, check=True)
            current_servers = result.stdout.splitlines()

            # Perform sanity checks
            if len(servers_to_remove) > n:
                return jsonify({"message": "<Error> Length of server list is more than removable instances", "status": "failure"}), 400

            elif(len(servers_to_remove)<n):
                k=n-len(servers_to_remove)
                remaining_servers = list(set(current_servers) - set(servers_to_remove))
                servers_to_remove.extend(random.sample(remaining_servers,k))

    
            rem_servers=len(current_servers)-n
            if(rem_servers<6):
                msg = {
                    "message":"<Error>  Cannot remove servers as the available server count after this operation will be less than 6.",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)

            for i in servers_to_remove:
                try:
                    result = subprocess.run(["python3","Helper.py",str(i),"remove"],stdout=subprocess.PIPE, text=True, check=True)
                    #implement hashing
                    # obj.remove_server(obj.dic[i])
                    list_of_servers.remove(i)
                except:
                    msg = {
                        "message":"<Error>  Unable to remove some container(s)",
                        "status" : "Faliure"
                    }
                    return make_response(jsonify(msg),400)

            hp.update_mapT(connection,servers_to_remove)
            connection.close()

            return jsonify({"message": {"N": rem_servers, "servers":servers_to_remove}, "status": "successful"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500



@app.route("/read", methods=["POST"])
def reading_data():
    try:
        req_payload = request.json

        if 'Stud_id' in req_payload:
            stud_id_range = req_payload['Stud_id']
            low = stud_id_range['low']
            high = stud_id_range['high']

            connection = mysql.connector.connect(**db_config)            
            shards_queried = hp.get_queried_shards_with_ranges(connection,low, high)
            data=[]
            keys=[]
            for item in shards_queried:
                shardid=item["Shard_id"]
                keys.append(shardid)
                servers_shard=hp.servers_given_shard(shardid,connection)
                #print(servers_shard,flush=True)
                #mapping=get(servers_shard)
                mapping_serverid=req_payload['server_id']  ###### consistent hashing
                config_payload = {
                    "shard": shardid,
                    "Stud_id" : item["Ranges"]  
                }
                config_response = requests.post(f"http://{mapping_serverid}:5000/read", json=config_payload).json()
                data.extend(config_response['data'])
            connection.close()

            return jsonify({"shards_queried": keys, "data": data, "status": "success"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


def write_to_shard(server, config_payload, shard_id):
    try:
        acquire_lock(shard_id)
        config_response = requests.post(f"http://{server}:5000/write", json=config_payload).json()
    finally:
        release_lock(shard_id)

@app.route('/write', methods=['POST'])
def write_data_load_balancer():
    try:
        request_payload = request.json
        data_entries = request_payload.get('data')
        if data_entries and isinstance(data_entries, list):
            connection = mysql.connector.connect(**db_config) 
            ind_shard_data = hp.get_shard_ids_corresponding_write_operations(connection, data_entries)
            threads = []

            for shard_id, shard_data in ind_shard_data.items():
                server = 'server1'  # Get server using consistent hashing
                config_payload = {
                    "shard": shard_id,
                    "curr_idx": shard_data['valid_idx'],
                    "data": shard_data['entries']
                }
                thread = threading.Thread(target=write_to_shard, args=(server, config_payload, shard_id))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()  # Wait for all threads to complete

            connection.close()
            return jsonify({"message": f"{len(data_entries)} Data entries added", "status": "success"}), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


#############
@app.route('/update', methods=['PUT'])
def update_student_info():

    try:
        req_payload = request.json
        if 'Stud_id' in req_payload and 'data' in req_payload:

            stud_id = req_payload['Stud_id']
            data = req_payload['data']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = hp.servers_given_shard(shard_id, connection)
            for server_id in servers_list:
                config_payload = {
                    "shard": shard_id,
                    "Stud_id" : stud_id,
                    "data" : data
                }
                acquire_lock(shard_id)
                config_response = requests.put(f"http://{server_id}:5000/update", json=config_payload).json()
                release_lock(shard_id)

            return jsonify({"message": f"Data entry for Stud_id: {stud_id} updated", 
                            "status" : "success"}
                            ), 200

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()


@app.route('/del', methods=['DELETE'])
def remove_student_info():

    try:
        req_payload = request.json
        if 'Stud_id' in req_payload:
            
            stud_id = req_payload['Stud_id']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = hp.servers_given_shard(shard_id, connection)
            for server_id in servers_list:
                
                config_payload = {
                    "shard": shard_id,
                    "Stud_id" : stud_id,
                }

                acquire_lock(shard_id)
                config_response = requests.delete(f"http://{server_id}:5000/del", json=config_payload).json()
                release_lock(shard_id)

            return jsonify({"message": f"Data entry with Stud_id:{stud_id} removed", 
                            "status" : "success"}
                            ), 200

    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()

##########################


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
            result = subprocess.run(["python","Helper.py",server_name,"sharding_net1","flaskserver1","add"],stdout=subprocess.PIPE, text=True, check=True)
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
            result = subprocess.run(["python3","Helper.py",server_name,"sharding_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
            obj.add_server(obj.dic[server_name])
            

        temp-=1
    response = requests.get(f'http://{server_name}:5000/home/{server_name}')
    return make_response(jsonify(response.json()),200)


# continuously check heartbeat
# Define the heartbeat function
def heartbeat(list_of_servers):
    connection = mysql.connector.connect(**db_config)
    '''
    while True:
        list_of_servers = list(set(list_of_servers))   # remove duplicates
        for server in list_of_servers:
            try:
                response = requests.get(f'http://{server}:5000/heartbeat')
                if response.status_code == 200:
                    print(f"Server {server} is up and running.",flush=True)
                else:
                    shard_ids = hp.get_shardid_given_server(connection,server)

                    print(f"Server {server} is down. Status code: {response.status_code}",flush=True)
                    add_response = requests.post('http://127.0.0.1:5000/add', json={'n': '1','new_shards':[],'servers':{f'{server}':[f'{shard_ids[0]}',f'{shard_ids[1]}']}})
                    if add_response.status_code == 200:
                        print("New server added successfully.")
                    else:
                        print("Failed to add a new server.")
            except requests.ConnectionError:
                print(f"Failed to connect to server {server}.",flush=True)
                add_response = requests.post('http://127.0.0.1:5000/add', json={'n': '1','new_shards':[],'servers':{f'{server}':[f'{shard_ids[0]}',f'{shard_ids[1]}']}})
                if add_response.status_code == 200:
                    print("New server added successfully.")
                else:
                    print("Failed to add a new server.")
                
        time.sleep(5)  # Wait for 5 seconds before checking again
    '''




@app.errorhandler(404)

def errorPage(k):
    return "Page not found"



if __name__ == "__main__":
    # 6 replicas of server are maintained
    for i in ["server0","server1"]:#,"server2","server3","server4","server5"]:
        try:
            result = subprocess.run(["python3","Helper.py",str(i),"sharding_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
        except Exception as e:
            # pass
            print("error",e)
        '''
        if(obj.dic.get(i)==None):
            obj.N+=1
            obj.dic[i] = obj.N
        obj.add_server(obj.dic[i])
        list_of_servers.append(i)
        #implement hashing
        '''

    # Create a thread to run the heartbeat function
    # heartbeat_thread = threading.Thread(target=heartbeat, args=(list_of_servers,))
    # heartbeat_thread.start()
    app.run(host = "0.0.0.0",debug = True)
