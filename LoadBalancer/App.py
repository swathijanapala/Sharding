from flask import Flask
from flask import jsonify, request,redirect,url_for,make_response
import os 
import traceback
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
from queue import Queue

chash = lb.ShardHandle()

db_config = {
    
    'host': 'lbserver1',
    'user': 'root',
    'password': 'user12',
    'database': 'STUDENT',
    'port' : 3306
}


app = Flask(__name__)
app.config.from_object('config.Config')

list_of_servers = []

shard_locks = {}


class ReaderWriterLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.readers_count = 0
        self.write_in_progress = False
        self.read_condition = threading.Condition(lock=self.lock)
        self.write_condition = threading.Condition(lock=self.lock)

    def acquire_read(self):
        with self.lock:
            while self.write_in_progress:
                self.read_condition.wait()
            self.readers_count += 1

    def release_read(self):
        with self.lock:
            self.readers_count -= 1
            if self.readers_count == 0:
                self.write_condition.notify()

    def acquire_write(self):
        with self.lock:
            while self.readers_count > 0 or self.write_in_progress:
                self.write_condition.wait()
            self.write_in_progress = True

    def release_write(self):
        with self.lock:
            self.write_in_progress = False
            self.read_condition.notify_all()
            self.write_condition.notify()


def initialize_locks(shard_ids):
    print(f"initializing lock vars for {shard_ids}",flush=True)
    for shard_id in shard_ids:
        shard_locks[shard_id] = ReaderWriterLock()
 

def config_shards(servers,method = ""):
    global schema
    #print("schema in config",schema,flush=True)
    config_responses={}
    config_responses = {}
    for server, server_shards in servers.items():
        config_payload = {
            "schema": schema,
            "shards": server_shards
        }
        if(method=="add"):
            time.sleep(50)
        config_response = requests.post(f"http://{server}:5000/config/{server}", json=config_payload).json()
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

            shards_list_for_intializing_locks=[]
            for shard in shards:
                sid = shard['Shard_id']
                shards_list_for_intializing_locks.append(sid)
                servers_list = hp.servers_given_shard(sid, connection)
                chash.add_shard(sid, servers_list)
            
            connection.close()
            initialize_locks(shards_list_for_intializing_locks)
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
        config_response = requests.get(f"http://{server_id}:5000/copy/{server_id}", json=config_payload).json()
        
        data_entries = config_response.get(f'{shard_id}', [])
        valid_idx=hp.get_valididx_given_shardid(connection,shard_id)
        #print(valid_idx,flush=True)
        shard_locks[shard_id].lock.acquire_write()
        config_payload2 = {
            "shard": shard_id,
            "curr_idx" : valid_idx,
            "data": data_entries["data"]
        }
        #print(f"copying data from {server_id} to {write_server}'s {shard_id}",flush=True)
        #print(f"data being copied is {config_payload2}",flush=True)
        shard_locks[shard_id].lock.acquire_write()
        config_response2 = requests.post(f"http://{write_server}:5000/write/{write_server}", json=config_payload2).json()
        
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
        #print(new_server_ids,flush=True)

        new_shard_ids_for_intializing_locks=[]
        for dic in new_shards:
            new_shard_ids_for_intializing_locks.append(dic["Shard_id"])
        
        initialize_locks(new_shard_ids_for_intializing_locks)

        for i in new_server_ids:
            try:
                result = subprocess.run(["python3","Helper.py",str(i),"sharding_net1","mysqlserver","add"],stdout=subprocess.PIPE, text=True, check=True)
                
                # add the server to the list_of_servers
                list_of_servers.append(str(i))
            except Exception as e:
                msg = {
                    "message":"<Error> Unable to create some container(s),"+str(e),
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)
        
        try:
            try:
                config_shards(servers,"add")
            except:
                msg = {
                "message":"<Error> Unable to create shards in new servers(s)",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)

            cur_shards=hp.get_shard_ids(connection)
            #print(f"getting shardids {cur_shards}",flush=True)
            for ser,shards in servers.items():
                for i in shards:
                    if i in cur_shards:
                        server_id=chash.get_server(i)
                        print("scheduled server id in add endpoint",server_id,flush=True)
                        #print(ser,flush=True)
                        copy_shard_data_to_given_server(connection,server_id,i,ser)

            if(len(new_shards)!=0):
                shard_insert_result = hp.insert_shard_info(connection,new_shards)
                if 'error' in shard_insert_result:
                    return jsonify({"error": f"An error occurred during shard info insertion: {shard_insert_result['error']}"}), 500

            mapping_insert_result = hp.insert_server_shard_mapping(connection,servers)
            if 'error' in mapping_insert_result:
                return jsonify({"error": f"An error occurred during server-shard mapping insertion: {mapping_insert_result['error']}"}), 500

            tempdict = {}
            for server in servers:
                shard_list = servers[server]
                for shard in shard_list:
                    if shard not in tempdict:
                        tempdict[shard] = [server]
                    else:
                        tempdict[shard].append(server)

            for shard_id in tempdict:
                chash.add_shard(shard_id, tempdict[shard_id])
            
            connection.close()
            
            return jsonify({
                    "N": n,
                    "message": f"Add Server:{', '.join(new_server_ids)}",
                    "status": "success"
                }), 200

        except:
            msg = {
                "message":"<Error> Unable to create database(s)1",
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
            if(rem_servers<2):  #here <6
                msg = {
                    "message":"<Error>  Cannot remove servers as the available server count after this operation will be less than 6.",
                    "status" : "Faliure"
                }
                return make_response(jsonify(msg),400)

            print("servers to remove",servers_to_remove,flush=True)
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

            hp.update_shardt_mapt_tables(connection,servers_to_remove)
            connection.close()

            return jsonify({"message": {"N": len(servers_to_remove), "servers":servers_to_remove}, "status": "successful"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

def read_from_shard(connection,shard_id,mapping_serverid,range_data):
    if shard_id not in shard_locks:
        shard_locks[shard_id] = ReaderWriterLock()
    shard_locks[shard_id].acquire_read()
    try:
        print(f"lock_acquired by {shard_id} on read request",flush=True)
        config_payload = {
            "shard": shard_id,
            "Stud_id" : range_data  
        }
        config_response = requests.post(f"http://{mapping_serverid}:5000/read/{mapping_serverid}", json=config_payload).json()
        data=config_response['data']

        print("",config_response)
        return data
    finally:
        shard_locks[shard_id].release_read()


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
            #multiple reads should be allowed
            for item in shards_queried:
                shardid=item["Shard_id"]
                keys.append(shardid)
                servers_shard=hp.servers_given_shard(shardid,connection)
                mapping_ser=chash.get_server(shardid)
                print("read endpoint load balancer,mapped server id ",mapping_ser,flush=True)
                # mapping_serverid=req_payload['server_id']  ###### consistent hashing
                
                data.extend(read_from_shard(connection,shardid,mapping_ser,item["Ranges"]))

            connection.close()

            return jsonify({"shards_queried": keys, "data": data, "status": "success"}), 200

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


def servers_given_shard(shard,connection):
    connection = mysql.connector.connect(**db_config) 
    try:
        cursor = connection.cursor()
        select_servers_query = '''
            SELECT Server_id FROM MapT WHERE Shard_id = %s;
        '''
        cursor.execute(select_servers_query, (shard,))
        server_ids = [row[0] for row in cursor.fetchall()]
        connection.commit()
        cursor.close()
        return server_ids
    except Exception as e:
        raise Exception(f"An error occurred while retrieving Server IDs for Shard {shard}: {str(e)}")


def write_to_shard(connection, shard_id, shard_data):
    
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()
        shard_locks[shard_id].acquire_write()
        s=[]
        servers_list = servers_given_shard(shard_id, connection)
        print(f" printing servers list in write_to_shard {servers_list} in {shard_id}",flush=True)
        for server in servers_list:
            s.append(server)
            config_payload = {
                "shard": shard_id,
                "curr_idx": shard_data['valid_idx'],
                "data": shard_data['entries']
            }
            config_response = requests.post(f"http://{server}:5000/write/{server}", json=config_payload).json()

    except Exception as e:
        print(f"An error occurred while writing to shard {shard_id} on server {server}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()
        print("write done on all shards and releasing the locks",s,flush=True)

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
                thread = threading.Thread(target=write_to_shard, args=(connection,shard_id,shard_data))
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

result_queue_for_update = Queue()

def update_to_shard(config_payload,shard_id,server_id):
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()

        shard_locks[shard_id].acquire_write()
        config_response = requests.put(f"http://{server_id}:5000/update/{server_id}", json=config_payload).json()
        result_queue_for_update.put(config_response)
        
    except Exception as e:
        print(f"An error occurred while updating to shard {shard_id} on server {server_id}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()

@app.route('/update', methods=['PUT'])
def update_student_info():

    try:
        req_payload = request.json
        if 'Stud_id' in req_payload and 'data' in req_payload:

            stud_id = req_payload['Stud_id']
            data = req_payload['data']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = servers_given_shard(shard_id, connection)
            threads = []
            for server_id in servers_list:
                config_payload = {
                    "shard": shard_id,
                    "Stud_id" : stud_id,
                    "data" : data
                }

                thread = threading.Thread(target=update_to_shard, args=(config_payload,shard_id,server_id))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            res=result_queue_for_update.get()
            if(res["status"]=="not_found"):
                return res
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

result_queue_for_delete = Queue()

def delete_from_shard(config_payload,shard_id,server_id):
    try:
        if shard_id not in shard_locks:
            shard_locks[shard_id] = ReaderWriterLock()

        shard_locks[shard_id].acquire_write()
        config_response = requests.delete(f"http://{server_id}:5000/del/{server_id}", json=config_payload).json()
        result_queue_for_delete.put(config_response)

    except Exception as e:
        print(f"An error occurred while updating to shard {shard_id} on server {server_id}: {str(e)}")
        traceback.print_exc()
    finally:
        shard_locks[shard_id].release_write()

@app.route('/del', methods=['DELETE'])
def remove_student_info():

    try:
        req_payload = request.json
        if 'Stud_id' in req_payload:
            
            stud_id = req_payload['Stud_id']
            connection = mysql.connector.connect(**db_config)
            shard_id = hp.get_shard_id_by_stud_id(connection, stud_id)
            servers_list = hp.servers_given_shard(shard_id, connection)
            threads = []
            for server_id in servers_list:
                config_payload = {
                    "shard": shard_id,
                    "Stud_id" : stud_id,
                }

                thread = threading.Thread(target=delete_from_shard, args=(config_payload,shard_id,server_id))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
            
            res=result_queue_for_delete.get()
            if(res["status"]=="not_found"):
                return res
            return jsonify({"message": f"Data entry for Stud_id: {stud_id} deleted", 
                            "status" : "success"}
                            ), 200
        
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()



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

# returns list of shardid corresponding to a server
def get_shardid_given_server(connection,server):
    try:
        cursor = connection.cursor()
        query = f"SELECT Shard_Id from MapT where server_id = '{server}';"
        cursor.execute(query)
        result = cursor.fetchall()
        shard_ids = [row[0] for row in result]
        return shard_ids

    except Exception as e:
        print(f"An error occurred while fetching shard ids: {str(e)}")

    finally:
        connection.commit()
        cursor.close()


# continuously check heartbeat
# Define the heartbeat function
def heartbeat(list_of_servers):
    connection = mysql.connector.connect(**db_config)
    while True:
        list_of_servers = list(set(list_of_servers))   # remove duplicates
        print(list_of_servers)
        for server in list_of_servers:
            try:
                response = requests.get(f'http://{server}:5000/heartbeat')
                if response.status_code == 200:
                    print(f"Server {server} is up and running.",flush=True)
                else:
                    shard_ids = get_shardid_given_server(connection,server)
                    list_of_servers.remove(server)
                    print(f"Server {server} is down. Status code: {response.status_code}",flush=True)
                    add_response = requests.post('http://127.0.0.1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids}})
                    if add_response.status_code == 200:
                        print("New server added successfully.")
                    else:
                        print("Failed to add a new server.")
            except requests.ConnectionError:
                print(f"Failed to connect to server {server}.",flush=True)
                shard_ids = get_shardid_given_server(connection,server)
                list_of_servers.remove(server)
                add_response = requests.post('http://127.0.0.1:5000/add', json={'n': 1,'new_shards':[],'servers':{f'{server}':shard_ids}})
                if add_response.status_code == 200:
                    print("New server added successfully.")
                else:
                    print("Failed to add a new server.")
                
        time.sleep(5)  # Wait for 5 seconds before checking again




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
        list_of_servers.append(i)


    # Create a thread to run the heartbeat function
    time.sleep(60)
    heartbeat_thread = threading.Thread(target=heartbeat, args=(list_of_servers,))
    heartbeat_thread.start()
    app.run(host = "0.0.0.0",debug = True)
