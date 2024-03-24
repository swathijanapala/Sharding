from flask import Flask
from flask import jsonify, request,make_response
import os

import mysql.connector
app = Flask(__name__)


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



# Replace these values with your MySQL database credentials
db_config = {
    
    'host': 'server0',
    'user': 'root',
    'password': 'user12',
    'database': 'STUDENT',
    'port' : 3306
}


def initialize_shard_tables(payload,server_id):
    db_config['host'] = server_id
    try:
        db_config['host'] = server_id
        #print("in server  ",db_config,flush=True)
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
    except Exception as e:
        return "error"+str(e)
    try:       
        # Extract schema and shards from the payload
        schema = payload.get('schema', {})
        columns = schema.get('columns', [])
        dtypes = schema.get('dtypes', [])
        shards = payload.get('shards', [])
        
        # Create shard tables in the database
        for shard in shards:
            table_name = f'StudT_{shard}'
            print(f"creating table {table_name}")
            create_table_query = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join([f'{column} INT' if dtype == 'Number' else f'{column} VARCHAR(100)' if dtype == 'String' else f'{column} {dtype}' for column, dtype in zip(columns, dtypes)])},
                    PRIMARY KEY (Stud_id)
                );
            '''
            # print(create_table_query,flush=True)
            cursor.execute(create_table_query)

        connection.commit()

        return {"message": f"{', '.join([f'{server_id}:{shard}' for shard in shards])} configured", "status": "success"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    
@app.route('/config/<server_id>', methods=['POST'])
def configure_shard_tables(server_id):

    try:
        request_payload = request.json

        # Validate the payload structure
        if 'schema' in request_payload and 'shards' in request_payload:
            response = initialize_shard_tables(request_payload,server_id)
            return jsonify(response)

        return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/users', methods=['GET'])
def get_data():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
    except Exception as e:
        print("error"+str(e))
    query = "select * from StudT_sh1;"
    cursor.execute(query)
    result = cursor.fetchall()
    students = []
    for row in result:
        student = {
            'Stud_id': row[0],
            'Stud_name': row[1],
            'Stud_marks': row[2],
        }
        students.append(student)

        # Return the result as JSON
        return jsonify(students),200
    
@app.route('/insert/<server_id>', methods=['POST'])
def insert(server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
    except Exception as e:
        print("error"+str(e))
    query = "insert into StudT_sh1 values(1000, 'karthik','24');"
    cursor.execute(query)
    
    connection.commit()

    return {"message": "Shard tables initialized successfully"}
    

# Function to copy data entries from replicas to a shard table
def copy_data_entries(shard,server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Replace 'StudT' with your actual table name
        table_name = f'StudT_{shard}'

        # Select all data entries from the shard table
        select_query = f'SELECT * FROM {table_name}'
        cursor.execute(select_query)
        data_entries = cursor.fetchall()
        # print("data entries of server",data_entries)
        if(data_entries):
        # Format the data entries into a list of dictionaries
            formatted_data = []
            for entry in data_entries:
                formatted_data.append({
                    "Stud_id": entry[0],
                    "Stud_name": entry[1],
                    "Stud_marks": entry[2],
                })

            return {"data": formatted_data, "status": "success"}
        else:
            return { "status": "success"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /copy GET requests
@app.route('/copy/<server_id>', methods=['GET'])
def copy_data(server_id):
    try:
        request_payload = request.json

        shards = request_payload.get('shards')

        if shards and isinstance(shards, list):
            response_data = {}
            for shard in shards:
                response_data[shard] = copy_data_entries(shard,server_id)

            return jsonify(response_data), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

def read_data_entries(shard, stud_id_range,server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Replace 'StudT' with your actual table name
        table_name = f'StudT_{shard}'

        # Read data entries within the specified Stud_id range
        read_query = f'''
            SELECT * FROM {table_name}
            WHERE stud_id BETWEEN {stud_id_range['low']} AND {stud_id_range['high']}
        '''
        cursor.execute(read_query)
        data_entries = cursor.fetchall()

        # Format the data entries into a list of dictionaries
        formatted_data = []
        for entry in data_entries:
            formatted_data.append({
                "Stud_id": entry[0],
                "Stud_name": entry[1],
                "Stud_marks": entry[2],
            })

        return {"data": formatted_data, "status": "success"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /read POST requests
@app.route('/read/<server_id>', methods=['POST'])
def read_data(server_id):
    try:
        request_payload = request.json
        shard = request_payload.get('shard')
        stud_id_range = request_payload.get('Stud_id')

        if shard and stud_id_range and isinstance(stud_id_range, dict):
            response = read_data_entries(shard, stud_id_range,server_id)
            
            return jsonify(response), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    
# Function to write data entries to a shard in a particular server container
def write_data_entries(shard, curr_idx, data,server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Replace 'StudT' with your actual table name
        table_name = f'StudT_{shard}'

        # Write data entries to the shard table
        for entry in data:
            insert_query = f'''
                INSERT INTO {table_name} (stud_id, stud_name, stud_marks)
                VALUES (%s, %s, %s)
            '''
            cursor.execute(insert_query, (entry['Stud_id'], entry['Stud_name'], entry['Stud_marks']))

        connection.commit()

        # Get the current index after writing the data entries
        # get_current_index_query = f'SELECT MAX(stud_id) FROM {table_name}'
        # cursor.execute(get_current_index_query)

        return {"message": "Data entries added", "current_idx": curr_idx+len(data), "status": "success"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /write POST requests
@app.route('/write/<server_id>', methods=['POST'])
def write_data(server_id):
    
    try:
        request_payload = request.json
        shard = request_payload.get('shard')
        curr_idx = request_payload.get('curr_idx')
        data = request_payload.get('data')

        if shard and curr_idx is not None and data and isinstance(data, list):
            response = write_data_entries(shard, curr_idx, data,server_id)
            return jsonify(response), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
# Function to update a data entry in a shard
def update_data_entry(payload,server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        shard = payload.get('shard')
        stud_id = payload.get('Stud_id')
        data = payload.get('data', {})

        table_name = f'StudT_{shard}'

         # Check if the entry exists in the database
        check_query = f'''
            SELECT * FROM {table_name}
            WHERE Stud_id = {stud_id};
        '''
        cursor.execute(check_query)
        existing_entry = cursor.fetchone()
        #print("existing_entry in update endpoint",existing_entry,flush=True)
        if existing_entry:
            update_query = f'''
                UPDATE {table_name}
                SET Stud_name = '{data.get("Stud_name")}', Stud_marks = {data.get("Stud_marks")}
                WHERE Stud_id = {stud_id};
            '''

            cursor.execute(update_query)
            connection.commit()

            return {"message": f"Data entry for Stud_id: {stud_id} updated", "status": "success"}
        else:
            return {"message": f"Data entry with Stud_id: {stud_id} not found", "status": "not_found"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /update PUT requests
@app.route('/update/<server_id>', methods=['PUT'])
def update_data_entry_endpoint(server_id):
    
    try:
        request_payload = request.json

        # Validate the payload structure
        if 'shard' in request_payload and 'Stud_id' in request_payload and 'data' in request_payload:
            success = update_data_entry(request_payload,server_id)
            if success["status"]=="success":
                return jsonify(success), 200
            else:
                return jsonify({"error": "Failed to update data entry","status":"not_found"}), 500
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# Function to delete a data entry in a shard
def delete_data_entry(payload,server_id):
    db_config['host'] = server_id
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        shard = payload.get('shard')
        stud_id = payload.get('Stud_id')

        table_name = f'StudT_{shard}'

        check_query = f'''
            SELECT * FROM {table_name}
            WHERE Stud_id = {stud_id};
        '''
        cursor.execute(check_query)
        existing_entry = cursor.fetchone()

        if existing_entry:
            delete_query = f'''
                DELETE FROM {table_name}
                WHERE Stud_id = {stud_id};
            '''
            cursor.execute(delete_query)
            connection.commit()

            return {"message": f"Data entry with Stud_id: {stud_id} removed", "status": "success"}
        else:
            return {"message": f"Data entry with Stud_id: {stud_id} not found", "status":"not_found"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /del DELETE requests
@app.route('/del/<server_id>', methods=['DELETE'])
def delete_data_entry_endpoint(server_id):
    try:
        request_payload = request.json

        if 'shard' in request_payload and 'Stud_id' in request_payload:
            success = delete_data_entry(request_payload,server_id)
            if success["status"]=="success":
                return jsonify({"message": success["message"], "status": "success"}), 200
            else:
                return jsonify({"error": "Failed to delete data entry","status": "not_found"}), 500
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


if __name__ == "__main__":
    
    app.run(host = "0.0.0.0",port=5000 , debug = True)
