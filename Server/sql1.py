from flask import Flask, request, jsonify
import mysql.connector


app = Flask(__name__)

# Replace these values with your MySQL database credentials
db_config = {
    
    'host': 'server1',
    'user': 'root',
    'password': 'user12',
    'database': 'STUDENT',
    'port' : 3306
}


def initialize_shard_tables(payload):
    try:
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
            create_table_query = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join([f'{column} INT' if dtype == 'Number' else f'{column} VARCHAR(100)' if dtype == 'String' else f'{column} {dtype}' for column, dtype in zip(columns, dtypes)])},
                    PRIMARY KEY (Stud_id)
                );
            '''

            print(create_table_query,flush=True)
            cursor.execute(create_table_query)

        connection.commit()

        return {"message": "Shard tables initialized successfully"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    

@app.route('/config', methods=['POST'])
def configure_shard_tables():

    try:
        request_payload = request.json

        # Validate the payload structure
        if 'schema' in request_payload and 'shards' in request_payload:
            response = initialize_shard_tables(request_payload)
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
    
@app.route('/insert', methods=['POST'])
def insert():
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
def copy_data_entries(shard):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Replace 'StudT' with your actual table name
        table_name = f'StudT_{shard}'

        # Select all data entries from the shard table
        select_query = f'SELECT * FROM {table_name}'
        cursor.execute(select_query)
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

# Endpoint to handle /copy GET requests
@app.route('/copy', methods=['GET'])
def copy_data():
    try:
        request_payload = request.get_json()

        shards = request_payload.get('shards')

        if shards and isinstance(shards, list):
            response_data = {}
            for shard in shards:
                response_data[shard] = copy_data_entries(shard)

            return jsonify(response_data), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
# Function to write data entries to a shard in a particular server container
def write_data_entries(shard, curr_idx, data):
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
        get_current_index_query = f'SELECT MAX(stud_id) FROM {table_name}'
        cursor.execute(get_current_index_query)
        current_idx = cursor.fetchone()[0]

        if current_idx is None:
            current_idx = 0

        return {"message": "Data entries added", "current_idx": current_idx, "status": "success"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Endpoint to handle /write POST requests
@app.route('/write', methods=['POST'])
def write_data():
    try:
        request_payload = request.json

        shard = request_payload.get('shard')
        curr_idx = request_payload.get('curr_idx')
        data = request_payload.get('data')

        if shard and curr_idx is not None and data and isinstance(data, list):
            response = write_data_entries(shard, curr_idx, data)
            return jsonify(response), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    
if __name__ == '__main__':
     # Connect to MySQL
    
    app.run(host = "0.0.0.0",debug=True)
