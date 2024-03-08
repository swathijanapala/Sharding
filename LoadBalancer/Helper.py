import subprocess
import sys
import docker 
def main1(params):
   
    cmd = ["docker", "run", "--name", params[1],"--network", params[2], "--network-alias", params[1], "-d",params[3]]
    
    try:
        result = subprocess.run(cmd)
        print("Return Code:", result.returncode)
        if(result.returncode!=0):
            print("Container not created")
       
    except Exception as e:
        print("Container not created")

def main2(params):
   
    cmd = ["docker", "rm", "-f", params[1]]
    
    try:
        result = subprocess.run(cmd)
        print("Return Code:", result.returncode)
        if(result.returncode!=0):
            raise Exception("Container not removed")
       
    except Exception as e:
        raise Exception("Container not removed")

def get_docker_processes():
    try:
        # Run the docker ps command with custom formatting
        result = subprocess.run(["docker", "ps","--filter", "ancestor=flaskserver1", "--format",'{{.Names}}'], capture_output=True, text=True, check=True)

        # Extract container names from the output
        container_names = result.stdout.splitlines()
        for i in container_names:
            print(i)
        # if(container_names):
        #     print(container_names,end="")
        # return container_names

    except subprocess.CalledProcessError as e:
        print(f"Error running 'docker ps' command: {e.stderr}")



def initialize_tables(connection):
    try:
        #connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Initialize ShardT table
        cursor.execute('DROP TABLE IF EXISTS ShardT')
        create_shardt_table_query = '''
            CREATE TABLE IF NOT EXISTS ShardT (
                Stud_id_low INT PRIMARY KEY,
                Shard_id VARCHAR(255),
                Shard_size INT,
                valid_idx INT
            );
        '''
        cursor.execute(create_shardt_table_query)

        # Initialize MapT table
        cursor.execute('DROP TABLE IF EXISTS MapT')
        create_mapt_table_query = '''
            CREATE TABLE IF NOT EXISTS MapT (
                Shard_id VARCHAR(255),
                Server_id INT
            );
        '''
        cursor.execute(create_mapt_table_query)

        connection.commit()
        cursor.close()
        #connection.close()

    except Exception as e:
        return {"error": f"An error occurred while initializing tables: {str(e)}"}

    return {"message": "Tables initialized successfully"}


def insert_shard_info(connection,shards):
    try:
        #connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        for shard_info in shards:
            insert_shard_query = '''
                INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, valid_idx)
                VALUES (%s, %s, %s, %s)
            '''
            cursor.execute(insert_shard_query, (
                shard_info['Stud_id_low'],
                shard_info['Shard_id'],
                shard_info['Shard_size'],
                shard_info['valid_idx']
            ))

        connection.commit()
        cursor.close()
        #connection.close()

    except Exception as e:
        return {"error": f"An error occurred while inserting shard info: {str(e)}"}

    return {"message": "Shard info inserted successfully"}


def insert_server_shard_mapping(connection,servers):
    try:
      #connection = mysql.connector.connect(**db_config)
        #if connection is not None:
        cursor = connection.cursor()

        for server_id, shard_ids in servers.items():
            for shard_id in shard_ids:
                insert_mapping_query = '''
                    INSERT INTO MapT (Shard_id, Server_id)
                    VALUES (%s, %s)
                '''
                cursor.execute(insert_mapping_query, (shard_id, server_id))

        connection.commit()
        cursor.close()
        #connection.close()

    except Exception as e:
        return {"error": f"An error occurred while inserting server-shard mapping: {str(e)}"}

    return {"message": "Server-shard mapping inserted successfully"}


if __name__ == "__main__":
    # print(sys.argv)
    # print(">>>>>>>>>>>>>> IN HELPER <<<<<<<<<<<")
    if(sys.argv[-1]=="add"):
        main1(sys.argv)
    elif(sys.argv[-1]=="remove"):
        main2(sys.argv)
    else:
        #shards=sys.argv[1]
        #servers=sys.argv[2]
        #intialization(shards,servers)
        #print(">>>>>>>>>>>>>> IN HELPER <<<<<<<<<<<")
        get_docker_processes()


