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


if __name__ == "__main__":
    # print(sys.argv)
    # print(">>>>>>>>>>>>>> IN HELPER <<<<<<<<<<<")
    if(sys.argv[-1]=="add"):
        main1(sys.argv)
    elif(sys.argv[-1]=="remove"):
        main2(sys.argv)
    else:
        # print(">>>>>>>>>>>>>> IN HELPER <<<<<<<<<<<")
        get_docker_processes()

