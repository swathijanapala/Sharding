import asyncio
import aiohttp
import matplotlib.pyplot as plt
import time, random, uuid
import json
 
async def make_request(session, method, url, payload):

    if (method == 'POST'):
        async with session.post(url, json = payload) as response:
            return await response.text()

def generate_unique_id():
    id = uuid.uuid4()  
    hash_id = hash(id)  
    return abs(hash_id) % 1000000 

def generate_name():
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=6))
    return name



async def send_10k_write_req():

    url_write = "http://127.0.0.1:5000/write"
    cnt = 0
    start_time = time.time()
    while (cnt < 10000):
        n = 1
        temp, write_payload = {}, {}
        write_payload['data'] = []
        for i in range(n):
            sid = generate_unique_id()
            #existing_ids.add(sid)
            sname = generate_name()
            smarks = random.randint(0, 101)

            temp['Stud_id'] = sid
            temp['Stud_marks'] = smarks
            temp['Stud_name'] = sname
            
            write_payload['data'].append(temp.copy())
            temp.clear()
 
        async with aiohttp.ClientSession() as session:
            tasks = [make_request(session, 'POST', url_write, payload = write_payload)]
            responses = await asyncio.gather(*tasks)
        cnt += 1

    write_time = time.time() - start_time
    print('write speed for 10000 requests: ', write_time)



async def send_10k_read_req():
    
    url_read = "http://127.0.0.1:5000/read"
    cnt = 0
    start_time, read_time = time.time(), 0
    while (cnt < 10000):

        high = random.randint(0, 999999)
        low = random.randint(0, high)
        read_payload = {"Stud_id": {"low":low, "high":high}}

        try:
            async with aiohttp.ClientSession() as session:
                tasks = [make_request(session, 'POST', url_read, payload = read_payload)]
                responses = await asyncio.gather(*tasks)

            end_time = time.time()
            read_time += (end_time - start_time)
            start_time = end_time

            # To see the data of students records
            # for res in responses:
            #     print(json.loads(res)["data"])
        
        except:
            pass
        cnt += 1

    print('read speed for 10000 requests: ', read_time)


async def main():

    # A1 task
    await send_10k_write_req()
    await send_10k_read_req()
    
 
if __name__ == "__main__":
    asyncio.run(main())
