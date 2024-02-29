
FROM  python:3.10-slim 
COPY . /LoadBalancer

WORKDIR /LoadBalancer

ENV server_id = 1
RUN apt-get update
RUN apt-get -y install sudo

# RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y docker.io
EXPOSE 5000
# run pip install flask
CMD ["python","App.py"]