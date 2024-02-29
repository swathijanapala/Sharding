FROM  python:3.10-slim 

COPY . /Server

WORKDIR /Server
EXPOSE 5000
# COPY requirements.txt .

# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip3 install -r requirements.txt

# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip3 install -r requirements.txt

RUN pip install --trusted-host pypi.python.org flask


# run pip install flask
CMD ["python","App.py"]