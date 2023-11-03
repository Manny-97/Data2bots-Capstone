# set base image (host OS)
FROM python:3.11-alpine3.17

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# copy shell script
COPY run.sh .
RUN chmod +x ./run.sh

# command to run on container start
CMD [ "./data-ingestion/run.sh" ]
