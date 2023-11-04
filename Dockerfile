# set base image (host OS)
FROM python:3.11-alpine3.17

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY . .

# copy shell script
COPY ./data-ingestion/run.sh ./data-ingestion/run.sh
RUN chmod +x ./data-ingestion/run.sh

# command to run on container start
CMD [ "./data-ingestion/run.sh" ]
