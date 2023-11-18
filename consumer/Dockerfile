# write a python dockerfile
FROM python:3.9

# set the working directory
WORKDIR /app
# copy the requirements file
COPY requirements.txt .
# install the requirements
RUN pip install -r requirements.txt
# copy the rest of the files
COPY src/ .

# copy shell script
COPY run.sh .
# make shell script executable
RUN chmod +x ./run.sh

# command to run on container start
CMD [ "./run.sh" ]
