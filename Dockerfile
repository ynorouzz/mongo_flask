FROM python:3

COPY ./src/ /src

COPY ./requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

CMD [ "python", "src/part1_to_mongodb.py" ]