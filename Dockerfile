# syntax=docker/dockerfile:1
FROM python:3.10-bullseye
WORKDIR /home/app
COPY app.py /home/app
COPY parser.py /home/app
COPY templates /home/app/templates/
COPY media/devplans home/app/media/devplans
COPY requirements.txt /home/app
RUN pip install -r requirements.txt
RUN apt update
RUN apt install default-jre --assume-yes
CMD ["uvicorn", "app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]
EXPOSE 80
