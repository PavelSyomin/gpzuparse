# syntax=docker/dockerfile:1
FROM python:3.10-bullseye
WORKDIR /home/app
COPY app.py /home/app
COPY parser.py /home/app
COPY templates /home/app/templates/
COPY data /home/app/data
COPY requirements.txt /home/app
RUN pip install -r requirements.txt
RUN apt update
RUN apt install default-jre ghostscript --assume-yes
RUN sed -i '/<policy domain="coder" rights="none" pattern="PDF" \/>/d' /etc/ImageMagick-6/policy.xml
CMD ["uvicorn", "app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]
EXPOSE 80
