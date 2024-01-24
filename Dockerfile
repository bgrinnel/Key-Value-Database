FROM python:3.7-alpine

WORKDIR /app


RUN pip3 install Flask  requests

COPY ./src ./src

ENV PORT = 8080

EXPOSE 8080

CMD [ "python", "./src/index.py"]

