FROM coady/pylucene:latest

WORKDIR /usr/app/src

COPY . .

CMD ["python3", "-m", "http.server", "80"]