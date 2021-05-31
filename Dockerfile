FROM python:3.8.7-slim

RUN apt-get update -y --fix-missing && \
  apt-get -y install \
  make \
  git

WORKDIR /root
RUN pip install --upgrade pip && pip --version
RUN pip install pipenv && pipenv --version

COPY Pipfile Pipfile.lock ./
RUN pipenv install --system

COPY . .
EXPOSE 8080
CMD ["/bin/sh", "-c", "exec /usr/local/bin/uvicorn --host 0.0.0.0 --port $PORT api:app"]
