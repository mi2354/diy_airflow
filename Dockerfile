FROM python:3.9.13-slim-buster
RUN pip install poetry
COPY . .
RUN poetry config virtualenvs.create false && poetry install
ENTRYPOINT [ "diy_airflow" ]
CMD [ "scheduler" ]