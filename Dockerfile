FROM python:3.9.13-slim-buster
WORKDIR /app
# We install first the requirements.txt, so if something changes in the rest of the package, it does not trigger a whole reinstall
COPY requirements.txt .
RUN pip install -r requirements.txt
# We are copying again the requirements.txt, but it does not matter
COPY . .
# It will detect that the packages are already installed, so it will not install them again
RUN pip install .
ENTRYPOINT [ "diy_airflow" ]
CMD ["scheduler"]
