To have the script running, you should have a .env file containing those fields:

# .env file
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_S3_ENDPOINT=
AWS_DEFAULT_REGION=


Your bucket and the output file should be in the yaml file.

Finally, to install the necessary dependencies you can use poetry.

1) pip install poetry
2) poetry install
3) poetry env activate
