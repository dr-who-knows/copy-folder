docker build -t sf-copier .
docker run --rm -p 8000:8000 --env-file .env sf-copier
