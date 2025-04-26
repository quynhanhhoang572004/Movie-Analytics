SongSight-Analytics


## 1. Build the docker image
docker build -t pyspark-docker .

## 2. Prepare your pyspark script

## 3. Run the script in docker
docker run -it --rm -p 4041:4040 -v ${PWD}/test.py:/app/test.py pyspark-docker python /app/test.py (your file python)

```bash