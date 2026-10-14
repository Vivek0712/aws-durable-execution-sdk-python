FROM public.ecr.aws/docker/library/python:3.13-slim
WORKDIR /app
# The runner builds the worker from this repository and adds its wheel.
COPY *.whl app.py ./
RUN pip install --no-cache-dir ./*.whl
EXPOSE 8080
# Lambda snapshots the running process after it starts.
CMD ["python", "app.py"]
