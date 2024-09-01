FROM jupyter/base-notebook:latest

WORKDIR /home/jovyan/work

COPY requirements.txt /home/jovyan/work/requirements.txt

RUN pip install -r /home/jovyan/work/requirements.txt

CMD ["start.sh", "jupyter", "lab"]
