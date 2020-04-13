FROM python:3.7

ADD coronascript.py /

RUN pip install -U pip pandas "dask[complete]" psutil

VOLUME /data, /output

CMD [ "python", "./coronascript.py" ]