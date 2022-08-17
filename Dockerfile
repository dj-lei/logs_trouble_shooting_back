# ===============
# --- Release ---
# ===============
FROM pytorch/pytorch:1.12.0-cuda11.3-cudnn8-runtime
LABEL maintainer="logs_trouble_shooting"

ENV http_proxy "http://100.98.146.3:8080"
ENV https_proxy "http://100.98.146.3:8080"
ENV ftp_proxy "http://100.98.146.3:8080"
ENV FLASK_ENV "production"
ENV FLASK_APP "logs_trouble_shooting.py"

RUN mkdir -p /logs_trouble_shooting

WORKDIR /logs_trouble_shooting
COPY ./ ./
RUN pip3 install -r requirements.txt

EXPOSE 8000

CMD ["python3", "logs_trouble_shooting.py"]
