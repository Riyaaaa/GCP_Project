FROM python:3.7-slim
RUN pip install tweepy
RUN pip install pytz
RUN pip install google-cloud-pubsub
WORKDIR /capstone
COPY . .
ENV PYTHONPATH="."
CMD ["tweets.py","tensile-pier-322516","twitter"]
ENTRYPOINT ["python3"]
