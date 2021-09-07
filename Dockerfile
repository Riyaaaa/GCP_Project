FROM python:3.7-slim
RUN pip install tweepy
RUN pip install pytz
RUN pip install google-cloud-pubsub
WORKDIR /tweets
COPY . .
ENV PYTHONPATH="."
CMD ["twitter_stream.py","annular-garage-325314","twitter"]
ENTRYPOINT ["python3"]
