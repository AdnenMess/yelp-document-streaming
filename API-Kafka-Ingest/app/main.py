import json

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from schemas import Business, Checkin, Review, Tip, User
from flatten_json import flatten
from kafka import KafkaProducer

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Welcome to my App"}


@app.post("/business")
async def create_business(business: Business):
    print("Message received from business.json")
    # Validate input data using Pydantic
    business = business.dict()
    business_obj = Business(**business)

    # Flatten the object
    flat_business = flatten(business_obj.dict(), separator='_')

    # Send the data to Kafka
    await send_to_kafka(json.dumps(flat_business), key='business')

    return JSONResponse(content=jsonable_encoder(flat_business), status_code=201)


@app.post("/checkin")
async def create_checkin(checkin: Checkin):
    print("Message received from checking.json")

    checkin = checkin.dict()
    checkin_obj = Checkin(**checkin)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(checkin), key='checkin')

    return JSONResponse(content=dict(checkin_obj), status_code=201)


@app.post("/review")
async def create_review(review: Review):
    print("Message received from review.json")

    review = review.dict()
    review_obj = Review(**review)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(review), key='review')

    return JSONResponse(content=dict(review_obj), status_code=201)


@app.post("/tip")
async def create_review(tip: Tip):
    print("Message received from tip.json")

    tip = tip.dict()
    tip_obj = Tip(**tip)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(tip), key='tip')

    return JSONResponse(content=dict(tip_obj), status_code=201)


@app.post("/user")
async def create_review(user: User):
    print("Message received from user.json")

    user = user.dict()
    user_obj = User(**user)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(user), key='user')

    return JSONResponse(content=dict(user_obj), status_code=201)


async def send_to_kafka(value, key=None):
    # linger_ms=500, producer will wait up to half a second before sending a batch of messages to improve performance
    # by reducing the number of network round trips required to send messages

    # Create producer
    # bootstrap_servers='localhost:9093' if we work on localhost
    # bootstrap_servers='kafka:9092' if we work on Docker

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        acks=1,
        linger_ms=500
    )

    # Send the message to Kafka
    producer.send("Yelp-topic", key=bytes(key, 'utf-8') if key else None, value=bytes(value, 'utf-8'))

    producer.flush()
