import json

import uvicorn
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
    flat_business = flatten(business_obj.dict(), separator='.')

    # Send the data to Kafka
    await send_to_kafka(json.dumps(flat_business))

    return JSONResponse(content=jsonable_encoder(flat_business), status_code=201)


@app.post("/checkin")
async def create_checkin(checkin: Checkin):
    print("Message received from checking.json")

    checkin = checkin.dict()
    checkin_obj = Checkin(**checkin)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(checkin))

    return JSONResponse(content=dict(checkin_obj), status_code=201)


@app.post("/review")
async def create_review(review: Review):
    print("Message received from review.json")

    review = review.dict()
    review_obj = Review(**review)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(review))

    return JSONResponse(content=dict(review_obj), status_code=201)


@app.post("/tip")
async def create_review(tip: Tip):
    print("Message received from tip.json")

    tip = tip.dict()
    tip_obj = Tip(**tip)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(tip))

    return JSONResponse(content=dict(tip_obj), status_code=201)


@app.post("/user")
async def create_review(user: User):
    print("Message received from user.json")

    user = user.dict()
    user_obj = User(**user)

    # Send the data to Kafka
    await send_to_kafka(json.dumps(user))

    return JSONResponse(content=dict(user_obj), status_code=201)


async def send_to_kafka(value):
    # linger_ms=500, producer will wait up to half a second before sending a batch of messages to improve performance
    # by reducing the number of network round trips required to send messages

    # Create producer if we work on localhost
    producer = KafkaProducer(
        bootstrap_servers='localhost:9093',
        acks=1,
        linger_ms=500
    )
    # Create producer if we work on docker container
    # producer = KafkaProducer(
    #     bootstrap_servers='kafka:9092',
    #     acks=1,
    #     linger_ms=500
    # )
    # Convert JSON string to bytes

    # Send the message to Kafka
    producer.send("Yelp-topic", bytes(value, 'utf-8'))

    producer.flush()


# Start the server of FastAPI on the localhost
uvicorn.run(app, host="127.0.0.1", port=8000)
