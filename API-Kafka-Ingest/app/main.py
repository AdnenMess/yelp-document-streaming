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

    # Flatten the categories
    flat_categories = {'categories_' + str(i): category for i, category in enumerate(business_obj.categories)}

    # Flatten the hours
    flat_hours = {}
    if business_obj.hours:
        flat_hours = flatten(business_obj.hours.dict(), separator='_')

    # Merge the flattened fields
    flat_business = {**flat_categories, **flat_hours}

    # Add the remaining fields
    flat_business.update({k: v for k, v in business_obj.dict().items() if k not in ['categories', 'hours']})

    # Send the data to Kafka
    send_to_kafka(json.dumps(flat_business))

    return JSONResponse(content=jsonable_encoder(flat_business), status_code=201)


@app.post("/checkin")
async def create_checkin(checkin: Checkin):
    print("Message received from checking.json")

    checkin = checkin.dict()
    checkin_obj = Checkin(**checkin)

    # Send the data to Kafka
    send_to_kafka(json.dumps(checkin))

    return JSONResponse(content=dict(checkin_obj), status_code=201)


@app.post("/review")
async def create_review(review: Review):
    print("Message received from review.json")

    review = review.dict()
    review_obj = Review(**review)

    # Send the data to Kafka
    send_to_kafka(json.dumps(review))

    return JSONResponse(content=dict(review_obj), status_code=201)


@app.post("/tip")
async def create_review(tip: Tip):
    print("Message received from tip.json")

    tip = tip.dict()
    tip_obj = Tip(**tip)

    # Send the data to Kafka
    send_to_kafka(json.dumps(tip))

    return JSONResponse(content=dict(tip_obj), status_code=201)


@app.post("/user")
async def create_review(user: User):
    print("Message received from user.json")

    user = user.dict()
    user_obj = User(**user)

    # Send the data to Kafka
    send_to_kafka(json.dumps(user))

    return JSONResponse(content=dict(user_obj), status_code=201)


def send_to_kafka(value):
    # Create producer
    # bootstrap_servers='localhost:9093' if we work on localhost
    # bootstrap_servers='kafka:9092' if we work on Docker

    producer = KafkaProducer(bootstrap_servers='kafka:9092', acks=1)

    # Send the message to Kafka
    producer.send("Yelp-topic", value=bytes(value, 'utf-8'))

    producer.flush()
