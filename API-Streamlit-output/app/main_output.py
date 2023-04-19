from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import uvicorn

app = FastAPI()

client = MongoClient("mongodb://localhost:27017/", username="root", password="example")
db = client["docstreaming"]
yelp_collection = db["yelp"]


@app.get("/")
async def root():
    return {"message": "Welcome to my App"}


@app.get("/yelp/{business_id}")
async def get_business(business_id: str):
    business_id = yelp_collection.find_one({"business_id": business_id}, {"_id": 0, "attributes": 0})
    return JSONResponse(content=jsonable_encoder(business_id), status_code=201)


@app.get("/yelp/{user_id}")
async def get_business(user_id: str):
    user_id = yelp_collection.find_one({"user_id": user_id}, {"_id": 0, "attributes": 0})
    return JSONResponse(content=jsonable_encoder(user_id), status_code=201)


uvicorn.run(app, port=8001, host="127.0.0.1")
