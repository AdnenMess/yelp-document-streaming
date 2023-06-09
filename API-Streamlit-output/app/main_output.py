from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

app = FastAPI()

client = MongoClient("mongodb://mongo:27017/", username="root", password="example", directConnection=True)
db = client["docstreaming"]
yelp_collection = db["yelp"]


@app.get("/")
async def root():
    return {"message": "Welcome to my App"}


@app.get("/yelp/business/{business_id}")
async def get_business(business_id: str):
    business_id = yelp_collection.find_one({"business_id": business_id}, {"_id": 0, "attributes": 0})
    return JSONResponse(content=jsonable_encoder(business_id), status_code=201)


@app.get("/yelp/user/{user_id}")
async def get_business(user_id: str):
    user_id = yelp_collection.find_one({"user_id": user_id}, {"_id": 0, "attributes": 0})
    return JSONResponse(content=jsonable_encoder(user_id), status_code=201)
