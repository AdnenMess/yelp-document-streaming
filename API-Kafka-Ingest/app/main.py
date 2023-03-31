import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from schemas import Business, Checkin, Review, Tip, User
from flatten_json import flatten

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

    return JSONResponse(content=dict(flat_business), status_code=201)


@app.post("/checkin")
async def create_checkin(checkin: Checkin):
    print("Message received from checking.json")

    checkin = checkin.dict()
    checkin_obj = Checkin(**checkin)

    return JSONResponse(content=dict(checkin_obj), status_code=201)


@app.post("/review")
async def create_review(review: Review):
    print("Message received from review.json")

    review = review.dict()
    review_obj = Review(**review)

    return JSONResponse(content=dict(review_obj), status_code=201)


@app.post("/tip")
async def create_review(tip: Tip):
    print("Message received from tip.json")

    tip = tip.dict()
    tip_obj = Tip(**tip)

    return JSONResponse(content=dict(tip_obj), status_code=201)


@app.post("/user")
async def create_review(user: User):
    print("Message received from user.json")

    user = user.dict()
    user_obj = User(**user)

    return JSONResponse(content=dict(user_obj), status_code=201)

# Start the server on the localhost
uvicorn.run(app, host="127.0.0.1", port=8000)
