from typing import Optional, List, Dict, Any
from pydantic import BaseModel

'''
Define all Classes for Business BaseModel
'''


class BusinessHours(BaseModel):
    Monday: Optional[str]
    Tuesday: Optional[str]
    Wednesday: Optional[str]
    Thursday: Optional[str]
    Friday: Optional[str]
    Saturday: Optional[str]
    Sunday: Optional[str]


# The main class
class Business(BaseModel):
    business_id: str
    name: str
    address: str
    city: str
    postal_code: str
    latitude: float
    longitude: float
    stars: float
    review_count: int
    is_open: int
    attributes: Dict[str, Any]
    categories: List[str]
    hours: Optional[BusinessHours]

    # The Config class inside DynamicModel allows arbitrary types to be allowed,
    # which means any type can be assigned to the attributes field.
    class Config:
        arbitrary_types_allowed = True


'''
Define the BaseModel Class Checkin
'''


class Checkin(BaseModel):
    business_id: str
    date: int


'''
Define the BaseModel Class Review
'''


class Review(BaseModel):
    review_id: str
    user_id: str
    business_id: str
    stars: float
    useful: int
    funny: int
    cool: int
    text: str
    date: str


'''
Define the BaseModel Class Tip
'''


class Tip(BaseModel):
    user_id: str
    business_id: str
    text: str
    date: str
    compliment_count: int


'''
Define the BaseModel Class User
'''


class User(BaseModel):
    user_id: str
    name: str
    review_count: int
    yelping_since: str
    useful: int
    funny: int
    cool: int
    elite: str
    friends: int
    fans: int
    average_stars: float
    compliment_hot: int
    compliment_more: int
    compliment_profile: int
    compliment_cute: int
    compliment_list: int
    compliment_note: int
    compliment_plain: int
    compliment_cool: int
    compliment_funny: int
    compliment_writer: int
    compliment_photos: int
