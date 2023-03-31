from typing import Optional, List
from pydantic import BaseModel

'''
Define all Classes for Business BaseModel
'''


class AttributesMusic(BaseModel):
    dj: Optional[bool]
    background_music: Optional[bool]
    no_music: Optional[bool]
    jukebox: Optional[bool]
    live: Optional[bool]
    video: Optional[bool]
    karaoke: Optional[bool]


class AttributesAmbience(BaseModel):
    touristy: Optional[bool]
    hipster: Optional[bool]
    romantic: Optional[bool]
    divey: Optional[bool]
    intimate: Optional[bool]
    trendy: Optional[bool]
    upscale: Optional[bool]
    classy: Optional[bool]
    casual: Optional[bool]


class AttributeGoodForMeal(BaseModel):
    dessert: Optional[bool]
    latenight: Optional[bool]
    lunch: Optional[bool]
    dinner: Optional[bool]
    brunch: Optional[bool]
    breakfast: Optional[bool]


class AttributeBusinessParking(BaseModel):
    garage: Optional[bool]
    street: Optional[bool]
    validated: Optional[bool]
    lot: Optional[bool]
    valet: Optional[bool]


class BusinessAttributes(BaseModel):
    Smoking: str
    NoiseLevel: str
    Caters: str
    WiFi: str
    RestaurantsGoodForGroups: str
    Music: AttributesMusic
    OutdoorSeating: str
    RestaurantsTableService: str
    RestaurantsAttire: str
    Ambience: AttributesAmbience
    RestaurantsReservations: str
    RestaurantsTakeOut: str
    GoodForDancing: str
    RestaurantsPriceRange2: str
    GoodForMeal: AttributeGoodForMeal
    GoodForKids: str
    HappyHour: str
    RestaurantsDelivery: str
    BusinessParking: AttributeBusinessParking
    BikeParking: str
    BusinessAcceptsCreditCards: str
    HasTV: str
    Alcohol: str


class BusinessHours(BaseModel):
    Monday: str
    Tuesday: str
    Wednesday: str
    Thursday: str
    Friday: str
    Saturday: str
    Sunday: str


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
    attributes: BusinessAttributes
    categories: List[str]
    hours: BusinessHours


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
