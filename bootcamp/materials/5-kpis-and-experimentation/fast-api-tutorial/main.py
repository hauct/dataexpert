from fastapi import FastAPI, Header
from enum import Enum
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

@app.get("/", description='This is our first route')
async def base_get_root():
    return {'message': 'Hello World, I am Hau, i am an data hello'}

@app.get('/greet')
async def greet_name(name: Optional[str] = 'User', age:int = 0) -> dict:
    return {'message': f'Hello {name}', 'age': age}

class BookCreateModel(BaseModel):
    title: str
    author: str

@app.post('/create_book')
async def create_book(book_data: BookCreateModel):
    return {
        'title': book_data.title
        , 'author': book_data.author
    }

@app.get('/get_headers')
async def get_headers(accept:str = Header(None)
                      , content_type:str = Header(None)
                      , user_agent:str = Header(None)
                      , host:str = Header(None)
                    ):
    request_headers = {}
    request_headers['Accept'] = accept
    request_headers['Content-Type'] = content_type
    request_headers['User-Agent'] = user_agent
    request_headers['Host'] = host

    return request_headers




# @app.post("/")
# async def post():
#     return {'message': 'Hello from the post route'}

# @app.put("/")
# async def put():
#     return {'message': 'Hello from the put route'}

# # @app.get("/items",)
# # async def get_list_items():
# #     return {'message': 'list item route'}

# # @app.get("/items/{item_id}",)
# # async def get_item(item_id :int):
# #     return {'item': item_id}

# # @app.get("/users")
# # async def list_users():
# #     return {"message": "list users route"}

# @app.get("/users/me")
# async def get_current_user():
#     return {"user_id": 'hello i am'}

# @app.get("/users/{user_id}")
# async def get_user(user_id: str):
#     return {"user_id": user_id}

# class FoodEnum(str, Enum):
#     fruits = 'fruits'
#     vegetables = 'vegetables'
#     dairy = 'dairy'

# @app.get('/foods/{food_name}')
# async def get_user(food_name: FoodEnum):
#     if food_name == FoodEnum.vegetables:
#         return {'food_name': food_name, "message": "you're healthy"}

#     if food_name.value == 'fruits':
#     # if food_name == FoodEnum.fruits:
#     # if food_name.fruits == 'fruits':
#         return {'food_name': food_name, "message": "you're still healthy, but like sweet things"}

#     return {'food_name': food_name, "message": "i like chocolate milk"}


# # fake_items_db = [{"item_name": "Foo"}, {"item_name": "Bar"}, {"item_name": "Baz"}]

# # @app.get("/items")
# # async def list_items(skip: int = 0, limit: int = 10):
# #     return fake_items_db[skip: skip+limit]


# @app.get("/items/{item_id}")
# async def get_item(item_id: str, sample_query_param: str, q: str | None = None, short: bool = False):
#     item = {"item_id": item_id, "sample_query_param": sample_query_param}
#     if q:
#         if not short:
#             item.update({"q": q})
#         item.update(
#             {
#                 "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
#             }
#         )
#     return item

# @app.get("/users/{user_id}/items/{item_id}")
# async def get_user_item(user_id: int, item_id: str, q: str | None = None, short: bool = False):
#     item = {"item_id": item_id, "owner_id": user_id}
#     if q:
#         item.update({"q": q})
#     if not short:
#         item.update(
#             {
#                 "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
#             }
#         )
#     return item

# class Item(BaseModel):
#     name: str
#     description: str | None = None
#     price: float
#     tax: float | None = None


# @app.post("/items")
# async def create_item(item: Item) -> Item:
#     item_dict = item.dict()
#     if item.tax:
#         price_with_tax = item.price + item.tax
#         item_dict.update({'price_with_tax': price_with_tax})
#     return item_dict

# @app.put("/items/{item_id}")
# async def create_item_with_put(item_id: int, item: Item, q: str | None = None):
#     result =  {'item_id': item_id, **item.dict()}
#     if q:
#         result.update({'q': q})
#     return result

# @app.get("/items")
# async def read_items(q: list[str] | None = Query(None, min_length=3, max_length=10, title='simple query string', description='This is a simple query string!')):
#     results = {"items": [{"item_id": "Foo"}, {"item_id": "Bar"}]}
#     if q:   
#         results.update({"q": q})
#     return results