import json
import requests

from bs4 import BeautifulSoup
import pdb
import os
from flask import Flask, request 
from flask_cors import CORS, cross_origin
from elasticsearch import Elasticsearch 
import boto3
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"*": {"origins": "*"}})

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([os.environ.get("ELASTICSEARCH_URI")], http_auth=(os.environ.get("ELASTICSEARCH_USER"), os.environ.get("ELASTICSEARCH_PASSWORD")))
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es
es = connect_elasticsearch()

@app.route("/check")
def check():
    return json.dumps({})

headers = { 
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1" 
}

def get_resource_url(object_name, expiration=3600):
    ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY") 
    SECRET_KEY = os.environ.get("AWS_SECRET_KEY") 
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    return s3.generate_presigned_url('get_object',
                                Params={'Bucket': 'menu-images-wave',
                                        'Key': object_name},
                                ExpiresIn=expiration)

@app.route("/search", methods = ['GET'])
def get_resources():
    lat = request.args.get("lat") 
    lon = request.args.get("lon") 
    radius = request.args.get("radius", default="12km")
    query = request.args.get("query") 
    size = request.args.get("size", default=10)
    from_index = request.args.get("from", default=0)

    search_object = {
      "from": from_index,
      "size": size,
      "query": {
        "bool": {
          "must": {
            "match_phrase": {
                "menu_pages.text": {
                    "query": query
                }
            }   
          },
          "filter": {
              "geo_distance": {
              "distance": radius,
              "location": {
                "lat": lat, 
                "lon": lon 
              }
            }
          }
        }
      }
    }

    response = es.search(index='places', body=json.dumps(search_object))
    total = response['hits']['total']['value']
    
    places = list(map(lambda x: x['_source'], response['hits']['hits']))
    
    for place in places:
        for page in place['menu_pages']:
            if 'image_uri' in page:
                page['image_url'] = get_resource_url(page['image_uri'])
            
    return json.dumps({ "places": places, "total": total, "from": from_index  })

    ## Crawl site for all images (need to work on filter) 

    ## For each PDF and image.. OCR them if they say contain menu and combine them 

if __name__ == "__main__":
    app.run()
