from c8 import C8Client
import requests

#----------------------------
class Cache:
  def __init__(self, fabric):
    self.fabric = fabric
    if fabric.has_collection('cache'):
        self.cache = fabric.collection('cache')
    else:
        self.cache = fabric.create_collection('cache')

  def get(self, key):
    return self.cache.get(key)

  def set(self, key, document, ttl=0):
    if self.cache.has(key):
        self.cache.replace(document)
    else:
        self.cache.insert(document)
    return True

  def purge(self, keys):
    for key in keys:
        self.cache.delete(key)
    return True
#----------------------------

def get_from_origin_db(key):
    r = requests.get(url="https://openlibrary.org/subjects/"+key+".json?limit=0")
    document = r.json()
    document["_key"] = key
    return document

def write_to_origin_db(document):
    # Write to Origin DB
    print("writing to origin database..")
    # Origin specific code...
    return True
#----------------------------

if __name__ == '__main__':
    # variables
    fed_url = "gdn1.macrometa.io"
    guest_mail = "guest1@macrometa.io"
    guest_password = "guest1"
    geo_fabric = "guest1"
    keys = ["dogs", "cats", "birds", "books", "business"]

    print("\nConnection: federation:{},  user: {}".format(fed_url, guest_mail))
    client = C8Client(protocol='https', host=fed_url, port=443)
    tenant = client.tenant(guest_mail, guest_password)
    fabric = tenant.useFabric(geo_fabric)
    cache = Cache(fabric)
    cache.purge(keys) # Clean up cache data from any prior runs.

    # Macrometa GDN as Cache for Remote Origin Database
    print("----------------------\n1. First time access:\n----------------------")
    for key in keys:
        doc = cache.get(key)
        if(doc is None):
            print("CACHE_MISS: get from remote origin database...")
            doc = get_from_origin_db(key)
            print("CACHE_UPDATE: \ndoc:{}".format(doc))
            cache.set(key, doc)
        else:
            print("CACHE_HIT: \ndoc:{}".format(doc))

    # 2nd time access served from GDN edge cache.
    print("----------------------\n2. Second time access:\n----------------------")
    for key in keys:
        doc = cache.get(key)
        if (doc is None):
            print("CACHE_MISS: get from remote origin database...")
            doc = get_from_origin_db(key)
            print("CACHE_UPDATE: \ndoc:{}".format(doc))
            cache.set(key, doc)
        else:
            print("CACHE_HIT: \ndoc:{}".format(doc))

    # GDN Cache Geo distributed replication in action
    print("------------------------------\n3. Access from different region:\n------------------------------")
    fed_url_asia = "gdn1-fra1.prod.macrometa.io"
    client2 = C8Client(protocol='https', host=fed_url_asia, port=443)
    tenant2 = client.tenant(guest_mail, guest_password)
    fabric2 = tenant.useFabric(geo_fabric)
    cache2 = Cache(fabric)

    for key in keys:
        doc = cache2.get(key)
        if (doc is None):
            print("CACHE_MISS: get from remote origin database...")
            doc = get_from_origin_db(key)
            print("CACHE_UPDATE: \ndoc:{}".format(doc))
            cache2.set(key, doc)
        else:
            print("CACHE_HIT: \ndoc:{}".format(doc))

    # Cache purge in action...
    print("------------------------------\n4. Update cache from edge:\n------------------------------")
    for key in keys:
        doc = cache.get(key)
        doc["company"] = "macrometa"
        write_to_origin_db(doc)
        cache.set(key, doc)

    print("------------------------------\n5. Access from different region:\n------------------------------")
    for key in keys:
        doc = cache2.get(key)
        if (doc is None):
            print("CACHE_MISS: get from remote origin database...")
            doc = get_from_origin_db(key)
            print("CACHE_UPDATE: \ndoc:{}".format(doc))
            cache2.set(key, doc)
        else:
            print("CACHE_HIT: \ndoc:{}".format(doc))
