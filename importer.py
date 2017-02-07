import os
import xml.etree.ElementTree as ET
import psycopg2 as p
from urllib.parse import urlparse

class Artist:

    def init(self, name, realname, profile, data_quality, urls):
        self.name = name
        self.realname = realname
        self.profile = profile
        self.data_quality = data_quality
        self.urls = urls

    def print(self):
        print(self.name, self.data_quality)


class Urls:
    def init(self, facebook, soundcloud, spotify, itunes, twitter):
        self.facebook = facebook
        self.soundcloud = soundcloud
        self.spotify = spotify
        self.itunes = itunes
        self.twitter = twitter


# Load XML file into a tree in memory
tree = ET.parse('data/artists.xml')
root = tree.getroot()

# Connect to database
con = p.connect(database='postgres', host='localhost', port=5432)
cur = con.cursor()


# Iterate over each artist in db
for artist in root:

    for elem in artist:
        if elem.tag == 'id':
            pass
        if elem.tag == 'name':
            pass
        if elem.tag == 'realname':
            pass
        if elem.tag == 'profile':
            pass
        if elem.tag == 'quality':
            pass
        if elem.tag == 'data_quality':
            pass
        if elem.tag == 'namevariation':
            pass
        if elem.tag == 'urls':
            pass

    # id = child.findtext('id')
    # name = child.findtext('name')
    # realname = child.findtext('realname')
    # profile = child.findtext('profile')
    # quality = child.findtext('data_quality')
    # namevariations = child.findtext('namevariations')
    #
    # urls_block = child.find("urls")
    # urls = list(urls_block.iter("url"))
    #
    # aliases = child.findtext('aliases')

    try:
        cur.execute("INSERT INTO artists VALUES (%s, %s, %s, %s, %s)", (id, name, realname, profile, quality))
        con.commit()
        print(name, realname, profile, quality, urls)
    except:
        con.rollback()
        print("Insert failed")


