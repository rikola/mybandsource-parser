import os
import xml.etree.ElementTree as ET
import psycopg2 as p
from urllib.parse import urlparse


class Artist:

    def __init__(self):
        self.id = None
        self.name = None
        self.realname = None
        self.profile = None
        self.data_quality = None
        self.urls = []
        self.namevariation = []
        self.facebook_url = None
        self.soundcloud_url = None
        self.spotify_url = None
        self.twitter_url = None
        self.instagram_url = None

    def parse_urls(self):
        for entry in self.urls:

            o = ''
            try:
                o = urlparse(entry)
            except:
                continue

            if o.netloc.find('facebook.com') >= 0:
                    self.facebook_url = entry
            elif o.netloc.find('soundcloud.com') >= 0:
                self.soundcloud_url = entry
            elif o.netloc.find('spotify.com') >= 0:
                self.spotify_url = entry
            elif o.netloc.find('twitter.com') >= 0:
                self.twitter_url = entry
            elif o.netloc.find('instagram.com') >= 0:
                self.instagram_url = entry

    def report_error(self, url):
        print(f"Empty URL tag error: Artist={artist.name}, {url.text}")


class Urls:
    def __init__(self, facebook, soundcloud, spotify, itunes, twitter):
        self.facebook = facebook
        self.soundcloud = soundcloud
        self.spotify = spotify
        self.itunes = itunes
        self.twitter = twitter


# Load XML file into a tree in memory
print("Beginning parse...")
tree = ET.parse('data/artists.xml')
print("Parse complete")
root = tree.getroot()

# Connect to database
con = p.connect(database='postgres', host='localhost', port=5432)
cur = con.cursor()


failure_counter = 0
success_counter = 0
# Iterate over each artist in db
for entry in root:

    artist = Artist()

    for elem in entry:
        if elem.tag == 'id':
            artist.id = int(elem.text)
        if elem.tag == 'name':
            artist.name = elem.text
        if elem.tag == 'realname':
            artist.realname = elem.text
        if elem.tag == 'profile':
            artist.profile = elem.text
        if elem.tag == 'data_quality':
            artist.data_quality = elem.text
        if elem.tag == 'namevariations':
            for name in elem:
                artist.namevariation.append(name.text)
        if elem.tag == 'urls':
            for url in elem:
                if url.text is not None:
                    artist.urls.append(url.text)

    artist.parse_urls()

    # Attempt the DB insert
    try:
        cur.execute("INSERT INTO artists VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (artist.id,
                                                                                                artist.name,
                                                                                                artist.realname,
                                                                                                artist.profile,
                                                                                                artist.data_quality,
                                                                                                artist.namevariation,
                                                                                                artist.facebook_url,
                                                                                                artist.soundcloud_url,
                                                                                                artist.spotify_url,
                                                                                                artist.twitter_url,
                                                                                                artist.instagram_url))
        con.commit()
        success_counter += 1
    except:
        con.rollback()
        failure_counter += 1

print(f"Successful Inserts: {success_counter}\nFailed Inserts: {failure_counter}")
