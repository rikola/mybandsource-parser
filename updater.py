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
        self.wikipedia_url = None

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
            elif o.netloc.find('wikipedia') >= 0:
                self.wikipedia_url = entry

    def report_error(self, url):
        print(f"Empty URL tag error: Artist={artist.name}, {url.text}")


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
        if elem.tag == 'urls':
            for url in elem:
                if url.text is not None:
                    artist.urls.append(url.text)

    artist.parse_urls()

    if artist.wikipedia_url is not None:

        # Attempt the DB insert
        try:
            data = (artist.wikipedia_url, artist.id)
            SQL = "UPDATE artists SET wikipedia_url=%s WHERE id=%s;"
            cur.execute(SQL, data)
            con.commit()
            success_counter += 1
        except:
            con.rollback()
            failure_counter += 1

print(f"Successful Inserts: {success_counter}\nFailed Inserts: {failure_counter}")
