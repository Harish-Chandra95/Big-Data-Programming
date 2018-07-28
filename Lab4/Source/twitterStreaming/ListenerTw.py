import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time

consumer_key = 'w557kolJzAyXl0l1kfy3bx9qe'
consumer_secret = 'YE8XaOQeaj90wyhkZC9SR645WKr6tpeUYz5w6TiCdRLaJa0rl2'
access_token = '1367004096-QlkbXQgRCfgnhUdGWpFKEiv6OcKspDEv1WLjloT'
access_secret = '6YrZTFWE3KmaexRh84WkU4ZsLfxYsCKEs8oNqZM2vDUy5'


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['cricket'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 1234  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port
    print("Listening on port: %s" % str(port))
    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.
    print("Received request from: " + str(addr))
    time.sleep(5)
    sendData(c)