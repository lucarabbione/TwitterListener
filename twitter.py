#------------------------------------------------------------------------------
#                         Libraries needed
#------------------------------------------------------------------------------

import json
import pandas
import pyarrow
import pyarrow.parquet as pq
import sys
import tweepy
import times
import os
from console_progressbar import ProgressBar

#------------------------------------------------------------------------------
#                        Global Variables Definition
#------------------------------------------------------------------------------

batch_status = 0 # Variable indicating the amount of tweets stored currently 
tweets_list = [] 
progress_bar = ProgressBar(total=100,prefix= '', suffix= '' , decimals=2, length=50, fill='█', zfill='-')
#
#------------------------------------------------------------------------------
#                              Class definition
#------------------------------------------------------------------------------

class TweetListener(tweepy.streaming.StreamListener):    
    
    def on_data(self, data):
        global batch_status 
        global tweets_list 
        tweets_buffer = []  
                           
        global progress_bar 
        global batch_size 
        
        json_map = json.loads(data) # json will be used as a data format
        
        # RTs will be excluded
        
        cond1 = 'extended_tweet' in json_map and 'RT @' not in json_map['extended_tweet']
        cond2 = 'text' in json_map and 'RT @' not in json_map['text']
        if cond1 or cond2:
        
            # Proceed to store all the desired variables:
            if 'extended_tweet' in json_map:
                tweets_buffer.append(json_map['extended_tweet']['full_text'])
            else:
                if "text" in json_map:
                    tweets_buffer.append(json_map['text'])
                else:
                    tweets_buffer.append('NA')
            
                
            if 'user' in json_map:
                if 'id_str' in json_map['user']:
                    tweets_buffer.append(json_map['user']['id_str'])
                else:
                    tweets_buffer.append('NA')
                if 'location' in json_map['user']:   
                    tweets_buffer.append(json_map['user']['location'])
                else:
                    tweets_buffer.append('NA')
                if 'screen_name' in json_map['user']:
                    tweets_buffer.append(json_map['user']['screen_name'])
                else:
                    tweets_buffer.append('NA')
            else:
                tweets_buffer.append(['NA','NA','NA'])
            
            
            if 'created_at' in json_map:
                tweets_buffer.append(json_map['created_at'])
            else:
                tweets_buffer.append('NA') 
        
            
            if len(tweets_buffer) == 5: 
                batch_status += 1  
                load_bar = (batch_status/int(sys.argv[5]))*100
                progress_bar.print_progress_bar(load_bar)
                tweets_list.append(tweets_buffer)

            
            if batch_status == int(batch_size):
                columns=['text', 'user_id_str', 'user_location', 'screen_name', 'created_at']
                
                tweets_list = pandas.DataFrame(tweets_list, columns = columns)
                frame = pyarrow.Table.from_pandas(tweets_list)
                ahora = str(times.now())
                ahora = ahora.replace(".", "-")
                ahora = ahora.replace(":", "-")
                ahora = ahora.replace(" ", "_")
                pq.write_table(frame, ('tweets_' + brand + '/') + brand + '_' + sys.argv[5] + "_" + ahora + '.parquet')
                raise SystemExit
        else:
            pass 
          
    
    def on_error(self, status):
        print('Something went wrong. Status', status)


#------------------------------------------------------------------------------
#                                 Main
#------------------------------------------------------------------------------

if __name__ == '__main__':
    # Brand represents an optional argument
    if len(sys.argv) < 6 or len(sys.argv) > 7:
        print('Usage: <access_token> <access_token_secret> <consumer_key> <consumer_secret> <batch_size> [brand]')
        sys.exit(1)
    
    
    elif len(sys.argv) == 6:
        access_token = sys.argv[1]
        access_token_secret = sys.argv[2]
        consumer_key = sys.argv[3]
        consumer_secret = sys.argv[4]
        batch_size = int(sys.argv[5])
        brand = "control"
        output_directory = "tweets_" + brand    

        # Create output directory if it does not yet exist
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
    
        # Access to keys
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        
        print("Batch is being generated...") 
        
        api = tweepy.API(auth)
        # Turn on the streaming tool 
        myStream = tweepy.Stream(auth = auth, listener = TweetListener())
        
        tweets = myStream.filter(languages=["en"], track=["a", "the", "i", "you", "u"]) 

       
    else:
        access_token = sys.argv[1]
        access_token_secret = sys.argv[2]
        consumer_key = sys.argv[3]
        consumer_secret = sys.argv[4]
        batch_size = int(sys.argv[5])
        brand = str(sys.argv[6])
        output_directory = "tweets_" + brand

        # Create output directory if it does not yet exist
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)


        # Access to the keys
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        print ("Batch is being generated...")

        api = tweepy.API(auth)
        # Turn on the streaming tool
        myStream = tweepy.Stream(auth = auth, listener = TweetListener())
        tweets = myStream.filter(track = [brand], languages = ["en"])
