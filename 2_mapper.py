#------------------------------------------------------------------------------
#                         Libraries needed
#------------------------------------------------------------------------------

import sys
from pyarrow import parquet as pq
from nltk import word_tokenize
import os

import re
from unicodedata import normalize

from nltk.corpus import stopwords

from nltk.stem import PorterStemmer

import string
from console_progressbar import ProgressBar


progress_bar = ProgressBar(total=100,prefix= '', suffix= '' , decimals=2, length=50, fill='█', zfill='-')

#------------------------------------------------------------------------------
#                         Functions to be used
#------------------------------------------------------------------------------


# Proceed to normalize the words into the tweets
def sanitize(word): 
    
    word = word.lower()
    
    word = re.sub(r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1",normalize( "NFD", word), 0, re.I)   
    word = normalize( 'NFC', word)
    
    lema = PorterStemmer()
    word = lema.stem(word)
    
    no_num_word = ""
    for w in word:
        if w.isdigit() == False:
            no_num_word += w
        word = no_num_word 
        
    no_punct_word = ""
    for w in word:
        if w not in string.punctuation:
            no_punct_word += w
        word = no_punct_word 
    
    if (word > '\uFFFF') or word.startswith('tco') or word.startswith("http"): 
        del word  
    else:
        return word

#------------------------------------------------------------------------------


        
def tokenize(tweet): 
    
    
    tokenized_tweet = word_tokenize(tweet) 
    
   
    for i in range(len(tokenized_tweet)):
        tokenized_tweet[i] = sanitize(tokenized_tweet[i])
        
    return tokenized_tweet
    

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def rework(dictionary):
   
    punct = ['.','!',':','','“','”','’',',','#','$','...','@','?',';',')','-','(','_','%','&','=',',','—', '-','•','°','|','~','+','*','/','{','}','[',']'] 
    for p in punct:
        for key in list(dictionary.keys()):
            if key == p:
                del dictionary[key] 
               
            if key == "https" or key == None:
                del dictionary[key]
            
    for stop_word in set(stopwords.words('english')):
        for key in list(dictionary.keys()):
            if key == stop_word:
                del dictionary[key]
                
    
    for key, value in list(dictionary.items()):
        if value == 1:
            del dictionary[key]           
      
    return dictionary


#------------------------------------------------------------------------------


def update_dictionary(tokenized_tweet, dictionary): 
    
    for tweet_word in tokenized_tweet:
        if tweet_word not in dictionary:
            dictionary[tweet_word] = 0
        dictionary[tweet_word] += 1
    return dictionary


#------------------------------------------------------------------------------
#                             Main
#------------------------------------------------------------------------------
    
if __name__ == '__main__':
    words = {}
    output = sys.argv[2]

    outputfile = open(output, 'w', encoding = 'utf-8')
    brand = 'control'
    if len(sys.argv) == 4:
        brand = sys.argv[3] 
    for inputfile in os.listdir(sys.argv[1]):
        # read parquet inputfile
        table = pq.read_pandas(sys.argv[1] + inputfile, columns = ['text']).to_pandas()
        print ("Processing file: " + inputfile)
        # extracting relevant info
        counter = 0
        for tweet in table['text']:
            
            tokenized_tweet = tokenize(tweet)
            words = update_dictionary(tokenized_tweet, words)
       
            
            # updates progress bar
            counter += 1
            load_bar = (counter/len(table['text']))*100
            progress_bar.print_progress_bar(load_bar)
        rework(words)
         
    
    for key, count in words.items():
        outputfile.write(key + "\t" + brand + "\t" + str(count) + "\n")
    outputfile.close()
