########################## VERY IMPORTANT #####################################
#ElasticSearch must be initialized from console before running this code
#Connection to localhost needs to be confirmed to proceed

#------------------------------------------------------------------------------
#                         Libraries needed
#------------------------------------------------------------------------------
 
import sys
from elasticsearch import Elasticsearch

#------------------------------------------------------------------------------
#                        Object definition
#------------------------------------------------------------------------------

es = Elasticsearch([{'host':'localhost','port':9200}]) 
last_key = None 
running_totals = {} # This is the dictionary that will show the key:value

#------------------------------------------------------------------------------
#                        Function definition
#------------------------------------------------------------------------------

def process_term(term):
   print(term)
   print(running_totals)
   document={
      'word':term,
      'brand':brand,
      'count':running_totals[brand]
   }
   es.index(index = 'tech-data-and-the-innovation-mindset', doc_type = 'word_counter',  body = document) 
   

def collect(brand, count):
   if brand in running_totals:
      running_totals[brand] += count
   else:
      running_totals[brand] = count

#------------------------------------------------------------------------------
#                            Main
#------------------------------------------------------------------------------
      
if __name__ == '__main__':
   inputfile = open(str(sys.argv[1]), 'r', encoding = 'utf-8')
   for input_line in inputfile:
      input_line = input_line.strip()
      this_key, brand, count = input_line.split("\t", 2)
      count = int(count)

      if last_key == this_key:
         collect(brand, count)
      else:
         if last_key:
            process_term(last_key)
         running_totals = {}
         collect(brand, count) 
         last_key = this_key
   if last_key == this_key:
      process_term(last_key)
