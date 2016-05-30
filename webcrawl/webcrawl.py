import robotparser
import requests
import re
import HTMLParser
from bs4 import BeautifulSoup
import time
from urlparse import urlparse
from urlparse import urljoin
import os
import sys
import json
import io


url = "http://www.mcgill.ca"
filename = "webcrawl.json"
arguments_list = []
arguments_list = sys.argv


if len(arguments_list) > 1:
    url = arguments_list[1]
    
    if url.startswith('www.'):
        url = "http://"+url
        
    if arguments_list[2] == ">":
        filename = arguments_list[3]
    else:
        filename = arguments_list[2]


host_name = ""
dwell_time = 5.0
url_visited = []
next_pages_to_crawl = []
crawled_objects_list = []
start_time = time.time()     
time_limit = 43200 #43200 seconds = 12 hours
count_limit = 5000



sys.setrecursionlimit(10000)
o = urlparse(url)
hostname = o.hostname.replace("www.","")
rp = robotparser.RobotFileParser()
rp.set_url(url+"/robots.txt")
rp.read() 
next_pages_to_crawl.append(url)



def clean_html(html):
    """
    Copied from NLTK package.
    Remove HTML markup from the given string.

    :param html: the HTML string to be cleaned
    :type html: str
    :rtype: str
    """
    # First we remove inline JavaScript/CSS:
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", "", html.strip())
    # Then we remove html comments. This has to be done before removing regular
    # tags since comments can contain '>' characters.
    cleaned = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", cleaned)
    # Next we can remove the remaining tags:
    cleaned = re.sub(r"(?s)<.*?>", " ", cleaned)
    # Finally, we deal with whitespace
    cleaned = re.sub(r"&nbsp;", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    cleaned = re.sub(r"  ", " ", cleaned)
    return HTMLParser.HTMLParser().unescape(cleaned.strip())


def crawl(url,next_pages_to_crawl): 
   
   try:
       print "Pages Crawled: %d   Time elapsed: %f h   Pages to crawl: %d" % (len(crawled_objects_list)+1,round((time.time() - start_time),3)/3600.0,len(next_pages_to_crawl))
       
       if url.startswith('//'):
           url = "http:"+url
       
       url = url.rstrip('/')
       o = urlparse(url)
       ext = urlparse(url).path
       url_is_file = is_file(os.path.splitext(ext)[1])
    
       host_url = ""
       if o.hostname != None:
           host_url = o.hostname.replace("www.","")
       
       if rp.can_fetch("*", url) == True and url not in url_visited and hostname in host_url and url_is_file == False:
           r = requests.get(url)
           
           #crawl url
           crawled_object = {}
           if r.status_code == 200:
               if r.headers['Content-Type'].startswith('text') == True:
                   url_visited.append(url)
                   soup = BeautifulSoup(r.text,'html.parser')
                   links = soup.find_all("a", href=True)
                   parsed_links = []
                   
                   for link in links:        
                       href = str(link['href'].encode('utf8'))
                       parse = urlparse(href)
                       if parse.hostname == None:
                          #print urljoin(url,href)
                          parsed_links.append(str(urljoin(url,href)))
                       else:
                          #print href
                          parsed_links.append(str(href))
                       
                   crawled_object['url'] = url
                   crawled_object['html'] = r.text
                   crawled_object['text'] = str(clean_html(r.text).encode('utf8'))
                   crawled_object['links'] =list(set(parsed_links))
                   crawled_objects_list.append(crawled_object)  
                   next_pages_to_crawl.pop(0)
                   next_pages_to_crawl.extend(list(set(parsed_links)))
                   next_pages_to_crawl = list(set(next_pages_to_crawl))
                   
                   if len(crawled_objects_list) >= count_limit or (time.time() - start_time) >= time_limit or len(next_pages_to_crawl)-1 <= 0:
                       pass
                   else:
                       time.sleep(dwell_time)
                       crawl(next_pages_to_crawl[0],next_pages_to_crawl)
    
               else:
                   #URL content is not text crawl next
                   if len(crawled_objects_list) >= count_limit or (time.time() - start_time) >= time_limit or len(next_pages_to_crawl)-1 <= 0:
                       pass
                   else:
                       next_pages_to_crawl.pop(0)
                       time.sleep(dwell_time)
                       crawl(next_pages_to_crawl[0],next_pages_to_crawl)
                   
           else:
                #print "Server not responding! or Bad URL"
                #crawl next
                   if len(crawled_objects_list) >= count_limit or (time.time() - start_time) >= time_limit or  len(next_pages_to_crawl)-1 <= 0: 
                       pass
                   else:
                       next_pages_to_crawl.pop(0)
                       time.sleep(dwell_time)
                       crawl(next_pages_to_crawl[0],next_pages_to_crawl)
                
       else:
          # robots.txt permission not allowd, crawl next url
          if len(crawled_objects_list) >= count_limit or (time.time() - start_time) >= time_limit or len(next_pages_to_crawl)-1 <= 0:
              pass
          else:
              next_pages_to_crawl.pop(0)
              time.sleep(dwell_time)
              crawl(next_pages_to_crawl[0],next_pages_to_crawl)
   except:
       next_pages_to_crawl.pop(0)
       time.sleep(dwell_time)
       crawl(next_pages_to_crawl[0],next_pages_to_crawl)
                             
   return True    



   
def is_file(extention): # More extensions can be added
    if extention == ".pdf":
        return True
    else:
        return False
    

crawl(next_pages_to_crawl[0],next_pages_to_crawl)

try:
    os.remove('webcrawl.json')
except OSError:
    pass

if len(crawled_objects_list) > 0:
    with io.open(filename, 'a', encoding='utf-8') as f:
        f.write(unicode(json.dumps(crawled_objects_list, ensure_ascii=True, sort_keys=False, indent=4, separators=(',', ': '))))
else:
    print "No page crawled!!!"