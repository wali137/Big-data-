import sys
import json
import networkx as nx
import io
import os



filename = "webcrawl.json"
output_filename = "pagerank.txt"
arguments_list = []
arguments_list = sys.argv
G = nx.DiGraph()

if len(arguments_list) > 1:      
        filename = arguments_list[1]
        
with open(filename) as json_file:
    crawled_json_data = json.load(json_file)

for crawled_page in crawled_json_data:
    url = crawled_page["url"]
    links = crawled_page["links"]
    for link in links:
        G.add_edge(url, link) 
 
pr = nx.pagerank(G)   
d = sorted(pr.items(), key=lambda x:x[1], reverse=True)
 
try:
    os.remove(output_filename)
except OSError:
    pass

with io.open(output_filename, 'a', encoding='utf-8') as f:
    for t in d:
        url = t[0].encode("utf-8")
        rank = str('{0:.20f}'.format(t[1])).encode("utf-8")
        line = url+": "+rank+"\n"
        f.write(line.decode('unicode-escape'))
f.close()
print "File written successfully!"