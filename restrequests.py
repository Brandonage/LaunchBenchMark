
import requests
r = requests.get('http://adonis-4.grenoble.grid5000.fr:8088/ws/v1/cluster/metrics')

r = requests.get('http://granduc-22.luxembourg.grid5000.fr:18080/api/v1/applications/application_1460638681315_0007/stages/2/0/taskList?length=200')  ## FOr spark history server

r = requests.get('http://parapluie-13.rennes.grid5000.fr:8088/ws/v1/cluster/metrics')  ## For yarn resource manager

r = requests.get('http://griffon-2.nancy.grid5000.fr:8088/ws/v1/cluster/nodes')  ## Give you state of app strated time

r = requests.get('http://paranoia-1.rennes.grid5000.fr:8088/ws/v1/cluster/apps')

r = requests.get('http://granduc-13.luxembourg.grid5000.fr:8088/ws/v1/cluster/metrics')

r = requests.get('http://kwapi.rennes.grid5000.fr:12000/power/timeseries/?from=1474375113&to=1474375210&only=stremi-11,stremi-12')

1474375113
1474375210
r = requests.get('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=1469801080&to=1469801397&only=parapide-10,parapide-11,parapide-12')


granduc-10.luxembourg.grid5000.fr:18080/api/v1


str = json.dumps(r.json()['nodes']['node'])
pd.read_json(str)

r = requests.get('http://jsonplaceholder.typicode.com/posts')

val url = "http://jsonplaceholder.typicode.com/posts"
val result = scala.io.Source.fromURL(url).mkString
println(result)

import re

text = '.lyon.grid5000.fr'

r = requests.get('http://orion-1.lyon.grid5000.fr:8088/ws/v1/cluster/nodes')
listOfNodes = ''

for node in r.json.get('nodes').get('node'):
    listOfNodes = listOfNodes + node.get('nodeHostName') + ','

listOfNodes = listOfNodes.replace('.lyon.grid5000.fr','')[:-1]

r = requests.get('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=1469801080&to=1469801397&only=' + listOfNodes)
global_energy = []
for item in r.json.get('items'):
    global_energy.extend(item.get('values'))

sum(global_energy)/len(global_energy)