import pandas as pd
import docker
import time
from py2neo import Graph, Node, Relationship, NodeMatcher

##############################
### ENSURE NEO4J IS RUNNING  #
##############################

client = docker.from_env()
if 'neo4j-facts' in [cont.name for cont in client.containers.list(all=True)]:
    container = client.containers.get('neo4j-facts')
elif input('Container not found : press y if you want to create it\n')=='y' :
    container = client.containers.run('neo4j:4.0.7',
                                      name='neo4j-facts',
                                      ports={'7474/tcp': 7474, '7687/tcp': 7687},
                                      volumes={'/home/martin/neo4j-facts/data': {'bind': '/data', 'mode': 'rw'}},
                                      environment={'NEO4J_AUTH':'neo4j/neo', 'NEO4JLABS_PLUGINS':'["n10s"]'})
else :
    raise Exception('Docker container not found')
    
if container.status != "running":
    print('Starting containers..')
    container.start()
    time.sleep(30)
    
    
class FactGraph():
        
    def __init__(self, *args, **kwargs):
        self.graph = Graph(*args, **kwargs)
        self.matcher = NodeMatcher(self.graph)
        
    def reset(self):
        self.graph.run('match (a) -[r] -> () delete a, r')
        self.graph.run('match (a) delete a')
        
    def set_source(self, path):
        self.source = pd.read_csv('/home/martin/data/fact.csv', sep=',', header=None)
        
    def parse_topics(self):
        # Parse source csv file
        topics = {}
        for topic in set([a.upper() for b in list(map(lambda lbls : lbls.split(', '), self.source.iloc[:,1])) for a in b]):
            topics[topic] = Node("TOPIC", name=topic)
        
        ## todo : topic modeling
        self.topics = topics
        
        # Create nodes
        for topic in topics:
            self.graph.create(topics[topic])

        return topics.keys()
    
    def create_notes(self):
        def create_note(line, graph, topics):
            note = Node("NOTE", name=line.iloc[0])
            rels = [Relationship(note, "about", topics[topic.upper()]) for topic in line.iloc[1].split(', ')]
            for rel in rels:
                graph.create(rel)
            return note
        return self.source.apply(create_note, axis=1, topics=self.topics, graph=self.graph)
