import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher

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
