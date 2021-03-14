docker pull neo4j

docker run \
     --name=neo4j-facts \
     --publish=7474:7474 --publish=7687:7687 \
     --volume=$HOME/neo4j-facts/data:/data \
     --env='NEO4J_AUTH=neo4j/neo' \
     --env='NEO4JLABS_PLUGINS=["n10s"]' \
     neo4j:4.0.7

