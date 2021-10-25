FROM docker.elastic.co/elasticsearch/elasticsearch:7.14.1
ADD ./elasticsearch.yml /usr/share/elasticsearch/config/
ADD ./synonyms.txt /usr/share/elasticsearch/config/
RUN bin/elasticsearch-plugin install --batch analysis-kuromoji
RUN bin/elasticsearch-plugin install --batch analysis-icu