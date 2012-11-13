require 'rdf'
require 'rdf/ntriples'
require 'neo4j'

NAME = "http://xmlns.com/foaf/0.1/name"
INFLUENCED = "http://dbpedia.org/ontology/influenced"
INFLUENCED_BY = "http://dbpedia.org/ontology/influencedBy"

def connect_nodes(influence, influenced)
  influenced.incoming(:INFLUENCED) << influence
end

nodes = Hash.new  # key: Person name (or URI?), value: node id
links = Hash.new  # key: Inflencer, value: recipient

# Read person entries from .nt file and store as Neo4j nodes.
Neo4j::Transaction.run do |txn|
  RDF::Reader.open(ARGV[0]) do |reader|
    reader.each_statement do |statement|
      predicate = statement.predicate.to_s
      if (predicate == NAME) then
        subject = statement.subject.to_s
        object = statement.object.to_s
        
        unless (nodes.has_key? subject) then
          node = Neo4j::Node.new :name => object
          nodes[subject] = node.getId
        end
      end
    end
  end
end

# Read influence relationships from .nt file and store as Neo4j links.
Neo4j::Transaction.run do |txn|
  RDF::Reader.open(ARGV[1]) do |reader|
    reader.each_statement do |statement|
      predicate = statement.predicate.to_s
      if (predicate == INFLUENCED || predicate == INFLUENCED_BY) then
        subject = statement.subject.to_s
        object = statement.object.to_s

        if (nodes.has_key?(subject) && nodes.has_key?(object)) then
          subject_node = Neo4j::Node.load(nodes[subject])
          object_node = Neo4j::Node.load(nodes[object])
          
          if (predicate == INFLUENCED) then
            connect_nodes(subject_node, object_node)
          elsif (predicate == INFLUENCED_BY)
            connect_nodes(object_node, subject_node)
          end
        end
      end
    end
  end
end

#puts nodes