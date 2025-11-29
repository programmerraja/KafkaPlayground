# KafkaPlayground
A repo contain kafka script to playaround with it on local with docker images



docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic new_orders
