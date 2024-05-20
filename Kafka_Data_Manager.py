import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

class KafkaDataManager:
    """
    Class to interacts with Kafka to manage topics and send the simulated streaming data.

    """
    def __init__(self, bootstrap_servers='marina-laptop:9092', client_id='OpenWeather_Data', group_id = 'OpenWeather_group'):
        """
        Initializes the class with specified server and client ID and generates the Kafka producer.
        
        Args:
            bootstrap_servers (str): Address of the Kafka server to connect to.
            client_id (str): Unique identifier for the Kafka client.
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.group_id = group_id
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.json_serializer_for_kafka)
        
    def create_kafka_topics(self, topics):
        """
        Creates Kafka topics if they do not already exist.

        Args:
            topics (list of str): List of topic names to create.
        """
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id=self.client_id)
        existing_kafka_topics = admin_client.list_topics()
        topics_to_create = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics if topic not in existing_kafka_topics]

        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create)
            print(f"Created new topics: {[topic.name for topic in topics_to_create]}")
        else:
            print("No new topics needed to be created.")
        
        print(f"Total of topics generated in kafka server: {admin_client.list_topics()}")
        admin_client.close()

    def send_data_to_kafka(self, topic, data):
        """
        Sends data to a specified Kafka topic.

        Args:
            topic (str): The topic to send data to.
            data (dict): The dictionary with the data to send.
        """
        self.producer.send(topic, value=data)
        self.producer.flush()


    def json_serializer_for_kafka(self, data):
        """
        Serializes data to a JSON format suitable for sending to a Kafka topic. 

        Args:
            topic (str): The Kafka topic to which the data will be sent.
            data (dict): The data being sent.
        """
        return json.dumps(data).encode('utf-8')
    
    def create_kafka_consumer(self, topics):
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=[self.bootstrap_servers],
            auto_offset_reset='earliest',  
            enable_auto_commit=True,       
            group_id=self.group_id,           
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
        )
        return consumer
