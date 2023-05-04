import confluent_kafka

# Structs
client_properties = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "my-user",
    "sasl.password": "my-password",
}
topics = [
    {
        "name": "topic1",
        "num_partitions": 3,
        "replication_factor": 1,
    },
    {
        "name": "topic2",
        "num_partitions": 2,
        "replication_factor": 2,
    },
]
acls = [
    {
        "principal": "User:alice",
        "operation": "read",
        "host": "*",
        "topic": "topic1",
        "permission": "allow",
    },
    {
        "principal": "User:bob",
        "operation": "write",
        "host": "*",
        "topic": "topic2",
        "permission": "allow",
    },
    {
        "principal": "User:carol",
        "operation": "read,write",
        "host": "*",
        "topic": "topic3",
        "permission": "allow",
    },
]
connectors = [
    {
        "name": "my-connector1",
        "connector.class": "FileStreamSource",
        "file": "/path/to/file1",
        "topic": "my-topic1",
    },
    {
        "name": "my-connector2",
        "connector.class": "FileStreamSource",
        "file": "/path/to/file2",
        "topic": "my-topic2",
    },
]

# create the client
client = confluent_kafka.AdminClient(client_properties)

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
fs = client.create_topics(topics, idempotent=True)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))

# Add the acls to the client
for acl in acls:
    client.create_acl(acl, idempotent=True)

# Add the connectors to the client
for connector in connectors:
    client.create_connector(connector, idempotent=True)

# Close the client
client.close()
