import subprocess
import sys
import time

# An application server dumping its transaction into Mongo Atlas
dumpToMongo_process = subprocess.Popen(["python", "-m", "application_server.dumpToMongo"])

# Same Mongo Atlas Event are dumped to Kafka brokers using producers, could have used Kafka Connect
mongoProducer_process = subprocess.Popen(["python", "-m", "kafka_io.mongoProducer"])

# Everytime a new event is available in "source" topic, one of the stream processor runs on it and dumps to respective topic
streamProcessor_process = subprocess.Popen(["python","-m","stream_tranformation.stream_processors"])


## Adding timer sleep to kill the background processes, since this is demo.

time.sleep(300)


dumpToMongo_process.kill()
mongoProducer_process.kill()
streamProcessor_process.kill()

