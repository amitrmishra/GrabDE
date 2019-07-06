from confluent_kafka import Producer

import json
import time
import sys

if __name__ == '__main__':
    if len(sys.argv) != 7:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic> <stream_file> <num_records_in_batch> <secs_to_sleep> <list_key_fields>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    stream_file = sys.argv[3]
    num_records_in_batch = int(sys.argv[4])
    secs_to_sleep = int(sys.argv[5])
    list_key_fields = sys.argv[6].split(',')

    conf = {'bootstrap.servers': broker}

    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka
    with open(stream_file) as infile:
        current_count = 0
        for line in infile:
            try:
                json_message = json.loads(line)
                key = ','.join([str(json_message[key_field]) for key_field in list_key_fields])
                p.produce(topic, line.rstrip(), key, callback=delivery_callback)
            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(p))
            # Serve delivery callback queue.
            p.poll(0)
            current_count += 1
            if current_count >= num_records_in_batch:
                time.sleep(secs_to_sleep)
                current_count = 0

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
