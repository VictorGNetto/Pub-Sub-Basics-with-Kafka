import sys
import threading

from kafka import KafkaProducer, KafkaConsumer
from const import *

if len(sys.argv) < 2:
    print("Uso: >>> python chathub.py Usuario Topico-1 Topico-2 ...")
    sys.exit(1)

def receive_message():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER + ':' + KAFKA_PORT)
    topics = ['direct-' + sys.argv[1]]
    for arg in sys.argv[2:]:
        topics.append('group-' + arg)
    consumer.subscribe(topics)

    for msg in consumer:
        print("\n>>> [{}] {}".format(msg.key.decode(), msg.value.decode()))

thread = threading.Thread(target=receive_message, daemon=True)
thread.start()

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER + ':' + KAFKA_PORT)
while True:
    destType = input("Tipo de destino - (U)suário ou (G)rupo: ").upper()
    if destType != 'U' and destType != 'G':
        print("O tipo de destino deve ser U ou G")
        continue

    dest = input("Destino: ")
    msg = input("Mensagem: ")

    if destType == 'U':
        topic = 'direct-' + dest
    else:  # destType == 'G'
        topic = 'group-' + dest

    producer.send(topic, key=dest.encode(), value=msg.encode())
