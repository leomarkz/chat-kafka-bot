from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('new-message',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

for message in consumer:
    if message.value.get('text') == 'oi':
        response = 'oi pra vc tbm'
    elif message.value.get('text') == 'tudo bem?':
        response = 'estou bem e você?'
    else:
        response = 'Não entendi o que vc quis dizer com \''+message.value.get('text')+'\''      
    producer.send('send-message', value={'text': response})