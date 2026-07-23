const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
  await producer.connect();
  // ↑ Opens the actual connection to the broker — do this once, reuse it for many sends.

  const events = [
    { key: 'user1', value: 'order created' },
    { key: 'user2', value: 'order shipped' },
    { key: 'user1', value: 'order delivered' },
    { key: 'user3', value: 'order created' },
    { key: 'user2', value: 'order delivered' },
    { key: 'user4', value: 'order created' },
    { key: 'user5', value: 'order shipped' },
    { key: 'user6', value: 'order delivered' },
    { key: 'user7', value: 'order created' },
    { key: 'user8', value: 'order shipped' },
    { key: 'user9', value: 'order delivered' },
    { key: 'user10', value: 'order created' },
    { key: 'user11', value: 'order shipped' },
    { key: 'user12', value: 'order delivered' },
    { key: 'user13', value: 'order created' },
    { key: 'user14', value: 'order shipped' },
    { key: 'user15', value: 'order delivered' }
  ];

  for (const event of events) {
    await sleep(4000);
    await producer.send({
      topic: 'orders',
      messages: [{ key: event.key, value: event.value }]
    });
    console.log(`Sent [${event.key}]: ${event.value}`);
  }

  await producer.disconnect();
}

run();