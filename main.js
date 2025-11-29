const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kafka-explorer',
  brokers: ['localhost:9092'],
  logLevel: logLevel.DEBUG,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

// Utility to format output
const log = (title, data) => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📍 ${title}`);
  console.log(`${'='.repeat(60)}`);
  console.log(JSON.stringify(data, null, 2));
};

async function createTopic(topicName) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    
    const topics = await admin.listTopics();
    if (topics.includes(topicName)) {
      console.log(`✓ Topic '${topicName}' already exists`);
      return;
    }

    await admin.createTopics({
      topics: [
        {
          topic: topicName,
          numPartitions: 2,
          replicationFactor: 1,
        },
      ],
    });
    console.log(`✓ Created topic '${topicName}'`);
  } catch (error) {
    console.error(`✗ Error managing topics:`, error.message);
  } finally {
    await admin.disconnect();
  }
}

async function produceMessages(topicName, messageCount = 5) {
  try {
    await producer.connect();
    
    const messages = Array.from({ length: messageCount }, (_, i) => ({
      key: `key-${i}`,
      value: JSON.stringify({
        id: i + 1,
        timestamp: new Date().toISOString(),
        message: `Test message #${i + 1}`,
        userId: Math.floor(Math.random() * 100),
      }),
      headers: {
        'correlation-id': `corr-${Date.now()}`,
      },
    }));

    const result = await producer.send({
      topic: topicName,
      messages,
      timeout: 30000,
    });

    log('Produced Messages', {
      topic: topicName,
      messageCount,
      result: result.map(r => ({
        partition: r.partition,
        offset: r.offset,
        errorCode: r.errorCode,
      })),
    });
  } catch (error) {
    console.error(`✗ Producer error:`, error.message);
  } finally {
    await producer.disconnect();
  }
}

async function consumeMessages(topicName, fromBeginning = true) {
  try {
    await consumer.connect();
    
    await consumer.subscribe({
      topic: topicName,
      fromBeginning,
    });

    let messageCount = 0;
    const messages = [];

    const timeout = setTimeout(async () => {
      await consumer.disconnect();
      log('Consumed Messages', {
        topic: topicName,
        messageCount,
        messages: messages.slice(0, 5), // Show first 5
        note: messageCount > 5 ? `... and ${messageCount - 5} more` : '',
      });
      process.exit(0);
    }, 5000); // Listen for 5 seconds

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messageCount++;
        messages.push({
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          value: message.value?.toString(),
          headers: message.headers,
        });
        console.log(`Received message ${messageCount} from partition ${partition}`);
      },
    });
  } catch (error) {
    console.error(`✗ Consumer error:`, error.message);
    process.exit(1);
  }
}

async function getClusterInfo() {
  const admin = kafka.admin();
  try {
    await admin.connect();

    const cluster = await admin.fetchTopicMetadata();
    const brokers = await admin.describeBrokers();

    log('Cluster Information', {
      brokers: brokers.map(b => ({
        id: b.nodeId,
        host: b.host,
        port: b.port,
      })),
      topics: cluster.topics.map(t => ({
        name: t.name,
        partitions: t.partitions.length,
        replicas: t.partitions[0]?.replicas.length,
      })),
    });
  } catch (error) {
    console.error(`✗ Admin error:`, error.message);
  } finally {
    await admin.disconnect();
  }
}

async function main() {
  const topicName = 'test-topic';

  const command = process.argv[2];

  try {
    switch (command) {
      case 'info':
        await getClusterInfo();
        break;
      case 'create':
        await createTopic(topicName);
        break;
      case 'produce':
        await createTopic(topicName);
        await produceMessages(topicName, parseInt(process.argv[3]) || 5);
        break;
      case 'consume':
        await consumeMessages(topicName, true);
        break;
      case 'test':
        await createTopic(topicName);
        await produceMessages(topicName, 5);
        await new Promise(resolve => setTimeout(resolve, 1000));
        await consumeMessages(topicName, true);
        break;
      default:
        console.log(`
Usage: node kafka-test.js <command>

Commands:
  info       - Show cluster and topic information
  create     - Create test topic
  produce    - Produce sample messages
  consume    - Consume messages from beginning
  test       - Full end-to-end test (create → produce → consume)

Examples:
  node kafka-test.js test
  node kafka-test.js produce 10
  node kafka-test.js consume
        `);
    }
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

main();
