const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  const consumer = kafka.consumer({ groupId: group }); // 'user-1'

  console.log("Consumer Connecting...");
  await consumer.connect();
  console.log("Consumer Connected Successfully");

  await consumer.subscribe({ topic: "rider-updates", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log(
            `${group}: [${topic}]: PART:${partition}:`,
            message.value.toString()
          );
    },
  });
}

init();
