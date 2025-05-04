const { kafka } = require("./client");

async function init() {
  const consumer = kafka.consumer({ groupId: "dlq-viewer" });
  await consumer.connect();
  await consumer.subscribe({ topic: "rider-updates.DLQ", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("DLQ Message:", message.value.toString());
    },
  });
}

init();
