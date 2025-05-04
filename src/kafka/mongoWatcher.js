const { kafka } = require("./client");

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function init() {
  const consumer = kafka.consumer({ groupId: "delayed-group" });
  await consumer.connect();
  await consumer.subscribe({ topic: "rider-updates", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const msg = JSON.parse(message.value.toString());
      console.log("Delaying message by 5 seconds:", msg);
      await delay(5000);
      console.log("Processed after delay:", msg);
    },
  });
}

init();
