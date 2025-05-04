const { kafka } = require("./client");

async function init() {
  const consumer = kafka.consumer({ groupId: "stream-processor" });
  await consumer.connect();
  await consumer.subscribe({ topic: "rider-updates", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      if (data.location === "North") {
        console.log("Stream Processing: Alert - North Zone Update", data);
      }
    },
  });
}

init();
