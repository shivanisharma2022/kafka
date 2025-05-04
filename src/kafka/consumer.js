const { kafka } = require("./client");
const groupId = process.argv[2] || "default-group";
const producer = kafka.producer();

async function init() {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "rider-updates", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = message.value.toString();
      console.log(`${groupId}: [${topic}]: PART:${partition}:`, msg);

      try {
        // Simulate processing
        if (msg.includes("fail")) throw new Error("Processing failed");

        // Push to Firebase or some service
        console.log("Processed successfully:", msg);
      } catch (err) {
        console.error("Error processing message:", msg);
        await producer.send({
          topic: "rider-updates-dlq",
          messages: [{ value: msg }],
        });
      }
    },
  });
}

init();
