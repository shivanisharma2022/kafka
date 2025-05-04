const { kafka } = require("../kafka/client");

exports.runDLQRetry = async function () {
  const consumer = kafka.consumer({ groupId: "dlq-retry" });
  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "rider-updates.DLQ", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const msg = message.value.toString();
      await producer.send({
        topic: "rider-updates",
        messages: [{ value: msg }],
      });
      console.log("Retried DLQ Message:", msg);
    },
  });
};
