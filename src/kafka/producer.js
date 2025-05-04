const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();

  rl.setPrompt(">> ");
  rl.prompt();

  rl.on("line", async (line) => {
    const [riderName, location] = line.split(" ");

    try {
      await producer.send({
        topic: "rider-updates",
        acks: -1, // wait for all in-sync replicas
        messages: [
          {
            key: location, // helps with partitioning
            value: JSON.stringify({ name: riderName, location, timestamp: Date.now() }),
          },
        ],
      });
      console.log("Message sent successfully");
    } catch (err) {
      console.error("Failed to send message", err);
    }

    rl.prompt();
  });
}

init();
