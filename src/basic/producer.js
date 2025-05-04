const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({  // readline interface to take user input
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  console.log("Producer Connecting...");
  await producer.connect();
  console.log("Producer Connected Successfully");

  rl.setPrompt(">>");
  rl.prompt();

  rl.on("line", async (line) => {
    const [riderName, location] = line.split(" "); // >> Shivani Sharma North

    await producer.send({
      topic: "rider-updates",
      messages: [
        {
          partition: location.toLowerCase() === "north" ? 0 : 1, // can set defualt to 0
          key: "location-update",
          value: JSON.stringify({ name: riderName, location }),
        },
      ],
    });
  }).on("close", async () => {  // when we will close or exit
    console.log("Producer Disconnected Successfully");
    await producer.disconnect();
  });
}

init();
