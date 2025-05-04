const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();
  await admin.connect();

  await admin.createTopics({
    topics: [
      {
        topic: "rider-updates",
        numPartitions: 3,
        replicationFactor: 1,
      },
      {
        topic: "rider-updates-dlq",
        numPartitions: 1,
      },
    ],
  });

  console.log("Topics created successfully");
  await admin.disconnect();
}

init();
