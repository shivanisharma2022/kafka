const { kafka } = require("./client");
async function init() {
  const admin = kafka.admin();
  console.log("Admin Connecting...");
  admin.connect();
  console.log("Admin Connected Successfully");

  console.log("Creating topic...[rider updates...]");
  await admin.createTopics({
    topics: [
      {
        topic: "rider-updates",
        numPartitions: 2,
      },
    ],
  });

  console.log("[rider-updates] topic created successfully");

  console.log("Disconnecting Admin...");
  await admin.disconnect();
}

init();
