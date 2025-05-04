const express = require("express");
const bodyParser = require("body-parser");
const { kafka } = require("./kafka/client");
const { sendNotification } = require("./firebase/sendNotification");
const { runDLQRetry } = require("./dlq/retryDLQMessages");

const app = express();
const PORT = 3000;
app.use(bodyParser.json());

const producer = kafka.producer();
producer.connect();

app.post("/send-notification", async (req, res) => {
  const { name, location, deviceToken, scheduleAt } = req.body;

  const data = { name, location, scheduleAt: scheduleAt ? new Date(scheduleAt).getTime() : Date.now() };

  try {
    if (scheduleAt) {
      await producer.send({
        topic: "rider-updates.delayed",
        messages: [{ value: JSON.stringify(data) }],
      });
      return res.json({ message: "Scheduled message sent to delayed topic." });
    }

    await producer.send({
      topic: "rider-updates",
      messages: [{ value: JSON.stringify(data) }],
    });

    if (deviceToken) {
      await sendNotification(deviceToken, {
        notification: {
          title: `Hello from ${name}`,
          body: `You're in ${location}`,
        },
      });
    }

    res.json({ status: "Notification triggered." });
  } catch (err) {
    res.status(500).json({ error: "Failed to send notification" });
  }
});

app.post("/dlq/retry", async (req, res) => {
  try {
    await runDLQRetry();
    res.json({ message: "DLQ retry started" });
  } catch (err) {
    res.status(500).json({ error: "DLQ retry failed" });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});
