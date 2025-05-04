const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "notification-service",
  brokers: ["192.168.0.189:9092"],
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});
