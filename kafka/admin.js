import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "kafka-service",
  brokers,
});

const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic: "payment-successful",
      },
      {
        topic: "order-successful",
      },
      {
        topic: "email-successful",
      },
    ],
  });
};

run();
