import { Kafka } from "kafkajs";

const brokers = (
  process.env.KAFKA_BROKERS || "localhost:9094,localhost:9095,localhost:9096"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "kafka-service",
  brokers,
});

const admin = kafka.admin();

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const run = async () => {
  console.log("Connecting to Kafka brokers:", brokers);
  await admin.connect();
  console.log("Connected to Kafka");

  // Wait a bit for cluster to be fully ready
  console.log("Waiting for cluster to be fully ready...");
  await wait(10000);

  let retries = 5;
  while (retries > 0) {
    try {
      console.log("Creating topics...");
      await admin.createTopics({
        topics: [
          {
            topic: "payment-successful",
            numPartitions: 3,
            replicationFactor: 1,
          },
          {
            topic: "order-successful",
            numPartitions: 3,
            replicationFactor: 1,
          },
          {
            topic: "email-successful",
            numPartitions: 3,
            replicationFactor: 1,
          },
        ],
      });
      console.log("Topics created successfully!");
      break;
    } catch (error) {
      retries--;
      console.log(
        `Failed to create topics (${retries} retries left): ${error.message}`
      );
      if (retries === 0) {
        throw error;
      }
      await wait(5000);
    }
  }

  await admin.disconnect();
  console.log("Disconnected from Kafka");
};

run()
  .then(() => {
    console.log("Initialization complete");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Initialization failed:", error);
    process.exit(1);
  });
