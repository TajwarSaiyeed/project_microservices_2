import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "analytic-service",
  brokers,
});

const consumer = kafka.consumer({
  groupId: "analytic-service",
});

const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true,
    });

    console.log("[Analytic] Consumer Ready");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case "payment-successful":
            return handlePaymentMessage(message);
          case "order-successful":
            return handleOrderMessage(message);
          case "email-successful":
            return handleEmailMessage(message);
          default:
            console.log("Unknown topic: ", topic);
        }
      },
    });
  } catch (e) {
    console.log("[Analytic] Connect to kafka", e);
  }
};

const handleEmailMessage = async (message) => {
  const value = message.value.toString();
  const { userId, emailId } = JSON.parse(value);

  console.log("[Analytic Consumer] Email sent to : ", userId, emailId);
};

const handleOrderMessage = async (message) => {
  const value = message.value.toString();
  const { userId, orderId } = JSON.parse(value);

  console.log("[Analytic Consumer] Order created for : ", userId, orderId);
};

const handlePaymentMessage = async (message) => {
  try {
    const value = message.value.toString();
    const { userId, cart } = JSON.parse(value);

    const total = cart.reduce((acc, item) => acc + item.price, 0).toFixed(2);

    console.log("[Analytic Consumer] Paid By : ", userId, total);
  } catch (e) {
    console.log("Error in payment analytic consumer", e);
  }
};

run();
