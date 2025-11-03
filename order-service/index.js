import { Kafka } from "kafkajs";

const brokers = (
  process.env.KAFKA_BROKERS || "localhost:9094,localhost:9095,localhost:9096"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "order-service",
  brokers,
});

const producer = kafka.producer();

const consumer = kafka.consumer({
  groupId: "order-service",
});

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value.toString();
          const { userId, cart } = JSON.parse(value);

          if (!cart || !Array.isArray(cart)) {
            console.log(
              "[Order] Skipping invalid message: missing or invalid cart"
            );
            return;
          }

          console.log("[User ID] consumed : ", userId, cart.length);
          // TODO Create the order and send to order successful
          const orderId = "123-355-555";
          await producer.send({
            topic: "order-successful",
            messages: [
              {
                value: JSON.stringify({
                  userId,
                  orderId,
                }),
              },
            ],
          });
        } catch (error) {
          console.error("[Order] Error processing message:", error.message);
        }
      },
    });
  } catch (e) {
    console.log("[ORDER] Connect to kafka", e);
  }
};

run();
