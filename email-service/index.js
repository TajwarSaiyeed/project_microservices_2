import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "email-service",
  brokers,
});

const producer = kafka.producer();

const consumer = kafka.consumer({
  groupId: "email-service",
});

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "order-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId } = JSON.parse(value);

        console.log("[User ID] consumed : ", userId);

        // send email to the user
        const emailId = "1Yu$3287846";

        await producer.send({
          topic: "email-successful",
          messages: [
            {
              value: JSON.stringify({
                userId,
                emailId,
              }),
            },
          ],
        });
      },
    });
  } catch (e) {
    console.log("[Email] Connect to kafka", e);
  }
};

run();
