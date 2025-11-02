import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const PORT = process.env.PORT || 8000;

const app = express();

app.use(
  cors({
    origin: "http://localhost:3000",
  })
);

app.use(express.json());

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send("Something broke!");
});

const kafka = new Kafka({
  clientId: "payment-service",
  brokers,
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("[Payment] Producer Connected");
  } catch (e) {
    console.log("Connect to kafka error : ", e);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;

  const userId = "changed-user-1234" + Math.floor(Math.random() * 1000);

  console.log("[Payment] Payment Processed for User : ", userId);

  await producer.send({
    topic: "payment-successful",
    messages: [
      {
        value: JSON.stringify({
          userId,
          cart,
        }),
      },
    ],
  });

  return res.status(200).send("Payment Successful");
});

app.listen(PORT, () => {
  connectToKafka();
  console.log(`Payment Service is running on port ${PORT}`);
});
