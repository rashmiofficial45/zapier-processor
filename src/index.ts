import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

const prisma = new PrismaClient();
const TOPIC_NAME = "zap-events";
const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["localhost:9092"],
});

async function processOutbox() {
  const producer = kafka.producer();
  await producer.connect();

  while (true) {
    const pendingRows = await prisma.zapRunOutbox.findMany({
      where: {}, // you can add filters like `processedAt: null` if needed
      take: 10,
    });

    if (pendingRows.length === 0) {
      // No messages to process, avoid hammering the DB
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    await producer.send({
      topic: TOPIC_NAME,
      messages: pendingRows.map((r) => ({
        key: r.id.toString(), // optional, useful for ordering
        value: r.zapRunId,
      })),
    });

    await prisma.zapRunOutbox.deleteMany({
      where: {
        id: {
          in: pendingRows.map((r) => r.id),
        },
      },
    });

    // Optional small delay to allow other processes to insert new rows
    await new Promise((r) => setTimeout(r, 100));
  }
}

processOutbox().catch((err) => {
  console.error("Outbox processor crashed", err);
  process.exit(1);
});
