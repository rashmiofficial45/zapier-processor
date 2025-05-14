// Import PrismaClient to interact with the database.
import { PrismaClient } from "@prisma/client";

// Import Kafka class to produce messages to Kafka topics.
import { Kafka } from "kafkajs";

// Instantiate a PrismaClient for executing DB queries.
const prisma = new PrismaClient();

// Define the Kafka topic to which messages will be sent.
const TOPIC_NAME = "zap-events";

// Create a Kafka instance with:
// - clientId: Identifies this Kafka client in logs/monitoring.
// - brokers: List of Kafka brokers to connect to (localhost in this case).
const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["localhost:9092"],
});

// Main function that continuously polls the outbox table and publishes messages to Kafka.
async function processOutbox() {
  // Create and connect a Kafka producer.
  const producer = kafka.producer();
  await producer.connect();

  // Start an infinite loop to poll the outbox table periodically.
  while (true) {
    /**
     * Fetch pending records from the outbox table (`zapRunOutbox`).
     * - You can add filters like `{ processedAt: null }` to only get unprocessed messages.
     * - `take: 10` limits the batch size to reduce load and improve reliability.
     */
    const pendingRows = await prisma.zapRunOutbox.findMany({
      where: {}, // <-- Add conditions here if needed
      take: 10,
    });

    // If there are no pending messages, wait for 1 second before checking again.
    if (pendingRows.length === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    /**
     * Send the pending outbox records as Kafka messages.
     * - Each message uses the DB `id` as the key (optional, helps with ordering/partitioning).
     * - The `zapRunId` is used as the value payload (assumed to be a string or buffer-compatible).
     */
    await producer.send({
      topic: TOPIC_NAME,
      messages: pendingRows.map((r) => ({
        key: r.id.toString(),
        value: r.zapRunId,
      })),
    });

    /**
     * After successfully publishing messages to Kafka, delete the processed records
     * from the outbox table using their IDs to prevent re-processing.
     */
    await prisma.zapRunOutbox.deleteMany({
      where: {
        id: {
          in: pendingRows.map((r) => r.id),
        },
      },
    });

    // Optional: Wait a short time (100ms) before querying again to reduce DB pressure.
    await new Promise((r) => setTimeout(r, 100));
  }
}

// Start the outbox processing function and handle any uncaught errors.
processOutbox().catch((err) => {
  // Log the error and exit with status code 1 to indicate a crash.
  console.error("Outbox processor crashed", err);
  process.exit(1);
});
