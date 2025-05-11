"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("@prisma/client");
const kafkajs_1 = require("kafkajs");
const prisma = new client_1.PrismaClient();
const TOPIC_NAME = "zap-events";
const kafka = new kafkajs_1.Kafka({
    clientId: "outbox-processor",
    brokers: ["localhost:9092"],
});
function processOutbox() {
    return __awaiter(this, void 0, void 0, function* () {
        const producer = kafka.producer();
        yield producer.connect();
        while (true) {
            const pendingRows = yield prisma.zapRunOutbox.findMany({
                where: {}, // you can add filters like `processedAt: null` if needed
                take: 10,
            });
            if (pendingRows.length === 0) {
                // No messages to process, avoid hammering the DB
                yield new Promise((r) => setTimeout(r, 1000));
                continue;
            }
            yield producer.send({
                topic: TOPIC_NAME,
                messages: pendingRows.map((r) => ({
                    key: r.id.toString(), // optional, useful for ordering
                    value: r.zapRunId,
                })),
            });
            yield prisma.zapRunOutbox.deleteMany({
                where: {
                    id: {
                        in: pendingRows.map((r) => r.id),
                    },
                },
            });
            // Optional small delay to allow other processes to insert new rows
            yield new Promise((r) => setTimeout(r, 100));
        }
    });
}
processOutbox().catch((err) => {
    console.error("Outbox processor crashed", err);
    process.exit(1);
});
