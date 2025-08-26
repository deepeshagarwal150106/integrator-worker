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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const client_1 = require("@prisma/client");
const parse_1 = require("./parse");
const dotenv_1 = __importDefault(require("dotenv"));
const emial_1 = require("./emial");
const solana_1 = require("./solana");
dotenv_1.default.config();
const client = new client_1.PrismaClient();
const TOPIC_NAME = "zap-events";
const kafka = new kafkajs_1.Kafka({
    clientId: "outbox-processor-2",
    brokers: ["localhost: 9092"],
});
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const consumer = kafka.consumer({ groupId: "main-worker" });
        yield consumer.connect();
        yield consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
        const producer = kafka.producer();
        yield producer.connect();
        yield consumer.run({
            autoCommit: false,
            eachMessage: (_a) => __awaiter(this, [_a], void 0, function* ({ topic, partition, message }) {
                var _b, _c, _d;
                console.log({
                    partition: partition,
                    offset: message.offset,
                    value: (_b = message.value) === null || _b === void 0 ? void 0 : _b.toString(),
                });
                yield new Promise((r) => setTimeout(r, 500));
                const data = (_c = message.value) === null || _c === void 0 ? void 0 : _c.toString();
                if (!data) {
                    console.error("No data found in message");
                    return;
                }
                const parsedData = JSON.parse(data);
                const zapRunId = parsedData.zapRunId;
                const stage = parsedData.stage;
                const x = yield client.zapRun.findUnique({
                    where: { id: zapRunId }
                });
                const zapRunDetails = yield client.zapRun.findUnique({
                    where: { id: zapRunId },
                    include: {
                        zap: {
                            include: {
                                trigger: {
                                    include: {
                                        type: true,
                                    },
                                },
                                actions: {
                                    include: {
                                        type: true,
                                    },
                                },
                            },
                        },
                    },
                });
                console.log("processing done");
                const currentAction = zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.zap.actions.find(action => action.sortingOrder === stage);
                if (!currentAction) {
                    console.error("No current action found for stage:", stage);
                    return;
                }
                const zapRunMetadata = zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.metaData;
                if (currentAction.type.id === "email") {
                    console.log("Processing email action");
                    const body = (0, parse_1.parse)(currentAction.metadata.body, zapRunMetadata);
                    const to = (0, parse_1.parse)(currentAction.metadata.email, zapRunMetadata);
                    console.log(`Sending email to ${to} with body: ${body}`);
                    yield (0, emial_1.sendEmail)(to, body);
                }
                else if (currentAction.type.id === "send-sol") {
                    console.log("Processing send-sol action");
                    const amount = (0, parse_1.parse)(currentAction.metadata.amount, zapRunMetadata);
                    const address = (0, parse_1.parse)(currentAction.metadata.address, zapRunMetadata);
                    console.log(`Sending ${amount} SOL to ${address}`);
                    yield (0, solana_1.sendSol)(address, amount);
                }
                else {
                    console.error("Unknown action type:", currentAction.type.id);
                    return;
                }
                const lastStage = ((zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.zap.actions.length) || 1) - 1;
                if (stage !== lastStage) {
                    yield producer.send({
                        topic: TOPIC_NAME,
                        messages: [
                            {
                                value: JSON.stringify({
                                    zapRunId: zapRunId,
                                    stage: stage + 1,
                                }),
                                key: (_d = message.key) === null || _d === void 0 ? void 0 : _d.toString(),
                            },
                        ],
                    });
                }
                yield consumer.commitOffsets([
                    {
                        topic: TOPIC_NAME,
                        partition: partition,
                        offset: (parseInt(message.offset) + 1).toString(),
                    },
                ]);
            }),
        });
    });
}
main();
