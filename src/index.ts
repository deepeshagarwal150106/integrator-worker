import { Kafka } from "kafkajs";
import { PrismaClient } from "@prisma/client";
import { JsonObject } from "@prisma/client/runtime/library";
import { parse } from "./parse";
import dotenv from "dotenv";
import { sendEmail } from "./emial";
import { sendSol } from "./solana";
import fs from "fs";
import path from "path";
dotenv.config();

const client = new PrismaClient();

const TOPIC_NAME = "zap-events";

const BROKER = process.env.BROKER || "localhost:9092";
const kafka = new Kafka({
  clientId: "outbox-processor-2",
  brokers: [BROKER],
  ssl: {
    rejectUnauthorized: true, // Recommended for production
    // Read the certificate files from your 'certs' folder
    ca: [fs.readFileSync(path.join(__dirname, 'certs/ca.pem'), 'utf-8')],
    key: fs.readFileSync(path.join(__dirname, 'certs/service.key'), 'utf-8'),
    cert: fs.readFileSync(path.join(__dirname, 'certs/service.cert'), 'utf-8')
  }
});

async function main() {
  const consumer = kafka.consumer({ groupId: "main-worker" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
  const producer = kafka.producer();
  await producer.connect();
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition: partition,
        offset: message.offset,
        value: message.value?.toString(),
      });

      await new Promise((r) => setTimeout(r, 500));

      const data = message.value?.toString();
      if (!data) {
        console.error("No data found in message");
        return;
      }
      const parsedData = JSON.parse(data);
      const zapRunId = parsedData.zapRunId;
      const stage = parsedData.stage;

      const x = await client.zapRun.findUnique({
        where: { id: zapRunId }
      });

      const zapRunDetails = await client.zapRun.findUnique({
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
      const currentAction = zapRunDetails?.zap.actions.find(action => action.sortingOrder === stage);
      if (!currentAction) {
        console.error("No current action found for stage:", stage);
        return;
      }

      const zapRunMetadata = zapRunDetails?.metaData;
      if( currentAction.type.id === "email" ) {
        console.log("Processing email action");
        const body = parse((currentAction.metadata as JsonObject).body as string,zapRunMetadata);
        const to = parse((currentAction.metadata as JsonObject).email as string,zapRunMetadata);
        console.log(`Sending email to ${to} with body: ${body}`);
        await sendEmail(to, body);
      }
      else if (currentAction.type.id === "send-sol") {
        console.log("Processing send-sol action");
        const amount = parse((currentAction.metadata as JsonObject).amount as string, zapRunMetadata);
        const address = parse((currentAction.metadata as JsonObject).address as string, zapRunMetadata);
        console.log(`Sending ${amount} SOL to ${address}`);
        await sendSol(address, amount);
      } else {
        console.error("Unknown action type:", currentAction.type.id);
        return;
      }

      const lastStage = (zapRunDetails?.zap.actions.length || 1) - 1;
      if (stage !== lastStage) {
         await producer.send({
          topic: TOPIC_NAME,
          messages: [
            {
              value: JSON.stringify({
                zapRunId: zapRunId,
                stage: stage + 1,
              }),
              key: message.key?.toString(),
            },
          ],
        });
      }

      await consumer.commitOffsets([
        {
          topic: TOPIC_NAME,
          partition: partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });
}

main();
