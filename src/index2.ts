import express from 'express';
import {trace, context, propagation, SpanKind, Context, SpanStatusCode} from '@opentelemetry/api';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {Resource} from '@opentelemetry/resources';
import amqp, {Channel, ConsumeMessage} from 'amqplib';
import {MongoClient} from 'mongodb';
import dotenv from 'dotenv';
const { SemanticResourceAttributes } = require('@opentelemetry/semantic-conventions');
dotenv.config();


const app = express();
const PORT = process.env.SERVER_2_PORT!;
const RABBITMQ_URL = process.env.RABBITMQ_URL!;
const QUEUE = 'test-1';
const MONGO_URL = process.env.MONGO_URL!;
const DB_NAME = 'default_db';
const COLLECTION_NAME = 'users';

let channel: Channel;
let mongoClient: MongoClient;

// Задержка для имитации долгой обработки
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Функция для подключения к MongoDB и получения данных пользователей с трассировкой и событиями
// Функция для подключения к MongoDB и получения данных пользователей с трассировкой и событиями
const getUsersFromMongoDB = async (span: any): Promise<any> => {
    // Создаем новый спан для MongoDB операций внутри текущего активного контекста
    return context.with(trace.setSpan(context.active(), span), async () => {
        const mongoSpan = trace.getTracer('default').startSpan('mongo-operation', {
            kind: SpanKind.CLIENT,
            attributes: {
                'db.system': 'mongodb',
                'db.name': DB_NAME,
                'db.collection': COLLECTION_NAME
            },
        });

        try {
            mongoSpan.addEvent('Connecting to MongoDB', {'db.name': DB_NAME});
            const db = mongoClient.db(DB_NAME);

            mongoSpan.addEvent('Connected to MongoDB');
            mongoSpan.addEvent('Querying MongoDB', {'db.collection': COLLECTION_NAME, 'query.filter': '{}'});

            const collection = db.collection(COLLECTION_NAME);
            const users = await collection.find({}).toArray();

            mongoSpan.addEvent('Received data from MongoDB', {
                'result.count': users.length,
                'result.data': JSON.stringify(users)
            });
            mongoSpan.setStatus({code: SpanStatusCode.OK});
            return users;
        } catch (error: any) {
            mongoSpan.recordException(error);
            mongoSpan.setStatus({code: SpanStatusCode.ERROR, message: error.message});
            mongoSpan.addEvent('Error during MongoDB operations', {'error.message': error.message});
            return [];
        } finally {
            mongoSpan.end(); // Завершаем спан для MongoDB операций
        }
    });
};

// Функция для оборачивания задержки в контекст
const delayWithTracing = async (ms: number, span: any) => {
    return context.with(trace.setSpan(context.active(), span), async () => {
        const delaySpan = trace.getTracer('default').startSpan('delay-operation');
        delaySpan.addEvent('Starting delay');

        await delay(ms); // Ваш метод задержки

        delaySpan.addEvent('Delay completed');
        delaySpan.end(); // Завершаем спан задержки
    });
};

// Функция для обработки сообщений RabbitMQ
const startRabbitMQ = async () => {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
            if (msg) {
                // Извлекаем контекст трассировки из заголовков сообщения
                const parentContext = propagation.extract(context.active(), msg.properties.headers);

                // Создаем один родительский спан для всех операций внутри извлеченного контекста
                context.with(parentContext, async () => {
                    const span = trace.getTracer('default').startSpan('process-rpc-and-mongo', {
                        kind: SpanKind.SERVER,
                        attributes: {
                            'messaging.system': 'rabbitmq',
                            'rpc.call': 'process-employees',
                            'queue.name': QUEUE
                        },
                    });

                    try {
                        span.addEvent('Received RPC request');

                        // Получаем данные пользователей из MongoDB, создавая отдельный спан для MongoDB операций
                        const users = await getUsersFromMongoDB(span);

                        const replyToQueue = msg.properties.replyTo;
                        const correlationId = msg.properties.correlationId;
                        const responseMessage = JSON.stringify(users);

                        // Выполняем задержку с трассировкой
                        await delayWithTracing(3000, span); // Оборачиваем задержку в контекст и спан

                        // Отправляем ответ в callback-очередь
                        channel.sendToQueue(replyToQueue, Buffer.from(responseMessage), {correlationId});
                        span.addEvent('Sent response to RPC caller');

                        span.setStatus({code: SpanStatusCode.OK});
                    } catch (error: any) {
                        span.recordException(error);
                        span.setStatus({code: SpanStatusCode.ERROR, message: error.message});
                        console.error('Error processing RPC request:', error);
                    } finally {
                        span.end(); // Завершаем общий спан
                    }

                    channel.ack(msg);
                });
            }
        });
    } catch (error) {
        console.error('Ошибка при подключении к RabbitMQ:', error);
    }
};

// Настройка OpenTelemetry
const setupTracing = async () => {
    try {
        const sdk = new NodeSDK({
            traceExporter: new OTLPTraceExporter({url: 'http://localhost:4318/v1/traces'}),
            resource: new Resource({
                [SemanticResourceAttributes.SERVICE_NAME]: 'service-2',  // Задаем имя сервиса корректно
            }),
        });
        sdk.start();
        console.log('Tracing initialized');
    } catch (error) {
        console.log('Error initializing tracing', error);
    }
};

// Вызов функции настройки трассировки


// Подключение к MongoDB и запуск сервера
app.listen(PORT, async () => {
    console.log(`Server is running at http://localhost:${PORT}`);
   await setupTracing();
    try {
        mongoClient = new MongoClient(MONGO_URL);
        await mongoClient.connect();
        console.log('Подключение к MongoDB успешно установлено');
        await startRabbitMQ();
    } catch (error) {
        console.error('Ошибка при подключении к MongoDB:', error);
    }
});
