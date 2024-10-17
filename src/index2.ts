import express from 'express';
import { trace, context, propagation, SpanKind } from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import amqp, { Channel, ConsumeMessage } from 'amqplib';

const app = express();
const PORT = 3001;
const RABBITMQ_URL = 'amqp://gen_user:%3E%5Cp-13tGt4%258eN@80.242.57.39:5672/default_vhost'; // Замените на ваш URL RabbitMQ
const QUEUE = 'test-1';
const users = [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
];

// Задержка для имитации долгой обработки
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

let channel: Channel;

// Функция для обработки сообщений RabbitMQ
const startRabbitMQ = async () => {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        // Настраиваем потребление сообщений из очереди
        await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
            if (msg) {
                // Извлекаем контекст трассировки из заголовков сообщения
                const parentContext = propagation.extract(context.active(), msg.properties.headers);

                // Создаем спан для обработки RPC-запроса
                const parentSpan = trace.getTracer('default').startSpan('process-rpc-request', {
                    kind: SpanKind.SERVER,
                    attributes: { 'messaging.system': 'rabbitmq', 'rpc.call': 'process-employees' },
                }, parentContext);

                // Получаем данные для отправки ответа
                const replyToQueue = msg.properties.replyTo;
                const correlationId = msg.properties.correlationId;
                const responseMessage = JSON.stringify(users);

                // Имитация задержки обработки
                await delay(3000);

                // Отправляем ответ в callback-очередь
                channel.sendToQueue(replyToQueue, Buffer.from(responseMessage), { correlationId });

                // Завершаем спан
                parentSpan.end();

                // Подтверждаем обработку сообщения
                channel.ack(msg);
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
            traceExporter: new OTLPTraceExporter({ url: 'http://localhost:4318/v1/traces' }), // URL OpenTelemetry Collector
            resource: new Resource({ [ATTR_SERVICE_NAME]: 'service-2' }), // Имя сервиса
        });
        sdk.start();
        console.log('Tracing initialized');
    } catch (error) {
        console.log('Error initializing tracing', error);
    }
};

// Вызов функции настройки трассировки
setupTracing();

// Запуск сервера и настройка подключения к RabbitMQ
app.listen(PORT, async () => {
    console.log(`Server is running at http://localhost:${PORT}`);
    await startRabbitMQ();
});
