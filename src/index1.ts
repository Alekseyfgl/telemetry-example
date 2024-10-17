import express, { Request, Response } from 'express';
import { trace, context, propagation, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import amqp, { Channel, ConsumeMessage } from 'amqplib';
import { v4 } from 'uuid';

let rabbitChannel: Channel;

const RABBITMQ_URL = 'amqp://gen_user:K%3D%3C%5Cw4vO%40~%24X!4@80.242.57.39:5672/default_vhost';
const QUEUE = 'test-1';
const PORT = 3000;
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Функция для отправки RPC-запроса в очередь RabbitMQ
const sendRpcRequest = async (channel: Channel, queue: string, message: string, spanContext: any): Promise<string> => {
    const correlationId = v4(); // Генерация уникального идентификатора
    const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true }); // Временная очередь для ответа

    return new Promise<string>((resolve, reject) => {
        channel.consume(
            replyQueue,
            (msg: ConsumeMessage | null) => {
                if (msg?.properties.correlationId === correlationId) {
                    resolve(msg.content.toString()); // Возвращаем ответ
                }
            },
            { noAck: true }
        );

        // Включаем контекст span-а в сообщение
        const headers = {};
        propagation.inject(context.active(), headers);

        // Отправляем запрос в очередь с указанием replyTo, correlationId и headers (с контекстом)
        channel.sendToQueue(queue, Buffer.from(message), {
            replyTo: replyQueue,
            correlationId: correlationId,
            headers, // Передаем span контекст через заголовки
        });
    });
};

// Настройка OpenTelemetry
const setupTracing = async () => {
    try {
        const sdk = new NodeSDK({
            traceExporter: new OTLPTraceExporter({ url: `http://localhost:4318/v1/traces` }),
            resource: new Resource({ [ATTR_SERVICE_NAME]: 'service-1' }),
        });

        sdk.start();
        console.log('Tracing initialized');
    } catch (error) {
        console.error('Error initializing tracing', error);
    }
};

// Настраиваем OpenTelemetry
setupTracing();

// Подключение к RabbitMQ
const connectRabbitMQ = async (): Promise<Channel> => {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE, { durable: true });
        console.log(`Queue "${QUEUE}" created successfully`);
        return channel;
    } catch (error) {
        console.error('Failed to connect to RabbitMQ', error);
        process.exit(1);
    }
};

const app = express();

// Маршрут для главной страницы
app.get('/', (req: Request, res: Response) => {
    const span = trace.getTracer('default').startSpan('title-page', {
        kind: SpanKind.SERVER,
        attributes: { 'http.method': req.method, 'http.url': req.url, 'client.ip': req.ip },
    });

    console.log('Started span for main page request');
    span.end();
    res.status(200).send('Welcome to the main page!');
});

// Маршрут для получения списка пользователей с задержкой в 2 секунды
app.get('/users', (req: Request, res: Response) => {
    const parentSpan = trace.getTracer('default').startSpan('users-page', {
        kind: SpanKind.SERVER,
        attributes: { 'http.method': req.method, 'http.url': req.url, 'client.ip': req.ip },
    });
    const parentContext = trace.setSpan(context.active(), parentSpan);

    try {
        parentSpan.addEvent('Start processing request');

        const childSpan1 = trace.getTracer('default').startSpan('fetch-data', {
            kind: SpanKind.CLIENT,
            attributes: { 'db.system': 'mongodb', 'db.statement': 'SELECT * FROM users' },
        }, parentContext);

        const users = [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }];
        childSpan1.end();

        const childSpan2 = trace.getTracer('default').startSpan('process-data', {
            kind: SpanKind.INTERNAL,
            attributes: { 'processing.step': 'filter-users', 'processing.count': users.length },
        }, parentContext);

        setTimeout(() => {
            childSpan2.end();
            parentSpan.end();
            res.json(users);
        }, 2000);
    } catch (error: any) {
        parentSpan.recordException(error);
        parentSpan.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        parentSpan.end();
        res.status(500).send('Something went wrong');
    }
});

// Маршрут для получения списка сотрудников с использованием RPC и спанов
app.get('/employees', async (req: Request, res: Response) => {
    const parentSpan = trace.getTracer('default').startSpan('employees-page', {
        kind: SpanKind.SERVER,
        attributes: { 'http.method': req.method, 'http.url': req.url, 'client.ip': req.ip },
    });
    const parentContext = trace.setSpan(context.active(), parentSpan);

    try {
        parentSpan.addEvent('Start processing employee request');

        const rpcSpan = trace.getTracer('default').startSpan('send-rpc-request', {
            kind: SpanKind.CLIENT,
            attributes: { 'messaging.system': 'rabbitmq', 'messaging.destination': 'test-1', 'rpc.call': 'fetch-employee-data' },
        }, parentContext);

        const messageToSend = JSON.stringify({ message: 'Hello world!' });

        // Отправка запроса и ожидание ответа через RPC
        const rpcResponse = await sendRpcRequest(rabbitChannel, 'test-1', messageToSend, parentContext);

        rpcSpan.end(); // Закрываем спан RPC-запроса

        const employees = JSON.parse(rpcResponse); // Парсим ответ

        const processSpan = trace.getTracer('default').startSpan('process-employee-data', {
            kind: SpanKind.INTERNAL,
            attributes: { 'processing.data_type': 'employee-list', 'processing.data_size': employees.length },
        }, parentContext);

        setTimeout(() => {
            processSpan.end();
            parentSpan.end();
            res.json(employees);
        }, 2000);
    } catch (error: any) {
        parentSpan.recordException(error);
        parentSpan.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        parentSpan.end();
        res.status(500).send('Something went wrong');
    }
});

// Запуск сервера с подключением к RabbitMQ
const startServer = async () => {
    rabbitChannel = await connectRabbitMQ(); // Устанавливаем канал RabbitMQ
    app.listen(PORT, () => {
        console.log(`Server is running at http://localhost:${PORT}`);
    });
};

startServer();
