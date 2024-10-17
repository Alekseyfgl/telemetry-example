import express, {Request, Response} from 'express';
import {trace, context, SpanKind, SpanStatusCode} from '@opentelemetry/api';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {Resource} from "@opentelemetry/resources";
import {ATTR_SERVICE_NAME} from "@opentelemetry/semantic-conventions";
import amqp, {Channel, ConsumeMessage} from 'amqplib';
import {v4} from 'uuid';


process.env.OTEL_LOG_LEVEL = 'debug';
let rabbitChannel: Channel;

// Функция для отправки RPC-запроса в очередь RabbitMQ
const sendRpcRequest = async (channel: Channel, queue: string, message: string) => {
    const correlationId: string = v4(); // Генерация уникального идентификатора
    const {queue: replyQueue} = await channel.assertQueue('', {exclusive: true}); // Временная очередь для ответа

    return new Promise<string>((resolve, reject) => {
        channel.consume(
            replyQueue,
            (msg: ConsumeMessage | null) => {
                if (msg?.properties.correlationId === correlationId) {
                    resolve(msg.content.toString()); // Возвращаем ответ
                }
            },
            {noAck: true}
        );

        // Отправляем запрос в очередь с указанием replyTo и correlationId
        channel.sendToQueue(queue, Buffer.from(message), {
            replyTo: replyQueue,
            correlationId: correlationId,
        });
    });
};


// Асинхронная функция для настройки OpenTelemetry с ожиданием ресурсов
const setupTracing = async () => {
    try {
        const sdk = new NodeSDK({
            traceExporter: new OTLPTraceExporter({
                url: `http://localhost:4318/v1/traces`, // Используйте правильный порт для HTTP
            }),
            resource: new Resource({
                [ATTR_SERVICE_NAME]: 'my-test-project',
            }),
        });

        sdk.start(); // Запуск SDK
        console.log('Tracing initialized');
    } catch (error) {
        console.log('Error initializing tracing', error);
    }
};

// Вызов функции для настройки трассировки
setupTracing();
// Подключение к RabbitMQ

const connectRabbitMQ = async () => {
    try {
        const connection = await amqp.connect('amqp://gen_user:%3E%5Cp-13tGt4%258eN@80.242.57.39:5672/default_vhost'); // Адрес RabbitMQ
        const channel = await connection.createChannel();
        await channel.assertQueue('test-1', {
            durable: true // Очередь сохраняется при перезапуске RabbitMQ
        });
        console.log('Queue "test-1" created successfully');

        return channel; // Возвращаем канал для использования в маршрутах
    } catch (error) {
        console.error('Failed to connect to RabbitMQ', error);
        process.exit(1); // Завершаем приложение в случае ошибки
    }
};

const app = express();
const port = 3000;

// Маршрут для главной страницы
app.get('/', (req: Request, res: Response) => {
    const span = trace.getTracer('default').startSpan('title-page', {
        kind: SpanKind.SERVER, // Тип спана как серверный
        attributes: {
            'http.method': req.method,
            'http.url': req.url,
            'client.ip': req.ip
        }
    });

    console.log('Started span for my action');

    // После выполнения действия
    span.end();
    console.log('Ended span for my action');
    res.status(401).send('Welcome to the main page!');
});

// Маршрут для получения списка пользователей с задержкой в 5 секунд
app.get('/users', (req: Request, res: Response) => {
    const parentSpan = trace.getTracer('default').startSpan('users-page', {
        kind: SpanKind.SERVER,
        attributes: {
            'http.method': req.method,
            'http.url': req.url,
            'client.ip': req.ip
        }
    });
    const parentContext = trace.setSpan(context.active(), parentSpan);

    try {
        parentSpan.addEvent('Start processing request');

        // Первый под-интервал: начало подготовки данных
        const childSpan1 = trace.getTracer('default').startSpan('fetch-data', {
            kind: SpanKind.CLIENT,
            attributes: {
                'db.system': 'mongodb',
                'db.statement': 'SELECT * FROM users',
                'warning.level': 'medium',
                'warning.description': 'Database query took longer than expected'
            }
        }, parentContext);

        // Добавляем событие с атрибутами и временем начала
        const eventStartTime = Date.now() - 1000; // Время начала события на 1 секунду раньше
        childSpan1.addEvent('Warning: Slow query detected', {
            'warning.level': 'medium',
            'warning.description': 'Database query took longer than expected',
            'db.query_time': '2s'
        }, eventStartTime);

        const users = [
            {id: 1, name: 'Alice'},
            {id: 2, name: 'Bob'}
        ];
        childSpan1.end();

        // Второй под-интервал: обработка данных
        const childSpan2 = trace.getTracer('default').startSpan('process-data', {
            kind: SpanKind.INTERNAL,
            attributes: {
                'processing.step': 'filter-users',
                'processing.count': users.length
            }
        }, parentContext);

        setTimeout(() => {
            childSpan2.end();
            parentSpan.end();
            res.json(users);
        }, 2000);

    } catch (error: any) {
        parentSpan.recordException(error as any);
        parentSpan.setStatus({code: SpanStatusCode.ERROR, message: error.message});
        parentSpan.addEvent('Error occurred during request');
        parentSpan.end();
        res.status(500).send('Something went wrong');
    }
});


// Новый маршрут для получения списка сотрудников с использованием RPC и спанов
app.get('/employees', async (req: Request, res: Response) => {
    const parentSpan = trace.getTracer('default').startSpan('employees-page', {
        kind: SpanKind.SERVER,
        attributes: {
            'http.method': req.method,
            'http.url': req.url,
            'client.ip': req.ip
        }
    });
    const parentContext = trace.setSpan(context.active(), parentSpan);

    try {
        parentSpan.addEvent('Start processing employee request');

        // Под-интервал: отправка RPC-запроса в очередь RabbitMQ
        const rpcSpan = trace.getTracer('default').startSpan('send-rpc-request', {
            kind: SpanKind.CLIENT,
            attributes: {
                'messaging.system': 'rabbitmq',
                'messaging.destination': 'test-1',
                'rpc.call': 'fetch-employee-data'
            }
        }, parentContext);

        const messageToSend:string = JSON.stringify({ message: 'Hello world!' });

        // Отправка запроса и ожидание ответа через RPC
        const rpcResponse:string = await sendRpcRequest(rabbitChannel, 'test-1', messageToSend);

        rpcSpan.end(); // Закрываем спан RPC-запроса

        const employees = JSON.parse(rpcResponse); // Парсим ответ

        // Под-интервал: обработка полученных данных
        const processSpan = trace.getTracer('default').startSpan('process-employee-data', {
            kind: SpanKind.INTERNAL,
            attributes: {
                'processing.data_type': 'employee-list',
                'processing.data_size': employees.length
            }
        }, parentContext);

        // Имитация обработки данных
        setTimeout(() => {
            processSpan.end();
            parentSpan.end();
            res.json(employees);
        }, 1000);

    } catch (error: any) {
        parentSpan.recordException(error as any);
        parentSpan.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        parentSpan.addEvent('Error occurred during employee request');
        parentSpan.end();
        res.status(500).send('Something went wrong');
    }
});

app.get('/employees-exception', async (req: Request, res: Response) => {
    const parentSpan = trace.getTracer('default').startSpan('employees-page', {
        kind: SpanKind.SERVER,
        attributes: {
            'http.method': req.method,
            'http.url': req.url,
            'client.ip': req.ip
        }
    });
    const parentContext = trace.setSpan(context.active(), parentSpan);

    try {
        parentSpan.addEvent('Start processing employee request');

        // Под-интервал: отправка RPC-запроса в очередь RabbitMQ
        const rpcSpan = trace.getTracer('default').startSpan('send-rpc-request', {
            kind: SpanKind.CLIENT,
            attributes: {
                'messaging.system': 'rabbitmq',
                'messaging.destination': 'test-1',
                'rpc.call': 'fetch-employee-data'
            }
        }, parentContext);

        const messageToSend:string = JSON.stringify({ message: 'Hello world!' });

        // Отправка запроса и ожидание ответа через RPC
        const rpcResponse:string = await sendRpcRequest(rabbitChannel, 'test-1', messageToSend);
        throw new Error('Something went wrong');

        rpcSpan.end(); // Закрываем спан RPC-запроса

        const employees = JSON.parse(rpcResponse); // Парсим ответ

        // Под-интервал: обработка полученных данных
        const processSpan = trace.getTracer('default').startSpan('process-employee-data', {
            kind: SpanKind.INTERNAL,
            attributes: {
                'processing.data_type': 'employee-list',
                'processing.data_size': employees.length
            }
        }, parentContext);

        // Имитация обработки данных
        setTimeout(() => {
            processSpan.end();
            parentSpan.end();
            res.json(employees);
        }, 1000);

    } catch (error: any) {
        parentSpan.recordException(error as any);
        parentSpan.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        parentSpan.addEvent('Error occurred during employee request');
        parentSpan.end();
        res.status(500).send('Something went wrong');
    }
});

// Запуск сервера с подключением к RabbitMQ
const startServer = async () => {
    rabbitChannel = await connectRabbitMQ(); // Устанавливаем канал RabbitMQ
    app.listen(port, () => {
        console.log(`Server is running at http://localhost:${port}`);
    });
};

startServer();
