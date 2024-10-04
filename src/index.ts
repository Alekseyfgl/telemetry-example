import express, { Request, Response } from 'express';
import { trace, context } from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import {Resource} from "@opentelemetry/resources";
import {ATTR_SERVICE_NAME} from "@opentelemetry/semantic-conventions";


process.env.OTEL_LOG_LEVEL = 'debug';

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

const app = express();
const port = 3000;

// Маршрут для главной страницы
app.get('/', (req: Request, res: Response) => {
    const span = trace.getTracer('default').startSpan('title-page');
    console.log('Started span for my action');

    // После выполнения действия
    span.end();
    console.log('Ended span for my action');
    res.status(401).send('Welcome to the main page!');
});

// Маршрут для получения списка пользователей с задержкой в 5 секунд
app.get('/users', (req: Request, res: Response) => {
    const span = trace.getTracer('default').startSpan('users-page');

    const users = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
    ];

    // Задержка перед отправкой ответа на 5 секунд
    setTimeout(() => {
        span.end();
        res.json(users);
    }, 5000);
});

// Запуск сервера
app.listen(port, () => {
    console.log(`Server is running at http://localhost:${port}`);
});
