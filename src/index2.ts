import express, {Request, Response} from 'express';
import amqp, {Channel, Connection, ConsumeMessage} from 'amqplib';

const app = express();
const PORT = 3001;

const RABBITMQ_URL = 'amqp://gen_user:%3E%5Cp-13tGt4%258eN@80.242.57.39:5672/default_vhost'; // Или URL вашей RabbitMQ
const QUEUE = 'test-1';
const users = [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' }
];
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const startRabbitMQ = async (): Promise<void> => {
    try {
        const connection: Connection = await amqp.connect(RABBITMQ_URL);
        const channel: Channel = await connection.createChannel();

        console.log(`[*] Ожидаем сообщений в очереди: ${QUEUE}`);
        await channel.consume(
            QUEUE,
            async (msg: ConsumeMessage | null) => {
                if (msg !== null) {
                    const messageContent = msg.content.toString();
                    console.log(`[x] Получено сообщение: ${messageContent}`);

                    // Получаем replyTo и correlationId
                    const replyToQueue = msg.properties.replyTo;
                    const correlationId = msg.properties.correlationId;

                    // Преобразуем данные пользователей в строку JSON
                    const responseMessage = JSON.stringify(users);

                    // Синхронная задержка 3 секунды
                    await delay(3000);
                    console.log(`[x] Отправляем ответ: ${responseMessage}`);

                    // Отправляем ответ обратно по указанной replyTo очереди
                    channel.sendToQueue(replyToQueue, Buffer.from(responseMessage), {
                        correlationId: correlationId,
                    });

                    // Подтверждаем обработку сообщения
                    channel.ack(msg);
                }
            },
            {noAck: false}
        );

    } catch (error) {
        console.error('Ошибка при подключении к RabbitMQ:', error);
    }
};

// Стартуем Express сервер
app.listen(PORT, async () => {
    console.log(`Сервер запущен на http://localhost:${PORT}`);
    await startRabbitMQ(); // Запускаем обработку очереди RabbitMQ
});
