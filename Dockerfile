FROM node:20-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install

COPY . .

# Компиляция TypeScript файлов в JavaScript
RUN npx tsc --outDir dist

# Запуск сервера
CMD ["node", "dist/index1.js"]
