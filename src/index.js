// index.js —— 启动网关（npm start）
import { createGateway } from './gateway.js';

const port = Number(process.env.PORT ?? 3000);
createGateway({ port });
