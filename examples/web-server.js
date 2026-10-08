// web-server.js —— 同时托管 Web 客户端静态页 + SSE 网关（共用一个端口）
//
// 运行：npm run web  →  打开 http://localhost:8080
//
// 设计说明（呼应审查"双通道"）：
//  - GET  /                       → public/index.html（Web 客户端）
//  - GET  /runs/:id/stream        → SSE 事件推送（server→client，原生 EventSource 自动重连）
//  - POST /runs / cancel / pause / resume / retry → 命令通道（client→server）
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { createGateway } from '../src/gateway.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PORT ?? 8080);

createGateway({ port, staticRoot: join(__dirname, '../public') });

console.log(`[web] client  → http://localhost:${port}`);
console.log(`[web] gateway → POST /runs · GET /runs/:id/stream · POST /runs/:id/{cancel,pause,resume,retry}`);
