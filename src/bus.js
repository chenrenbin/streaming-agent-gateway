// bus.js —— 进程内发布订阅，用于把新事件实时推送给 SSE 订阅者。
// 单订阅者场景下它只是个简单的 fan-out；多订阅者时天然支持（每连接独立游标）。
import { EventEmitter } from 'node:events';

export const bus = new EventEmitter();
bus.setMaxListeners(0); // 一个 run 可能挂多个 SSE 连接
