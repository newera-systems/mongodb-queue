/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/
import { Db } from 'mongodb';
export type QueueOptions = {
    visibility?: number;
    delay?: number;
    deadQueue?: MongoDBQueue;
    maxRetries?: number;
};
export type AddOptions = {
    delay?: number;
};
export type GetOptions = {
    visibility?: number;
};
export type PingOptions = {
    visibility?: number;
    resetTries?: boolean;
};
export type BaseMessage<T = any> = {
    payload: T;
    visible: string;
};
export type Message<T = any> = BaseMessage<T> & {
    ack: string;
    tries: number;
    deleted?: string;
};
export type ExternalMessage<T = any> = {
    id: string;
    ack: string;
    payload: T;
    tries: number;
};
export declare class MongoDBQueue<T = any> {
    private readonly col;
    private readonly visibility;
    private readonly delay;
    private readonly maxRetries;
    private readonly deadQueue;
    constructor(db: Db, name: string, opts?: QueueOptions);
    createIndexes(): Promise<void>;
    add(payload: T | T[], opts?: AddOptions): Promise<string>;
    get(opts?: GetOptions): Promise<ExternalMessage<T> | null>;
    ping(ack: string, opts?: PingOptions): Promise<string>;
    ack(ack: string): Promise<string>;
    clean(): Promise<void>;
    total(): Promise<number>;
    size(): Promise<number>;
    inFlight(): Promise<number>;
    done(): Promise<number>;
}
