"use strict";
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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MongoDBQueue = void 0;
const crypto_1 = require("crypto");
function id() {
    return (0, crypto_1.randomBytes)(16).toString('hex');
}
function now() {
    return (new Date()).toISOString();
}
function nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000)).toISOString();
}
class MongoDBQueue {
    constructor(db, name, opts = {}) {
        if (!db) {
            throw new Error('mongodb-queue: provide a mongodb.MongoClient.db');
        }
        if (!name) {
            throw new Error('mongodb-queue: provide a queue name');
        }
        this.col = db.collection(name);
        this.visibility = opts.visibility || 30;
        this.delay = opts.delay || 0;
        if (opts.deadQueue) {
            this.deadQueue = opts.deadQueue;
            this.maxRetries = opts.maxRetries || 5;
        }
    }
    createIndexes() {
        return __awaiter(this, void 0, void 0, function* () {
            yield Promise.all([
                this.col.createIndex({ deleted: 1, visible: 1 }),
                this.col.createIndex({ ack: 1 }, { unique: true, sparse: true }),
                this.col.createIndex({ deleted: 1 }, { sparse: true }),
            ]);
        });
    }
    add(payload_1) {
        return __awaiter(this, arguments, void 0, function* (payload, opts = {}) {
            const delay = opts.delay || this.delay;
            const visible = delay ? nowPlusSecs(delay) : now();
            const msgs = [];
            if (payload instanceof Array) {
                if (payload.length === 0) {
                    throw new Error('Queue.add(): Array payload length must be greater than 0');
                }
                payload.forEach(function (payload) {
                    msgs.push({
                        visible: visible,
                        payload: payload,
                    });
                });
            }
            else {
                msgs.push({
                    visible: visible,
                    payload: payload,
                });
            }
            const results = yield this.col.insertMany(msgs, { ignoreUndefined: true });
            if (payload instanceof Array)
                return '' + results.insertedIds;
            return '' + results.insertedIds[0];
        });
    }
    get() {
        return __awaiter(this, arguments, void 0, function* (opts = {}) {
            const visibility = opts.visibility || this.visibility;
            const query = {
                deleted: { $exists: false },
                visible: { $lte: now() },
            };
            const sort = {
                visible: 1,
            };
            const update = {
                $inc: { tries: 1 },
                $set: {
                    ack: id(),
                    visible: nowPlusSecs(visibility),
                },
            };
            const options = {
                sort: sort,
                returnDocument: 'after',
                includeResultMetadata: true,
            };
            const result = yield this.col.findOneAndUpdate(query, update, options);
            const msg = result.value;
            if (!msg)
                return null;
            // convert to an external representation
            const externalMessage = {
                // convert '_id' to an 'id' string
                id: '' + msg._id,
                ack: msg.ack,
                payload: msg.payload,
                tries: msg.tries,
            };
            // check the tries
            if (this.deadQueue && msg.tries > this.maxRetries) {
                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                yield this.deadQueue.add(externalMessage);
                yield this.ack(msg.ack);
                return this.get();
            }
            return externalMessage;
        });
    }
    ping(ack_1) {
        return __awaiter(this, arguments, void 0, function* (ack, opts = {}) {
            const visibility = opts.visibility || this.visibility;
            const query = {
                ack: ack,
                visible: { $gt: now() },
                deleted: { $exists: false },
            };
            const update = {
                $set: {
                    visible: nowPlusSecs(visibility),
                },
            };
            const options = {
                returnDocument: 'after',
                includeResultMetadata: true,
            };
            if (opts.resetTries) {
                update.$set = Object.assign(Object.assign({}, update.$set), { tries: 0 });
            }
            const msg = yield this.col.findOneAndUpdate(query, update, options);
            if (!msg.value) {
                throw new Error('Queue.ping(): Unidentified ack  : ' + ack);
            }
            return '' + msg.value._id;
        });
    }
    ack(ack) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = {
                ack: ack,
                visible: { $gt: now() },
                deleted: { $exists: false },
            };
            const update = {
                $set: {
                    deleted: now(),
                },
            };
            const options = {
                returnDocument: 'after',
                includeResultMetadata: true,
            };
            const msg = yield this.col.findOneAndUpdate(query, update, options);
            if (!msg.value) {
                throw new Error('Queue.ack(): Unidentified ack : ' + ack);
            }
            return '' + msg.value._id;
        });
    }
    clean() {
        return __awaiter(this, void 0, void 0, function* () {
            const query = {
                deleted: { $exists: true },
            };
            yield this.col.deleteMany(query);
        });
    }
    total() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.col.countDocuments();
        });
    }
    size() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.col.countDocuments({
                deleted: { $exists: false },
                visible: { $lte: now() },
            });
        });
    }
    inFlight() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.col.countDocuments({
                ack: { $exists: true },
                visible: { $gt: now() },
                deleted: { $exists: false },
            });
        });
    }
    done() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.col.countDocuments({
                deleted: { $exists: true },
            });
        });
    }
}
exports.MongoDBQueue = MongoDBQueue;
//# sourceMappingURL=mongodb-queue.js.map