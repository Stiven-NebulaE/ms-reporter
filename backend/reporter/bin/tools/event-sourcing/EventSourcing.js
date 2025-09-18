'use strict'

const Rx = require('rxjs');
const nebulaeES = require('@nebulae/event-store');
const Event = nebulaeES.Event;
const EventStore = nebulaeES.EventStore;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const mbeKey = process.env.MICROBACKEND_KEY;

/**
 * @type {EventSourcing}
 */
let instance;

class EventSourcing extends EventStore {

    constructor() {
        ConsoleLogger.i(`EventSourcing: MICROBACKEND_KEY=${process.env.MICROBACKEND_KEY}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_BROKER_TYPE=${process.env.EVENT_STORE_BROKER_TYPE}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_BROKER_EVENTS_TOPIC=${process.env.EVENT_STORE_BROKER_EVENTS_TOPIC}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_BROKER_URL=${process.env.EVENT_STORE_BROKER_URL}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_STORE_TYPE=${process.env.EVENT_STORE_STORE_TYPE}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_STORE_URL=${process.env.EVENT_STORE_STORE_URL}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_STORE_EVENTSTORE_DB_NAME=${process.env.EVENT_STORE_STORE_EVENTSTORE_DB_NAME}`);
        ConsoleLogger.i(`EventSourcing: EVENT_STORE_STORE_AGGREGATES_DB_NAME=${process.env.EVENT_STORE_STORE_AGGREGATES_DB_NAME}`);
        
        super(
            {
                type: process.env.EVENT_STORE_BROKER_TYPE,
                eventsTopic: process.env.EVENT_STORE_BROKER_EVENTS_TOPIC,
                eventsTopicSubscription: `${process.env.EVENT_STORE_BROKER_EVENTS_TOPIC}_${mbeKey}`,
                brokerUrl: process.env.EVENT_STORE_BROKER_URL,
                projectId: process.env.EVENT_STORE_BROKER_PROJECT_ID,
            },
            {
                type: process.env.EVENT_STORE_STORE_TYPE,
                url: process.env.EVENT_STORE_STORE_URL,
                eventStoreDbName: process.env.EVENT_STORE_STORE_EVENTSTORE_DB_NAME,
                aggregatesDbName: process.env.EVENT_STORE_STORE_AGGREGATES_DB_NAME
            }
        );
    }

}

module.exports = 
/**
 * @returns {EventSourcing}
 */
() => {
    if (!instance) {
        instance = new EventSourcing();
        ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};