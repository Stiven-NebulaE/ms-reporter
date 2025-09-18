'use strict'

const { of } = require("rxjs");
const { tap } = require('rxjs/operators');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

/**
 * Singleton instance
 * @type { VehicleStatsES }
 */
let instance;

class VehicleStatsES {

    constructor() {
    }

    start$() {
        ConsoleLogger.i(`VehicleStatsES.start$: Starting vehicle events processing`);
        
        // The VehicleEventsProcessor handles MQTT events directly
        // This ES class is kept for Event Store compatibility if needed
        ConsoleLogger.i(`VehicleStatsES.start$: VehicleEventsProcessor will handle MQTT events`);
        
        return of('VehicleStatsES started - MQTT handled by VehicleEventsProcessor');
    }

    /**     
     * Generates and returns an object that defines the Event-Sourcing events handlers.
     * 
     * The map is a relationship of: AGGREGATE_TYPE VS { EVENT_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
     * 
     * ## Example
     *  { "User" : { "UserAdded" : {fn: handleUserAdded$, instance: classInstance } } }
     */
    generateEventProcessorMap() {
        return {
            'Vehicle': {
                "Generated": { fn: instance.handleVehicleGenerated$, instance, processOnlyOnSync: false },
            }
        }
    };

    /**
     * Handle VehicleGenerated events from Event Store (if any)
     * @param {*} VehicleGeneratedEvent Vehicle Generated Event
     */
    handleVehicleGenerated$({ etv, aid, av, data, user, timestamp }) {
        ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: Event received from Event Store - aid=${aid}`);
        
        // For now, just log the event. The VehicleEventsProcessor handles MQTT events directly
        return of({ success: true }).pipe(
            tap(() => ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: Event logged from Event Store: ${aid}`))
        );
    }
}


/**
 * @returns {VehicleStatsES}
 */
module.exports = () => {
    if (!instance) {
        instance = new VehicleStatsES();
        ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};