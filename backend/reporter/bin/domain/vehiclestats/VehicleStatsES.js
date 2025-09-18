'use strict'

const { of } = require("rxjs");
const { tap, mergeMap } = require('rxjs/operators');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
/**
 * Singleton instance
 * @type { VehicleStatsES }
 */
let instance;

class VehicleStatsES {

    constructor() {
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
                "VehicleGenerated": { fn: instance.handleVehicleGenerated$, instance, processOnlyOnSync: false },
            }
        }
    };

    /**
     * Handle VehicleGenerated events from ms-generator
     * @param {*} VehicleGeneratedEvent Vehicle Generated Event
     */
    handleVehicleGenerated$({ etv, aid, av, data, user, timestamp }) {
        ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: START - aid=${aid}, timestamp=${timestamp}, data=${JSON.stringify(data)}`);
        
        // Emit the event to the CRUD service for batch processing
        const VehicleStatsCRUD = require("./VehicleStatsCRUD");
        const crudInstance = VehicleStatsCRUD();
        
        ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: About to emit event to CRUD service`);
        
        return crudInstance.emitVehicleEvent$({ etv, aid, av, data, user, timestamp }).pipe(
            tap(() => ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: SUCCESS - Event emitted for processing ${aid}`)),
            tap(() => ConsoleLogger.i(`VehicleStatsES.handleVehicleGenerated$: END - aid=${aid}`))
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
    return instance;
};