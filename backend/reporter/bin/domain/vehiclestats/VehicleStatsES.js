'use strict'

const { of , Subject, Observable, concat , throwError , EMPTY} = require("rxjs");
const { tap, mergeMap, bufferTime, filter, catchError, toArray, map, pluck, takeUntil } = require('rxjs/operators');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
/**
 * Singleton instance
 * @type { VehicleStatsES }
 */
let instance;

class VehicleStatsES {

    constructor() {
        this.evtSubject$ = new Subject();
    }


    start$() {
        ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Starting vehicle events processing`);
        
        return new Observable(subscriber => { 
    
              // Subscribe to MQTT events and process them in batches
              //this.mqttService.getVehicleEventsSubject().pipe(
              this.evtSubject$.pipe(
                tap(event => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Received event from subject: ${JSON.stringify(event)}`)),
                bufferTime(1000), // Buffer events for 1 second
                filter(buffer => buffer.length > 0),
                tap(buffer => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Processing batch of ${buffer.length} events`)),
                mergeMap(events => this.processBatch$(events))
              ).subscribe(
                (EVT) => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Event processed: ${JSON.stringify(EVT)}`),
                (err) => ConsoleLogger.e(`VehicleStatsCRUD.processVehicleEvents$: Error processing events: ${err.message}`),
                () => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Completed processing events`)
              );
              subscriber.next('this.mqttService.getVehicleEventsSubject() STARTED');
              subscriber.complete();
        });    
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
        this.evtSubject$.next({ etv, aid, av, data, user, timestamp });
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
    return instance;
};