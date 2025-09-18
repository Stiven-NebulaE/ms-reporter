"use strict";

const crypto = require("crypto");
const { of, forkJoin, from, iif, throwError, Subject, interval, EMPTY } = require("rxjs");
const { mergeMap, catchError, map, toArray, pluck, takeUntil, tap, filter, bufferTime } = require('rxjs/operators');

const Event = require("@nebulae/event-store").Event;
const { CqrsResponseHelper } = require('@nebulae/backend-node-tools').cqrs;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const { CustomError, INTERNAL_SERVER_ERROR_CODE, PERMISSION_DENIED } = require("@nebulae/backend-node-tools").error;
const { brokerFactory } = require("@nebulae/backend-node-tools").broker;

const broker = brokerFactory();
const eventSourcing = require("../../tools/event-sourcing").eventSourcing;
const VehicleStatsDA = require("./data-access/VehicleStatsDA");
const MqttService = require('../../services/mqtt-service').getInstance;

const READ_ROLES = ["VEHICLESTATS_READ"];
const WRITE_ROLES = ["VEHICLESTATS_WRITE"];
const REQUIRED_ATTRIBUTES = [];
const MATERIALIZED_VIEW_TOPIC = "emi-gateway-materialized-view-updates";
const VEHICLE_GENERATED_TOPIC = "fleet/vehicles/generated";
const WEBSOCKET_TOPIC = "emi-gateway-websocket-updates";

/**
 * Singleton instance
 * @type { VehicleStatsCRUD }
 */
let instance;

class VehicleStatsCRUD {
  constructor() {
    this.vehicleEventsSubject = new Subject();
    this.mqttService = MqttService();
  }

  /**     
   * Generates and returns an object that defines the CQRS request handlers.
   * 
   * The map is a relationship of: AGGREGATE_TYPE VS { MESSAGE_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
   * 
   * ## Example
   *  { "CreateUser" : { "somegateway.someprotocol.mutation.CreateUser" : {fn: createUser$, instance: classInstance } } }
   */
  generateRequestProcessorMap() {
    return {
      'VehicleStats': {
        "emigateway.graphql.query.ReporterGetFleetStatistics": { fn: instance.getFleetStatistics$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
      }
    }
  };


  /**
   * Gets fleet statistics
   */
  getFleetStatistics$({ root, args, jwt }, authToken) {
    console.log(`ESTE LOG NO getFleetStatistics de CRUD <========`);
    console.log(`ESTE LOG SÍ getFleetStatistics de CRUD <======== AAAAAAAAAAAAAAAA`);

    ConsoleLogger.i(`VehicleStatsCRUD.getFleetStatistics$: START - Getting fleet statistics`);
    // return of({
    //   totalVehicles: 100,
    //   vehiclesByType: {
    //     SUV: 10,
    //     PickUp: 20,
    //     Sedan: 30,
    //     Hatchback: 40,
    //     Coupe: 50
    //   },
    //   vehiclesByDecade: {
    //     decade1980s: 10,
    //     decade1990s: 20,
    //     decade2000s: 30,
    //     decade2010s: 40,
    //     decade2020s: 50
    //   },
    //   vehiclesBySpeedClass: {
    //     Lento: 10,
    //     Normal: 20,
    //     Rapido: 30
    //   },
    //   hpStats: {
    //     min: 100,
    //     max: 1000,
    //     sum: 10000,
    //     count: 100,
    //     avg: 100
    //   },
    //   lastUpdated: new Date().toISOString()
    // })
    // return VehicleStatsDA.getFleetStatistics$()
    return VehicleStatsDA.getFleetStatistics$().pipe(
      tap(stats => ConsoleLogger.i(`VehicleStatsCRUD.getFleetStatistics$: Retrieved stats: ${JSON.stringify(stats)}`)),
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      tap(response => ConsoleLogger.i(`VehicleStatsCRUD.getFleetStatistics$: SUCCESS - Response: ${JSON.stringify(response)}`)),
      catchError(err => {
        console.error(`VehicleStatsCRUD.getFleetStatistics$: ERROR - ${err.message}`);
        return iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err));
      })
    );
  }

  /**
   * Processes vehicle events in batches
   */
  processVehicleEvents$() {
    ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Starting vehicle events processing`);
    
    // Start MQTT subscription
    this.mqttService.start$().subscribe({
      next: (result) => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: MQTT service started: ${result}`),
      error: (error) => ConsoleLogger.e(`VehicleStatsCRUD.processVehicleEvents$: MQTT service error: ${error.message}`)
    });
    
    // Subscribe to MQTT events and process them in batches
    return this.mqttService.getVehicleEventsSubject().pipe(
      tap(event => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Received event from subject: ${JSON.stringify(event)}`)),
      bufferTime(1000), // Buffer events for 1 second
      filter(buffer => buffer.length > 0),
      tap(buffer => ConsoleLogger.i(`VehicleStatsCRUD.processVehicleEvents$: Processing batch of ${buffer.length} events`)),
      mergeMap(events => this.processBatch$(events))
    );
  }

  /**
   * Processes a batch of vehicle events
   */
  processBatch$(events) {
    try {
      console.log(`ESTE LOG SÍ processBatch <========`);

      ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: START - Processing batch of ${events.length} events`);
      
      // Extract all aids from events
      const allAids = events.map(event => event.data?.aid || event.aid);
      
      // Check which aids have been processed before
      return VehicleStatsDA.getProcessedVehicleAids$(allAids).pipe(
        mergeMap(processedAids => {
          // Filter out already processed vehicles
          const newEvents = events.filter(event => {
            const aid = event.data?.aid || event.aid;
            return !processedAids.includes(aid);
          });
          
          ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Filtered to ${newEvents.length} new events (${events.length - newEvents.length} already processed)`);
          
          if (newEvents.length === 0) {
            ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: No new events to process`);
            return of({ success: true });
          }

          ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Processing ${newEvents.length} new vehicles`);

          // Process each new vehicle and update statistics
          return VehicleStatsDA.updateFleetStatistics$(newEvents).pipe(
            tap(() => {
              ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Statistics updated successfully`);
              
              // Mark vehicles as processed
              const newAids = newEvents.map(event => event.data?.aid || event.aid);
              VehicleStatsDA.markVehicleAidsAsProcessed$(newAids).subscribe({
                next: () => ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Marked ${newAids.length} vehicles as processed`),
                error: (error) => console.error(`VehicleStatsCRUD.processBatch$: Error marking vehicles as processed: ${error.message}`)
              });
              
              // Send updated statistics to WebSocket
              VehicleStatsDA.getFleetStatistics$().subscribe(stats => {
                ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Sending stats to WebSocket: ${JSON.stringify(stats)}`);
                ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: WebSocket topic: ${WEBSOCKET_TOPIC}`);
                broker.send$(WEBSOCKET_TOPIC, {
                  type: 'FLEET_STATISTICS_UPDATED',
                  data: stats,
                  timestamp: new Date().toISOString()
                }).subscribe({
                  next: () => ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: Stats sent to WebSocket successfully`),
                  error: (error) => console.error(`VehicleStatsCRUD.processBatch$: WebSocket error: ${error.message}`)
                });
              });
            }),
            tap(() => ConsoleLogger.i(`VehicleStatsCRUD.processBatch$: SUCCESS - Batch processing completed`)),
            map(() => ({ success: true })),
            catchError(error => {
              console.error(`VehicleStatsCRUD.processBatch$: ERROR - ${error.message}`);
              return of({ success: false, error: error.message });
            })
          );
        })
      );
    } catch (error) {
      console.error(`VehicleStatsCRUD.processBatch$: CATCH ERROR - ${error.message}`);
      return of({ success: false, error: error.message });
    }
  }

  /**
   * Emits a vehicle event to the subject
   */
  emitVehicleEvent$(event) {
    ConsoleLogger.i(`VehicleStatsCRUD.emitVehicleEvent$: Received event with aid=${event.aid}`);
    this.vehicleEventsSubject.next(event);
    return of({ success: true });
  }
}

/**
 * @returns {VehicleStatsCRUD}
 */
module.exports = () => {
  if (!instance) {
    instance = new VehicleStatsCRUD();
    ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
  }
  return instance;
};
