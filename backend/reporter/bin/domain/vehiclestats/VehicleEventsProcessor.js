'use strict'

const { Subject, from } = require('rxjs');
const { bufferTime, filter, mergeMap, tap, catchError } = require('rxjs/operators');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const crypto = require('crypto');

const VehicleStatsDA = require('./data-access/VehicleStatsDA');

/**
 * Singleton instance
 * @type { VehicleEventsProcessor }
 */
let instance;

class VehicleEventsProcessor {
    constructor() {
        this.events$ = new Subject();
        this.broker = brokerFactory();
        this.isProcessing = false;
    }

    /**
     * Gets decade from year
     * @param {number} year
     * @returns {string}
     */
    getDecade(year) {
        if (year >= 1980 && year < 1990) return "decade1980s";
        if (year >= 1990 && year < 2000) return "decade1990s";
        if (year >= 2000 && year < 2010) return "decade2000s";
        if (year >= 2010 && year < 2020) return "decade2010s";
        if (year >= 2020) return "decade2020s";
        return "decade1980s";
    }

    /**
     * Generates a stable hash from vehicle data for idempotency
     * @param {Object} vehicleData
     * @returns {string}
     */
    generateAidFromVehicleData(vehicleData) {
        const stableStringify = (obj) => {
            if (obj === null || typeof obj !== 'object') { return JSON.stringify(obj); }
            if (Array.isArray(obj)) {
                return '[' + obj.map(item => stableStringify(item)).join(',') + ']';
            }
            const keys = Object.keys(obj).sort();
            const keyValues = keys.map(k => `${JSON.stringify(k)}:${stableStringify(obj[k])}`);
            return '{' + keyValues.join(',') + '}';
        };
        const payload = stableStringify(vehicleData || {});
        return crypto.createHash('sha256').update(payload).digest('hex');
    }

    start$() {
        ConsoleLogger.i('VehicleEventsProcessor: Starting vehicle events processing...');
        
        // Configure MQTT subscription
        this.broker.configMessageListener$([process.env.MQTT_TOPIC_GENERATED || 'fleet/vehicles/generated'])
            .pipe(
                tap(topics => ConsoleLogger.i(`VehicleEventsProcessor: Subscribed to topics: ${JSON.stringify(topics)}`)),
                catchError(error => {
                    ConsoleLogger.e('VehicleEventsProcessor: Error subscribing to MQTT topics', error);
                    return from([]);
                })
            )
            .subscribe(
                topics => ConsoleLogger.i(`VehicleEventsProcessor: Successfully subscribed to ${topics.length} topics`),
                error => ConsoleLogger.e('VehicleEventsProcessor: Error in MQTT subscription', error),
                () => ConsoleLogger.i('VehicleEventsProcessor: MQTT subscription completed')
            );

        // Subscribe to incoming messages
        this.broker.incomingMessages$
            .pipe(
                filter(message => message && message.topic === (process.env.MQTT_TOPIC_GENERATED || 'fleet/vehicles/generated')),
                tap(event => ConsoleLogger.i(`VehicleEventsProcessor: Received event: ${JSON.stringify(event)}`)),
                catchError(error => {
                    ConsoleLogger.e('VehicleEventsProcessor: Error receiving MQTT events', error);
                    return from([]);
                })
            )
            .subscribe(
                event => {
                    const envelope = event && event.data ? event.data : event;
                    if (!envelope) { return; }
                    if (!envelope.aid && envelope.data) {
                        envelope.aid = this.generateAidFromVehicleData(envelope.data);
                    }
                    this.events$.next(envelope);
                },
                error => ConsoleLogger.e('VehicleEventsProcessor: Error in MQTT message processing', error),
                () => ConsoleLogger.i('VehicleEventsProcessor: MQTT message processing completed')
            );

        // Configure batch processing pipeline
        this.events$
            .pipe(
                bufferTime(1000), // Buffer of 1 second
                filter(batch => batch.length > 0),
                tap(batch => ConsoleLogger.i(`VehicleEventsProcessor: Processing batch of ${batch.length} events`))
            )
            .subscribe(
                async (batch) => {
                    if (this.isProcessing) {
                        ConsoleLogger.w('VehicleEventsProcessor: Previous batch still processing, skipping...');
                        return;
                    }
                    
                    this.isProcessing = true;
                    try {
                        await this.processBatch$(batch);
                    } catch (error) {
                        ConsoleLogger.e('VehicleEventsProcessor: Error processing batch', error);
                    } finally {
                        this.isProcessing = false;
                    }
                },
                error => ConsoleLogger.e('VehicleEventsProcessor: Error in batch processing', error)
            );

        // Return Observable that completes immediately
        return from([{ message: 'VehicleEventsProcessor started successfully' }]);
    }

    /**
     * Processes a batch of events
     * @param {Array} batch - Batch of events to process
     */
    async processBatch$(batch) {
        ConsoleLogger.i(`VehicleEventsProcessor: Processing batch of ${batch.length} events`);

        // 1. Extract unique aids from batch
        const aids = batch.map(event => event.aid).filter(aid => aid);
        
        if (aids.length === 0) {
            ConsoleLogger.w('VehicleEventsProcessor: No valid aids in batch, skipping...');
            return;
        }

        // 2. Check idempotency - get already processed aids
        const processedAids = await VehicleStatsDA.getProcessedVehicleAids$(aids).toPromise();
        const processedAidsSet = new Set(processedAids);

        // 3. Filter fresh events (not processed)
        const freshEvents = batch.filter(event => 
            event.aid && !processedAidsSet.has(event.aid)
        );

        if (freshEvents.length === 0) {
            ConsoleLogger.i('VehicleEventsProcessor: No fresh events to process, skipping...');
            return;
        }

        ConsoleLogger.i(`VehicleEventsProcessor: Processing ${freshEvents.length} fresh events out of ${batch.length} total`);

        // 4. Process fresh events
        await this.processFreshEvents$(freshEvents);
    }

    /**
     * Processes fresh events and updates statistics
     * @param {Array} freshEvents - Fresh events to process
     */
    async processFreshEvents$(freshEvents) {
        // 5. Derive fields and build accumulators
        const batchStats = this.calculateBatchStats(freshEvents);

        // 6. Update statistics in MongoDB
        const updatedStats = await VehicleStatsDA.updateFleetStatistics$(batchStats).toPromise();

        // 7. Insert processed aids
        const freshAids = freshEvents.map(event => event.aid);
        await VehicleStatsDA.insertProcessedVehicleAids$(freshAids).toPromise();

        // 8. Notify via WebSocket
        await this.notifyWebSocket$(updatedStats);

        ConsoleLogger.i(`VehicleEventsProcessor: Successfully processed ${freshEvents.length} events`);
    }

    /**
     * Calculates batch statistics
     * @param {Array} events - Events to process
     * @returns {Object} Batch statistics
     */
    calculateBatchStats(events) {
        const stats = {
            totalVehicles: events.length,
            vehiclesByType: {
                SUV: 0,
                PickUp: 0,
                Sedan: 0,
                Hatchback: 0,
                Coupe: 0
            },
            vehiclesByDecade: {
                decade1980s: 0,
                decade1990s: 0,
                decade2000s: 0,
                decade2010s: 0,
                decade2020s: 0
            },
            vehiclesBySpeedClass: {
                Lento: 0,
                Normal: 0,
                Rapido: 0
            },
            hpStats: {
                sum: 0,
                count: events.length,
                min: Infinity,
                max: -Infinity
            }
        };

        events.forEach(event => {
            const { data } = event;
            if (!data) return;

            const { type, powerSource, hp, year, topSpeed } = data;

            // Vehicles by type
            if (type && stats.vehiclesByType[type] !== undefined) {
                stats.vehiclesByType[type]++;
            }

            // Vehicles by decade
            if (year) {
                const decade = this.getDecade(year);
                if (stats.vehiclesByDecade[decade] !== undefined) {
                    stats.vehiclesByDecade[decade]++;
                }
            }

            // Speed classification
            if (topSpeed) {
                let speedClass;
                if (topSpeed < 140) speedClass = 'Lento';
                else if (topSpeed <= 240) speedClass = 'Normal';
                else speedClass = 'Rapido';
                
                if (stats.vehiclesBySpeedClass[speedClass] !== undefined) {
                    stats.vehiclesBySpeedClass[speedClass]++;
                }
            }

            // HP statistics
            if (hp && typeof hp === 'number') {
                stats.hpStats.sum += hp;
                stats.hpStats.min = Math.min(stats.hpStats.min, hp);
                stats.hpStats.max = Math.max(stats.hpStats.max, hp);
                ConsoleLogger.i(`VehicleEventsProcessor: HP stats updated - HP: ${hp}, Sum: ${stats.hpStats.sum}, Min: ${stats.hpStats.min}, Max: ${stats.hpStats.max}`);
            } else {
                ConsoleLogger.w(`VehicleEventsProcessor: Invalid HP value - HP: ${hp}, Type: ${typeof hp}`);
            }
        });

        // Clean infinite values
        if (stats.hpStats.min === Infinity) stats.hpStats.min = null;
        if (stats.hpStats.max === -Infinity) stats.hpStats.max = null;

        return stats;
    }

    /**
     * Notifies changes via WebSocket
     * @param {Object} stats - Updated statistics
     */
    async notifyWebSocket$(stats) {
        try {
            // Send notification via broker for WebSocket
            await this.broker.send$(
                'emi-gateway-materialized-view-updates',
                'FLEET_STATISTICS_UPDATED',
                stats
            ).toPromise();
            ConsoleLogger.i('VehicleEventsProcessor: WebSocket notification sent');
        } catch (error) {
            ConsoleLogger.e('VehicleEventsProcessor: Error sending WebSocket notification', error);
        }
    }

    /**
     * Stops processing
     */
    stop$() {
        ConsoleLogger.i('VehicleEventsProcessor: Stopping...');
        this.events$.complete();
        return from([{ message: 'VehicleEventsProcessor stopped' }]);
    }
}

/**
 * @returns {VehicleEventsProcessor}
 */
module.exports = () => {
    if (!instance) {
        instance = new VehicleEventsProcessor();
        ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};
