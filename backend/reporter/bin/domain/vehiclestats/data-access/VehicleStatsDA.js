"use strict";

let mongoDB = undefined;
const { map, mapTo, tap, mergeMap } = require("rxjs/operators");
const { of, Observable, defer } = require("rxjs");

const { CustomError } = require("@nebulae/backend-node-tools").error;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const CollectionName = 'VehicleStats';
const FleetStatsCollectionName = 'fleet_statistics';
const ProcessedVehiclesCollectionName = 'processed_vehicles';

// Vehicle configuration constants
const VEHICLE_TYPES = ['SUV', 'PickUp', 'Sedan', 'Hatchback', 'Coupe'];
const VEHICLE_DECADES = ['decade1980s', 'decade1990s', 'decade2000s', 'decade2010s', 'decade2020s'];
const SPEED_CLASSES = ['Lento', 'Normal', 'Rapido'];

// Speed classification thresholds
const SPEED_THRESHOLDS = {
  SLOW_MAX: 140,
  NORMAL_MAX: 240
};

// Year ranges for decades
const DECADE_RANGES = {
  decade1980s: { min: 1980, max: 1989 },
  decade1990s: { min: 1990, max: 1999 },
  decade2000s: { min: 2000, max: 2009 },
  decade2010s: { min: 2010, max: 2019 },
  decade2020s: { min: 2020, max: 2029 }
};

class VehicleStatsDA {
  static start$(mongoDbInstance) {
    return Observable.create(observer => {
      if (mongoDbInstance) {
        mongoDB = mongoDbInstance;
        observer.next(`${this.name} using given mongo instance`);
      } else {
        mongoDB = require("../../../tools/mongo-db/MongoDB").singleton();
        observer.next(`${this.name} using singleton system-wide mongo instance`);
      }
      observer.next(`${this.name} started`);
      observer.complete();
    });
  }

  /**
   * Gets an user by its username
   */
  static getVehicleStats$(id, organizationId) {
    const collection = mongoDB.db.collection(CollectionName);

    const query = {
      _id: id, organizationId
    };
    return defer(() => collection.findOne(query)).pipe(
      map((res) => {
        return res !== null
          ? { ...res, id: res._id }
          : {}
      })
    );
  }

  static generateListingQuery(filter) {
    const query = {};
    if (filter.name) {
      query["name"] = { $regex: filter.name, $options: "i" };
    }
    if (filter.organizationId) {
      query["organizationId"] = filter.organizationId;
    }
    if (filter.active !== undefined) {
      query["active"] = filter.active;
    }
    return query;
  }

  static getVehicleStatsList$(filter = {}, pagination = {}, sortInput) {
    const collection = mongoDB.db.collection(CollectionName);
    const { page = 0, count = 10 } = pagination;

    const query = this.generateListingQuery(filter);    
    const projection = { name: 1, active: 1 };

    let cursor = collection
      .find(query, { projection })
      .skip(count * page)
      .limit(count);

    const sort = {};
    if (sortInput) {
      sort[sortInput.field] = sortInput.asc ? 1 : -1;
    } else {
      sort["metadata.createdAt"] = -1;
    }
    cursor = cursor.sort(sort);


    return mongoDB.extractAllFromMongoCursor$(cursor).pipe(
      map(res => ({ ...res, id: res._id }))
    );
  }

  static getVehicleStatsSize$(filter = {}) {
    const collection = mongoDB.db.collection(CollectionName);
    const query = this.generateListingQuery(filter);    
    return defer(() => collection.countDocuments(query));
  }

  /**
  * creates a new VehicleStats 
  * @param {*} id VehicleStats ID
  * @param {*} VehicleStats properties
  */
  static createVehicleStats$(_id, properties, createdBy) {

    const metadata = { createdBy, createdAt: Date.now(), updatedBy: createdBy, updatedAt: Date.now() };
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() => collection.insertOne({
      _id,
      ...properties,
      metadata,
    })).pipe(
      map(({ insertedId }) => ({ id: insertedId, ...properties, metadata }))
    );
  }

  /**
  * modifies the VehicleStats properties
  * @param {String} id  VehicleStats ID
  * @param {*} VehicleStats properties to update
  */
  static updateVehicleStats$(_id, properties, updatedBy) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.findOneAndUpdate(
        { _id },
        {
          $set: {
            ...properties,
            "metadata.updatedBy": updatedBy, "metadata.updatedAt": Date.now()
          }
        },
        {
          returnOriginal: false,
        }
      )
    ).pipe(
      map(result => result && result.value ? { ...result.value, id: result.value._id } : undefined)
    );
  }

  /**
  * modifies the VehicleStats properties
  * @param {String} id  VehicleStats ID
  * @param {*} VehicleStats properties to update
  */
  static updateVehicleStatsFromRecovery$(_id, properties, av) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.updateOne(
        {
          _id,
        },
        { $set: { ...properties } },
        {
          returnOriginal: false,
          upsert: true
        }
      )
    ).pipe(
      map(result => result && result.value ? { ...result.value, id: result.value._id } : undefined)
    );
  }

  /**
  * modifies the VehicleStats properties
  * @param {String} id  VehicleStats ID
  * @param {*} VehicleStats properties to update
  */
  static replaceVehicleStats$(_id, properties) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.replaceOne(
        { _id },
        properties,
      )
    ).pipe(
      mapTo({ id: _id, ...properties })
    );
  }

  /**
    * deletes an VehicleStats 
    * @param {*} _id  VehicleStats ID
  */
  static deleteVehicleStats$(_id) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.deleteOne({ _id })
    );
  }

  /**
    * deletes multiple VehicleStats at once
    * @param {*} _ids  VehicleStats IDs array
  */
  static deleteVehicleStatss$(_ids) {
    const collection = mongoDB.db.collection(CollectionName);
    return defer(() =>
      collection.deleteMany({ _id: { $in: _ids } })
    ).pipe(
      map(({ deletedCount }) => deletedCount > 0)
    );
  }

  /**
   * Gets fleet statistics from materialized view
   */
  static getFleetStatistics$() {
    console.log(`ESTE LOG SÍ getFleetStatistics de DA <========`);
    ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: START - Getting fleet statistics from MongoDB`);
    
    const collection = mongoDB.db.collection(FleetStatsCollectionName);
    return defer(() => collection.findOne({ _id: "real_time_fleet_stats" })).pipe(
      tap(result => ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: MongoDB query result: ${JSON.stringify(result)}`)),
      map(stats => {
        if (!stats) {
          ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: No stats found, returning default stats`);
          return this.getDefaultFleetStats();
        }
        
        // Calculate average HP if not already calculated or if sum/count changed
        if (stats.hpStats && stats.hpStats.count > 0) {
          if (stats.hpStats.sum !== null && stats.hpStats.sum !== undefined) {
            stats.hpStats.avg = stats.hpStats.sum / stats.hpStats.count;
            ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: Calculated average HP: ${stats.hpStats.avg}`);
          } else {
            stats.hpStats.avg = null;
            ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: Set average HP to null because sum is null`);
          }
        } else if (stats.hpStats) {
          stats.hpStats.avg = null;
          ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: Set average HP to null because count is 0`);
        }
        
        ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: SUCCESS - Returning stats: ${JSON.stringify(stats)}`);
        return stats;
      })
    );
  }


  /**
   * Calculates statistics updates from vehicle events
   */
  static calculateStatsUpdates(events) {
    const updates = {
      totalVehicles: events.length,
      vehiclesByType: this.createInitialCounters(VEHICLE_TYPES),
      vehiclesByDecade: this.createInitialCounters(VEHICLE_DECADES),
      vehiclesBySpeedClass: this.createInitialCounters(SPEED_CLASSES),
      hpStats: this.createInitialHpStats(events.length)
    };

    events.forEach(event => {
      const vehicle = event.data;
      this.processVehicleEvent(vehicle, updates);
    });

    this.cleanInfiniteValues(updates.hpStats);
    return updates;
  }

  /**
   * Creates initial HP statistics structure
   */
  static createInitialHpStats(count) {
    return {
      sum: 0,
      count: count,
      min: count > 0 ? Infinity : null,
      max: count > 0 ? -Infinity : null
    };
  }

  /**
   * Processes a single vehicle event and updates statistics
   */
  static processVehicleEvent(vehicle, updates) {
    // Count by type
    if (VEHICLE_TYPES.includes(vehicle.type)) {
      updates.vehiclesByType[vehicle.type]++;
    }
    
    // Count by decade
    const decade = this.getDecade(vehicle.year);
    if (VEHICLE_DECADES.includes(decade)) {
      updates.vehiclesByDecade[decade]++;
    }
    
    // Count by speed class
    const speedClass = this.getSpeedClass(vehicle.topSpeed);
    if (SPEED_CLASSES.includes(speedClass)) {
      updates.vehiclesBySpeedClass[speedClass]++;
    }
    
    // HP statistics
    this.updateHpStats(vehicle.hp, updates.hpStats);
  }

  /**
   * Updates HP statistics for a single vehicle
   */
  static updateHpStats(hp, hpStats) {
    if (typeof hp === 'number' && !isNaN(hp)) {
      ConsoleLogger.i(`VehicleStatsDA.updateHpStats: Processing vehicle HP: ${hp}`);
      hpStats.sum += hp;
      hpStats.min = Math.min(hpStats.min, hp);
      hpStats.max = Math.max(hpStats.max, hp);
      ConsoleLogger.i(`VehicleStatsDA.updateHpStats: Updated HP stats - Sum: ${hpStats.sum}, Min: ${hpStats.min}, Max: ${hpStats.max}`);
    } else {
      ConsoleLogger.w(`VehicleStatsDA.updateHpStats: Invalid HP value - HP: ${hp}, Type: ${typeof hp}`);
    }
  }

  /**
   * Cleans infinite values from HP statistics
   */
  static cleanInfiniteValues(hpStats) {
    if (hpStats.min === Infinity) hpStats.min = null;
    if (hpStats.max === -Infinity) hpStats.max = null;
  }

  /**
   * Gets decade from year using configuration
   */
  static getDecade(year) {
    for (const [decade, range] of Object.entries(DECADE_RANGES)) {
      if (year >= range.min && year <= range.max) {
        return decade;
      }
    }
    // Default to 1980s for any year outside defined ranges
    return VEHICLE_DECADES[0];
  }

  /**
   * Gets speed class from top speed using configuration
   */
  static getSpeedClass(topSpeed) {
    if (topSpeed < SPEED_THRESHOLDS.SLOW_MAX) return SPEED_CLASSES[0]; // Lento
    if (topSpeed <= SPEED_THRESHOLDS.NORMAL_MAX) return SPEED_CLASSES[1]; // Normal
    return SPEED_CLASSES[2]; // Rapido
  }

  /**
   * Creates initial statistics structure using configuration
   */
  static createInitialStatsStructure() {
    return {
      totalVehicles: 0,
      vehiclesByType: this.createInitialCounters(VEHICLE_TYPES),
      vehiclesByDecade: this.createInitialCounters(VEHICLE_DECADES),
      vehiclesBySpeedClass: this.createInitialCounters(SPEED_CLASSES),
      hpStats: {
        min: null,
        max: null,
        sum: 0,
        count: 0,
        avg: null
      },
      lastUpdated: new Date().toISOString()
    };
  }

  /**
   * Creates initial counters object from array of keys
   */
  static createInitialCounters(keys) {
    return keys.reduce((acc, key) => {
      acc[key] = 0;
      return acc;
    }, {});
  }

  /**
   * Returns default fleet statistics structure
   */
  static getDefaultFleetStats() {
    return {
      _id: "real_time_fleet_stats",
      ...this.createInitialStatsStructure()
    };
  }

  /**
   * Checks if vehicle aids have been processed before
   */
  static getProcessedVehicleAids$(aids) {
    ConsoleLogger.i(`VehicleStatsDA.getProcessedVehicleAids$: Checking ${aids.length} aids`);
    
    const collection = mongoDB.db.collection(ProcessedVehiclesCollectionName);
    
    return defer(() => {
      return collection.find({ aid: { $in: aids } }).toArray();
    }).pipe(
      map(processedVehicles => {
        const processedAids = processedVehicles.map(v => v.aid);
        ConsoleLogger.i(`VehicleStatsDA.getProcessedVehicleAids$: Found ${processedAids.length} already processed aids`);
        return processedAids;
      })
    );
  }

  /**
   * Marks vehicle aids as processed
   */
  static markVehicleAidsAsProcessed$(aids) {
    ConsoleLogger.i(`VehicleStatsDA.markVehicleAidsAsProcessed$: Marking ${aids.length} aids as processed`);
    
    const collection = mongoDB.db.collection(ProcessedVehiclesCollectionName);
    
    return defer(() => {
      const documents = aids.map(aid => ({
        aid: aid,
        processedAt: new Date().toISOString()
      }));
      
      return collection.insertMany(documents, { ordered: false });
    }).pipe(
      tap(result => ConsoleLogger.i(`VehicleStatsDA.markVehicleAidsAsProcessed$: SUCCESS - Inserted ${result.insertedCount} aids`)),
      map(() => ({ success: true }))
    );
  }

  // ===== FLEET STATISTICS METHODS =====

  /**
   * Inserts processed vehicle aids
   * @param {Array} aids - Array of aids to insert
   * @returns {Observable} Observable with result
   */
  static insertProcessedVehicleAids$(aids) {
    const collection = mongoDB.db.collection(ProcessedVehiclesCollectionName);
    const documents = aids.map(aid => ({ aid, processedAt: new Date() }));

    return defer(() => collection.insertMany(documents))
      .pipe(
        map(result => result.insertedCount)
      );
  }

  /**
   * Updates fleet statistics with batch data
   * @param {Object} batchStats - Statistics from the batch
   * @returns {Observable} Observable with updated statistics
   */
  static updateFleetStatistics$(batchStats) {
    ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: Updating with batch stats: ${JSON.stringify(batchStats)}`);
    
    const collection = mongoDB.db.collection(FleetStatsCollectionName);
    
    return defer(() => collection.findOne({ _id: 'real_time_fleet_stats' }))
      .pipe(
        mergeMap(currentStats => {
          const update = this.buildUpdateOperation(batchStats, currentStats);
          ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: Built update operation: ${JSON.stringify(update)}`);
          
          return collection.findOneAndUpdate(
            { _id: 'real_time_fleet_stats' },
            update,
            { returnOriginal: false, upsert: true }
          );
        }),
        tap(result => ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: MongoDB update result: ${JSON.stringify(result)}`)),
        mergeMap(result => this.updateHpAverage$(result.value)),
        map(result => result.value)
      );
  }

  /**
   * Builds the MongoDB update operation based on batch stats and current stats
   * @param {Object} batchStats - Statistics from the batch
   * @param {Object} currentStats - Current statistics from database
   * @returns {Object} MongoDB update operation
   */
  static buildUpdateOperation(batchStats, currentStats) {
    const update = {
      $inc: {
        totalVehicles: batchStats.totalVehicles,
        'hpStats.count': batchStats.hpStats.count
      },
      $set: {
        lastUpdated: new Date().toISOString()
      }
    };

    // Add vehicle type increments
    this.addIncrements(update, 'vehiclesByType', batchStats.vehiclesByType);
    this.addIncrements(update, 'vehiclesByDecade', batchStats.vehiclesByDecade);
    this.addIncrements(update, 'vehiclesBySpeedClass', batchStats.vehiclesBySpeedClass);

    // Handle HP statistics with null-safe operations
    this.handleHpStatistics(update, batchStats, currentStats);

    return update;
  }

  /**
   * Adds increment operations for a given category
   * @param {Object} update - Update operation object
   * @param {string} category - Category name (e.g., 'vehiclesByType')
   * @param {Object} stats - Statistics object
   */
  static addIncrements(update, category, stats) {
    Object.keys(stats).forEach(key => {
      update.$inc[`${category}.${key}`] = stats[key];
    });
  }

  /**
   * Handles HP statistics with null-safe operations
   * @param {Object} update - Update operation object
   * @param {Object} batchStats - Batch statistics
   * @param {Object} currentStats - Current statistics
   */
  static handleHpStatistics(update, batchStats, currentStats) {
    const { hpStats } = batchStats;
    const currentHpStats = currentStats?.hpStats;

    // Handle HP sum
    if (this.isNullishOrNaN(currentHpStats?.sum)) {
      update.$set['hpStats.sum'] = hpStats.sum;
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $set for HP sum (current: ${currentHpStats?.sum})`);
    } else {
      update.$inc['hpStats.sum'] = hpStats.sum;
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $inc for HP sum (current: ${currentHpStats?.sum})`);
    }

    // Handle HP min
    if (this.isNullishOrNaN(currentHpStats?.min)) {
      update.$set['hpStats.min'] = hpStats.min;
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $set for HP min (current: ${currentHpStats?.min})`);
    } else {
      update.$min = { 'hpStats.min': hpStats.min };
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $min for HP min (current: ${currentHpStats?.min})`);
    }

    // Handle HP max
    if (this.isNullishOrNaN(currentHpStats?.max)) {
      update.$set['hpStats.max'] = hpStats.max;
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $set for HP max (current: ${currentHpStats?.max})`);
    } else {
      update.$max = { 'hpStats.max': hpStats.max };
      ConsoleLogger.i(`VehicleStatsDA.handleHpStatistics: Using $max for HP max (current: ${currentHpStats?.max})`);
    }
  }

  /**
   * Checks if a value is null, undefined, or NaN
   * @param {*} value - Value to check
   * @returns {boolean} True if value is nullish or NaN
   */
  static isNullishOrNaN(value) {
    return value === null || value === undefined || isNaN(value);
  }

  /**
   * Updates HP average in the database
   * @param {Object} stats - Updated statistics
   * @returns {Observable} Observable with result
   */
  static updateHpAverage$(stats) {
    if (stats.hpStats && stats.hpStats.count > 0 && 
        stats.hpStats.sum !== null && stats.hpStats.sum !== undefined) {
      
      const avg = stats.hpStats.sum / stats.hpStats.count;
      ConsoleLogger.i(`VehicleStatsDA.updateHpAverage$: Calculated average HP: ${avg}`);
      
      const collection = mongoDB.db.collection(FleetStatsCollectionName);
      return collection.findOneAndUpdate(
        { _id: 'real_time_fleet_stats' },
        { $set: { 'hpStats.avg': avg } },
        { returnOriginal: false }
      );
    }
    
    return of({ value: stats });
  }

  /**
   * Creates indexes for fleet statistics collections
   * @returns {Observable} Observable with result
   */
  static createFleetStatisticsIndexes$() {
    const processedVehiclesCollection = mongoDB.db.collection(ProcessedVehiclesCollectionName);
    const fleetStatisticsCollection = mongoDB.db.collection(FleetStatsCollectionName);
    
    const indexes = [
      processedVehiclesCollection.createIndex({ aid: 1 }, { unique: true }),
      fleetStatisticsCollection.createIndex({ _id: 1 })
    ];

    return defer(() => Promise.all(indexes))
      .pipe(
        map(results => results.length)
      );
  }
}

/**
 * @returns {VehicleStatsDA}
 */
module.exports = VehicleStatsDA;
