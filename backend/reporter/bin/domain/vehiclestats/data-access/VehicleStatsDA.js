"use strict";

let mongoDB = undefined;
const { map, mapTo, tap, mergeMap } = require("rxjs/operators");
const { of, Observable, defer } = require("rxjs");

const { CustomError } = require("@nebulae/backend-node-tools").error;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const CollectionName = 'VehicleStats';
const FleetStatsCollectionName = 'fleet_statistics';
const ProcessedVehiclesCollectionName = 'processed_vehicles';

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
        
        // Calculate average HP if not already calculated
        if (stats.hpStats && stats.hpStats.count > 0 && !stats.hpStats.avg) {
          stats.hpStats.avg = stats.hpStats.sum / stats.hpStats.count;
          ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: Calculated average HP: ${stats.hpStats.avg}`);
        }
        
        ConsoleLogger.i(`VehicleStatsDA.getFleetStatistics$: SUCCESS - Returning stats: ${JSON.stringify(stats)}`);
        return stats;
      })
    );
  }

  /**
   * Updates fleet statistics with new vehicle events
   */
  static updateFleetStatistics$(events) {
    console.log(`ESTE LOG SÍ update <========`);

    ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: START - Updating stats with ${events.length} events`);
    
    const collection = mongoDB.db.collection(FleetStatsCollectionName);
    
    return defer(() => {
      const updates = this.calculateStatsUpdates(events);
      ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: Calculated updates: ${JSON.stringify(updates)}`);
      
      return collection.findOneAndUpdate(
        { _id: "real_time_fleet_stats" },
        {
          $inc: {
            totalVehicles: updates.totalVehicles,
            "vehiclesByType.SUV": updates.vehiclesByType.SUV,
            "vehiclesByType.PickUp": updates.vehiclesByType.PickUp,
            "vehiclesByType.Sedan": updates.vehiclesByType.Sedan,
            "vehiclesByType.Hatchback": updates.vehiclesByType.Hatchback,
            "vehiclesByType.Coupe": updates.vehiclesByType.Coupe,
            "vehiclesByDecade.decade1980s": updates.vehiclesByDecade.decade1980s,
            "vehiclesByDecade.decade1990s": updates.vehiclesByDecade.decade1990s,
            "vehiclesByDecade.decade2000s": updates.vehiclesByDecade.decade2000s,
            "vehiclesByDecade.decade2010s": updates.vehiclesByDecade.decade2010s,
            "vehiclesByDecade.decade2020s": updates.vehiclesByDecade.decade2020s,
            "vehiclesBySpeedClass.Lento": updates.vehiclesBySpeedClass.Lento,
            "vehiclesBySpeedClass.Normal": updates.vehiclesBySpeedClass.Normal,
            "vehiclesBySpeedClass.Rapido": updates.vehiclesBySpeedClass.Rapido,
            "hpStats.sum": updates.hpStats.sum,
            "hpStats.count": updates.hpStats.count
          },
          $min: { "hpStats.min": updates.hpStats.min },
          $max: { "hpStats.max": updates.hpStats.max },
          $set: {
            lastUpdated: new Date().toISOString()
          }
        },
        { upsert: true, returnOriginal: false }
      );
    }).pipe(
      tap(result => ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: MongoDB update result: ${JSON.stringify(result)}`)),
      mergeMap(result => {
        // Calculate average HP after update
        const updatedStats = result.value;
        const avg = updatedStats.hpStats.count > 0 ? updatedStats.hpStats.sum / updatedStats.hpStats.count : 0;
        ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: Calculated average HP: ${avg}`);
        
        return collection.findOneAndUpdate(
          { _id: "real_time_fleet_stats" },
          { $set: { "hpStats.avg": avg } },
          { returnOriginal: false }
        );
      }),
      tap(result => ConsoleLogger.i(`VehicleStatsDA.updateFleetStatistics$: SUCCESS - Final result: ${JSON.stringify(result)}`)),
      map(result => result.value)
    );
  }

  /**
   * Calculates statistics updates from vehicle events
   */
  static calculateStatsUpdates(events) {
    const updates = {
      totalVehicles: events.length,
      vehiclesByType: { SUV: 0, PickUp: 0, Sedan: 0, Hatchback: 0, Coupe: 0 },
      vehiclesByDecade: { decade1980s: 0, decade1990s: 0, decade2000s: 0, decade2010s: 0, decade2020s: 0 },
      vehiclesBySpeedClass: { Lento: 0, Normal: 0, Rapido: 0 },
      hpStats: { sum: 0, count: events.length, min: Infinity, max: -Infinity }
    };

    events.forEach(event => {
      const vehicle = event.data;
      
      // Count by type
      if (updates.vehiclesByType[vehicle.type] !== undefined) {
        updates.vehiclesByType[vehicle.type]++;
      }
      
      // Count by decade
      const decade = this.getDecade(vehicle.year);
      if (updates.vehiclesByDecade[decade] !== undefined) {
        updates.vehiclesByDecade[decade]++;
      }
      
      // Count by speed class
      const speedClass = this.getSpeedClass(vehicle.topSpeed);
      updates.vehiclesBySpeedClass[speedClass]++;
      
      // HP statistics
      updates.hpStats.sum += vehicle.hp;
      updates.hpStats.min = Math.min(updates.hpStats.min, vehicle.hp);
      updates.hpStats.max = Math.max(updates.hpStats.max, vehicle.hp);
    });

    return updates;
  }

  /**
   * Gets decade from year
   */
  static getDecade(year) {
    if (year >= 1980 && year < 1990) return "decade1980s";
    if (year >= 1990 && year < 2000) return "decade1990s";
    if (year >= 2000 && year < 2010) return "decade2000s";
    if (year >= 2010 && year < 2020) return "decade2010s";
    if (year >= 2020) return "decade2020s";
    return "decade1980s";
  }

  /**
   * Gets speed class from top speed
   */
  static getSpeedClass(topSpeed) {
    if (topSpeed < 140) return "Lento";
    if (topSpeed <= 240) return "Normal";
    return "Rapido";
  }

  /**
   * Returns default fleet statistics structure
   */
  static getDefaultFleetStats() {
    return {
      _id: "real_time_fleet_stats",
      totalVehicles: 0,
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
        min: 0,
        max: 0,
        sum: 0,
        count: 0,
        avg: 0
      },
      lastUpdated: new Date().toISOString()
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

}
/**
 * @returns {VehicleStatsDA}
 */
module.exports = VehicleStatsDA;
