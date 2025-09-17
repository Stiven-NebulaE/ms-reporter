"use strict";

const Rx = require('rxjs');

const VehicleStatsDA = require("./VehicleStatsDA");

module.exports = {
  /**
   * Data-Access start workflow
   */
  start$: Rx.concat(VehicleStatsDA.start$()),
  /**
   * @returns {VehicleStatsDA}
   */
  VehicleStatsDA: VehicleStatsDA,
};
