"use strict";

const { Observable } = require("rxjs");
const MqttService = require('./MqttService');

let mqttService = undefined;

const start$ = () => {
  return Observable.create(observer => {
    if (mqttService) {
      observer.next(`${MqttService.name} using singleton system-wide instance`);
      // Start the MQTT service
      mqttService.start$().subscribe({
        next: (result) => observer.next(`MqttService started: ${result}`),
        error: (error) => observer.error(error),
        complete: () => observer.complete()
      });
    } else {
      mqttService = new MqttService();
      observer.next(`${MqttService.name} using singleton system-wide instance`);
      // Start the MQTT service
      mqttService.start$().subscribe({
        next: (result) => observer.next(`MqttService started: ${result}`),
        error: (error) => observer.error(error),
        complete: () => observer.complete()
      });
    }
  });
};

const getInstance = () => {
  if (!mqttService) {
    mqttService = new MqttService();
  }
  return mqttService;
};

module.exports = {
  start$,
  getInstance
};
