"use strict";

const MqttService = require('./MqttService');

let mqttService = undefined;

const start$ = () => {
  return Observable.create(observer => {
    if (mqttService) {
      observer.next(`${MqttService.name} using singleton system-wide instance`);
    } else {
      mqttService = new MqttService();
      observer.next(`${MqttService.name} using singleton system-wide instance`);
    }
    observer.complete();
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
