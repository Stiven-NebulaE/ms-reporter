"use strict";

const { Observable, Subject, of, throwError } = require("rxjs");
const { tap, catchError } = require("rxjs/operators");
const { ConsoleLogger } = require("@nebulae/backend-node-tools").log;
const mqtt = require('mqtt');

class MqttService {
  constructor() {
    this.mqttClient = null;
    this.vehicleEventsSubject = new Subject();
    this.isSubscribed = false;
  }

  /**
   * Starts MQTT subscription to vehicle events
   */
  start$() {
    ConsoleLogger.i(`MqttService: Starting MQTT subscription`);
    
    if (this.isSubscribed) {
      ConsoleLogger.i(`MqttService: Already subscribed to MQTT`);
      return of("Already subscribed");
    }

    const mqttTopic = 'fleet/vehicles/generated';
    const mqttServerUrl = process.env.MQTT_SERVER_URL || 'mqtt://localhost:1883';
    
    ConsoleLogger.i(`MqttService: Connecting to MQTT broker: ${mqttServerUrl}`);
    ConsoleLogger.i(`MqttService: Subscribing to MQTT topic: ${mqttTopic}`);

    // Connect to MQTT broker
    this.mqttClient = mqtt.connect(mqttServerUrl);

    this.mqttClient.on('connect', () => {
      ConsoleLogger.i(`MqttService: Connected to MQTT broker`);
      
      // Subscribe to the topic
      this.mqttClient.subscribe(mqttTopic, { qos: 0 }, (err) => {
        if (err) {
          ConsoleLogger.e(`MqttService: Subscription error: ${err.message}`);
        } else {
          ConsoleLogger.i(`MqttService: Successfully subscribed to topic: ${mqttTopic}`);
          this.isSubscribed = true;
        }
      });
    });

    this.mqttClient.on('message', (topic, message) => {
      ConsoleLogger.i(`MqttService: Received MQTT message on topic: ${topic}`);
      ConsoleLogger.i(`MqttService: Message content: ${message.toString()}`);
      
      try {
        // Parse the message
        const messageData = JSON.parse(message.toString());
        ConsoleLogger.i(`MqttService: Parsed message: ${JSON.stringify(messageData)}`);
        
        // Transform to EventStore format
        const eventStoreEvent = this.transformMqttToEventStore(messageData);
        ConsoleLogger.i(`MqttService: Transformed to EventStore format: ${JSON.stringify(eventStoreEvent)}`);
        
        // Emit the event to the subject for processing
        this.vehicleEventsSubject.next(eventStoreEvent);
        ConsoleLogger.i(`MqttService: Event emitted to subject`);
      } catch (error) {
        ConsoleLogger.e(`MqttService: Error parsing message: ${error.message}`);
      }
    });

    this.mqttClient.on('error', (error) => {
      ConsoleLogger.e(`MqttService: MQTT client error: ${error.message}`);
      this.isSubscribed = false;
    });

    this.mqttClient.on('close', () => {
      ConsoleLogger.i(`MqttService: MQTT client disconnected`);
      this.isSubscribed = false;
    });

    return of("MQTT subscription started");
  }

  /**
   * Transforms MQTT message to EventStore format
   */
  transformMqttToEventStore(mqttMessage) {
    // MQTT message structure: { at, et, aid, timestamp, data }
    // EventStore format: { etv, aid, av, data, user, timestamp }
    
    return {
      etv: 1, // Event version
      aid: mqttMessage.aid,
      av: 1, // Aggregate version
      data: mqttMessage.data,
      user: null, // No user context for generated events
      timestamp: mqttMessage.timestamp || new Date().toISOString(),
      et: mqttMessage.et, // Event type
      at: mqttMessage.at  // Aggregate type
    };
  }

  /**
   * Gets the vehicle events subject for external subscription
   */
  getVehicleEventsSubject() {
    return this.vehicleEventsSubject;
  }

  /**
   * Stops MQTT subscription
   */
  stop$() {
    ConsoleLogger.i(`MqttService: Stopping MQTT subscription`);
    
    if (this.mqttClient) {
      this.mqttClient.end();
      this.mqttClient = null;
    }
    
    this.isSubscribed = false;
    return of("MQTT subscription stopped");
  }
}

module.exports = MqttService;
