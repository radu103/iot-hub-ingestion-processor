// load env vars from CF
require('dotenv').config();

// required modules
const cfenv = require('cfenv');
const async = require('async');
const kafkaNode = require('kafka-node');
var ConsumerGroup = require('kafka-node').ConsumerGroup;
var request = require('request');

// get ENV vars from CF
const landscapeName = process.env.LANDSCAPE_NAME;
const tenantName = process.env.TENANT_NAME;
const zookeeperHost = process.env.ZOOKEEPER_HOST;
const zookeeperPort = process.env.ZOOKEEPER_PORT;

// mongo create url
// configs from env vars
var appEnv = cfenv.getAppEnv();
console.log(appEnv.getServices());

var metadataService = appEnv.getService('iot-hub-service-odata-shared-new-metadata');
console.log("metadataService", metadataService);

var rawdataService = appEnv.getService('iot-hub-service-odata-shared-new-rawdata');
console.log("rawdataService", rawdataService);

var locationService = appEnv.getService('iot-hub-service-odata-shared-new-location');
console.log("locationService", locationService);

var eventService = appEnv.getService('iot-hub-service-odata-shared-new-event');
console.log("eventService", eventService);

// zookeeper connect client
var zookeeper = require('node-zookeeper-client');
var client = zookeeper.createClient(zookeeperHost + ':' + zookeeperPort);

client.once('connected', function () {
    
    console.log('Connected to Zookeeper : ' + zookeeperHost + ':' + zookeeperPort);

    //get all topics
    client.getChildren("/brokers/topics", (err, children, stats) => {
        
        console.log("Kafka Topics : ", children);

        children.forEach(child => checkLoadedTopic(child));

        client.close();

        startConsumerGroups();
    });
});

client.connect();

// kafka topics consume with consumer groups

var consumerOptions = {
    host: zookeeperHost + ':' + zookeeperPort,
    groupId: landscapeName + '_' + tenantName,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest'
  };
  
var topics = [];
var consumerGroups = [];

// checks loaded topic if needed to be read
function checkLoadedTopic(topic){

    var topicPre = process.env.KAFKA_TOPIC_PREFIX + landscapeName + "-" + tenantName;
    if(topic.indexOf(topicPre) >= 0)
    {
        console.log("Topic needs to be monitored : ", topic);
        topics.push(topic);
    }
}

// start consumer groups for all topics
function startConsumerGroups(){

    console.log("All monitored topics : ", topics);

    async.each(topics, function (topic) {

        var consumerGroup = new ConsumerGroup(Object.assign({
            id: landscapeName + '_' + tenantName + '_' + topic
        }, consumerOptions), topic);

        consumerGroup.on('error', onError);
        consumerGroup.on('message', onMessage);
    });
}

// log error
function onError(error) {
    console.error(error);
}

// device found request callback
var fnGetDeviceCallback = function(error, response, body, msg) {

    console.log('Get device from metadata response');

    var body = JSON.parse(body);
    console.log(body);
    
    if(error){
        console.log("Metadata service : ", error);
    }
    
    if(body.value === undefined || body.value[0] === undefined){
        console.log("Device not found !");
    }

    var device = body.value[0];
    if(device["_id"].length > 0){

        console.log("Device info : ", device); 

        var project_id = null;
        var group_id = null;

        // get project_id and group_id if specified on device
        if(device.project_id !== undefined && device.project_id !== null){
            project_id = device.project_id;
        }

        if(device.group_id !== undefined && device.group_id !== null){
            group_id = device.group_id;
        }

        // compose raw data
        var rawData = {
            'project_id' : project_id,
            'group_id' : group_id,
            'device_id' : device["_id"],
            'values' : msg.values,
            'recorded_time' : new Date(msg.receive_time),
            'created_at' : new Date()
        };

        // post request to create rawdata record
        var rawdataUrl = rawdataService.credentials.url + "/raw_data";
        var rawdataUsername = rawdataService.credentials.user;
        var rawdataPassword = rawdataService.credentials.password;
        var rawdataAuth = "Basic " + new Buffer(rawdataUsername + ":" + rawdataPassword).toString("base64");

        request(
            {
                url : rawdataUrl,
                method: 'POST',
                json: rawData,     
                headers : {
                    "Authorization" : rawdataAuth,
                    "Accept": "application/json"
                }
            },
            function(error, response, body){
                fnRawDataInsertCallback(error, response, body, msg, device["_id"]);
            }
        );

    }
}

// rawdata insert callback
var fnRawDataInsertCallback = function(error, response, body, msg, deviceId){

    console.log('Rawdata insert response');
    console.log(body);
    
    var body = JSON.parse(body);
    console.log(body);
}

// location insert callback
var fnLocationInsertCallback = function(error, response, body){
;
}

// process device event rules callback
var fnProcessDeviceEventRulesCallback = function(error, response, body){
;
}

// process message
function onMessage(message) {
    console.log("Message from '" + this.client.clientId + "' topic: '" + message.topic + "'  offset: " + message.offset);
    
    var msg = JSON.parse(message.value);
    console.log('Message : ', msg);

    var deviceId = msg.device_id;

    //get device metadata
    var metadataUrl = metadataService.credentials.url + "/device('" + deviceId + "')";
    var metadataUsername = metadataService.credentials.user;
    var metadataPassword = metadataService.credentials.password;
    var metadataAuth = "Basic " + new Buffer(metadataUsername + ":" + metadataPassword).toString("base64");

    request(
        {
            url : metadataUrl,
            headers : {
                "Authorization" : metadataAuth
            }
        },
        function(error, response, body){
            fnGetDeviceCallback(error, response, body, msg);
        }
    );
}

// close all consumer groups on exit
process.once('SIGINT', function () {
  async.each(consumerGroups, function (consumer, callback) {
    consumer.close(true, callback);
  });
});
