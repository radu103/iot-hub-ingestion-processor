// load env vars from CF
require('dotenv').config();

// required modules
const cfenv = require('cfenv');
const async = require('async');
const kafkaNode = require('kafka-node');
var ConsumerGroup = require('kafka-node').ConsumerGroup;
var mongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;

// get ENV vars from CF
const landscapeName = process.env.LANDSCAPE_NAME;
const tenantName = process.env.TENANT_NAME;
const zookeeperHost = process.env.ZOOKEEPER_HOST;
const zookeeperPort = process.env.ZOOKEEPER_PORT;

// mongo create url
// configs from env vars
var appEnv = cfenv.getAppEnv();
//console.log(appEnv.getServices());

var mongoServiceName = "iot_hub_mongo_" + landscapeName;
var mongoService = appEnv.getService(mongoServiceName);
var mongoCredentials = appEnv.getServiceCreds(mongoServiceName);
var mongoUrl = mongoCredentials.uri;

console.log(mongoServiceName + " found in VCAP_SERVICES");
console.log(mongoService.credentials);

var mongoDbName = '';
var mongoUrl = '';

if(mongoService !== undefined){

    mongoUrl = mongoService.credentials.uri + "?ssl=false";

    var mongodbUri = require('mongodb-uri');
    var uriObject = mongodbUri.parse(mongoUrl);
    mongoDbName = uriObject.database;
}

console.log("Mongo url : ", mongoUrl);
console.log("Mongo db : ", mongoDbName);

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

// process message
function onMessage(message) {
    console.log("Message from '" + this.client.clientId + "' topic: '" + message.topic + "'  offset: " + message.offset);
    
    var msg = JSON.parse(message.value);
    console.log('Message : ', msg);

    // connect to mongo and put raw data there
    mongoClient.connect(mongoUrl, function(err, db){

        var dId = new ObjectId(msg.device_id);

        var deviceColName = tenantName + "_device";
        //console.log('deviceColName : ', deviceColName);

        var deviceCol = db.collection(deviceColName);     

        deviceCol.findOne({ _id : dId }, function(err, device){

            if(device !== undefined && device !== null){

                console.log('Device found : ', device);

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
                    'device_id' : msg.device_id,
                    'values' : msg.values,
                    'recorded_time' : new Date(msg.receive_time),
                    'created_at' : new Date()
                };
        
                // Get the raw data collection collection 
                var rawDataCol = db.collection(tenantName + "_raw_data");
                
                // Insert raw data record
                rawDataCol.insertOne(rawData, function(err, raw_data) {
                    
                    //console.log(raw_data);

                    if(err){
                        console.log('Mongo err : ', err);
                    }
        
                    db.close();

                    // process and insert location from raw_data if latitude & longitude are present
                    // TO DO

                    // process and generate events & validate schema if specified
                    // TO DO

                    // process and transfer to cold store
                    // TO DO
                });      
            }
            else
            {
                console.log('Device not found : ', msg.device_id, dId);
                db.close();
            }  
        });

    });
}

// close all consumer groups on exit
process.once('SIGINT', function () {
  async.each(consumerGroups, function (consumer, callback) {
    consumer.close(true, callback);
  });
});
