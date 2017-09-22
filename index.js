const dotenv = require('dotenv');


const async = require('async');
const kafkaNode = require('kafka-node');
const ConsumerGroup = require('kafka-node').ConsumerGroup;

// get ENV vars from CF
const landscapeName = process.env.LANDSCAPE_NAME;
const tenantName = process.env.TENANT_NAME;
const zookeeperHost = process.env.ZOOKEEPER_HOST;
const zookeeperPort = process.env.ZOOKEEPER_PORT;
console.log("ENV : ", landscapeName, tenantName, zookeeperHost, zookeeperPort);

// connect client
var zookeeper = require('node-zookeeper-client');
var client = zookeeper.createClient(zookeeperHost + ':' + zookeeperPort);

client.once('connected', function () {
    
    console.log('Connected to Zookeeper ' + zookeeperHost + ':' + zookeeperPort);

    //get all topics
    client.getChildren("/brokers/topics", (err, children, stats) => {
        
        console.log("Kafka Topics : ", children);

        children.forEach(child => checkLoadedTopic(child));

        client.close();
    });
});

client.connect();

// kafka topics consume

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

    startConsumerGroups();
}

// start consumer groups for all topics
function startConsumerGroups(){

    console.log("All monitored topics : ", topics);

    async.each(topics, function (topic) {

        var consumerGroup = new ConsumerGroup(Object.assign({id: landscapeName + '_' + tenantName + '_' + topic}, consumerOptions), topic);
        consumerGroup.on('error', onError);
        consumerGroup.on('message', onMessage);
    });
}

function onError (error) {
    console.error(error);
    console.error(error.stack);
}

function onMessage (message) {
    console.log(this.client.clientId, message);
}

process.once('SIGINT', function () {
  async.each(consumerGroups, function (consumer, callback) {
    consumer.close(true, callback);
  });
});
