var async = require('async');
var kafkaNode = require('kafka-node');
var ConsumerGroup = require('kafka-node').ConsumerGroup;

// get ENV vars from CF

var landscapeName = process.env.landscapeName;
var tenantName = process.env.tenantName;
var zookeeperHost = process.env.zookeeperHost;
var zookeeperPort = process.env.zookeeperPort;
console.log("ENV : ", landscapeName, tenantName, zookeeperHost, zookeeperPort);

// read zookeeper topics

var zookeeper = require('node-zookeeper-client');
var client = zookeeper.createClient(zookeeperHost + ':' + zookeeperPort);

client.once('connected', function () {
    
    console.log('Connected to Zookeeper ' + zookeeperHost + ':' + zookeeperPort);

    //get all topics
    client.getChildren("/brokers/topics", (err, children, stats) => {
        
        console.log("Kafka Topics : ", children);

        children.forEach(child => topicsLoaded(child));

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

function topicsLoaded(topic){

    if(topic.indexOf('test') >= 0)
    {
        console.log("Topic loaded : ", topic);
        topics.push(topic);
    }

    startConsumerGroups();
}

function startConsumerGroups(){

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
