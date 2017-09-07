var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;


var Client = kafka.Client;

// get ENV vars from CF

var landscapeName = process.env.landscapeName;
var tenantName = process.env.tenantName;
var zookeeperHost = process.env.zookeeperHost;
var zookeeperPort = process.env.zookeeperPort;
console.log("ENV : ", landscapeName, tenantName, zookeeperHost, zookeeperPort);

var client = new Client(zookeeperHost + ':' + zookeeperPort);

var topic = 'test';
var partition = 0;

var producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {
  var keyedMessage = new KeyedMessage('keyed', 'a keyed message');

  producer.send(
    [
      {
        topic: topic,
        messages: [
          keyedMessage
        ],
        attributes: partition
      }
    ],
  function (err, result) {
    console.log(err || result);
    process.exit();
  });
});

producer.on('error', function (err) {
  console.log('error', err);
});
