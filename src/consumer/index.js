const amqp = require('amqplib');

module.exports = async function (topic) {
  const connection = await amqp.connect('amqp://localhost');
  const channel  = await connection.createChannel();
  const exchangeName = 'eventExchange'
  const queueName = topic.replace('.', '_')
  const fiveMinutes = 300000 // ms

  channel.assertExchange(exchangeName, 'topic', {durable: true})
  channel.assertQueue(queueName, {autoDelete: false, expires: fiveMinutes})
  channel.bindQueue(queueName, exchangeName, topic);

  channel.consume(queueName, async (msg) => {
    await handleMessage(msg)
    channel.ack(msg)
  }, {noAck: false})

  return new Promise((resolve) => {
    setTimeout(() => {resolve(0)}, 50000)
  })
}

const handleMessage = async function (msg) {
  console.log(`received`, msg.content.toString())
}
