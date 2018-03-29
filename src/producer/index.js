const amqp = require('amqplib');

let msgCounter = 1

module.exports = async function (topic) {
  const connection = await amqp.connect('amqp://localhost');
  const channel  = await connection.createChannel();
  const exchangeName = 'eventExchange'

  if (topic.includes('*') || topic.includes('#')) {
    throw Error('topic cannot contain wildcards (*, #) when producing messages')
  }

  channel.assertExchange(exchangeName, 'topic', {durable: true})

  setInterval(() => {
    sendMessage(channel, exchangeName, topic)
  }, 500)

  return new Promise((resolve) => {
    setTimeout(() => {resolve(0)}, 200000)
  })
}

const sendMessage = function (channel, exchangeName, topic) {
  const user = {
    _id: msgCounter++,
    data: {foo: 'bar'}
  }
  const msg = Buffer.from(JSON.stringify(user))

  channel.publish(exchangeName, topic, msg)
  console.log(`sent ${topic}:${msg}`);
}
