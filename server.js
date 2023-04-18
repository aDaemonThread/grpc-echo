// server.js

const path = require('path')
const Mali = require('mali')
const hl = require('highland')

const PROTO_PATH = path.resolve(__dirname, './protos/echo.proto')
const HOSTPORT = '0.0.0.0:9090'

const streamData = [{
  message: 'Hello Bob'
},
  {
    message: 'Hello Kate'
  },
  {
    message: 'Hello Jim'
  },
  {
    message: 'Hello Sara'
  }
]

/**
 * Implements the EchoService RPC method.
 */
async function echoUnary (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log(`got sayHello request name: ${ctx.req.message}`)
  ctx.res = { message: 'Hello ' + ctx.req.message }
  console.log(`set sayHello response: ${ctx.res.message}`)
}

async function echoServerStream (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log(`got sayHellos request name: ${ctx.req.message}`)
  ctx.res = hl(streamData)
  console.log(`done sayHellos`)
}

async function echoClientStream (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log('got sayHelloCs')
  let counter = 0
  return new Promise((resolve, reject) => {
    hl(ctx.req)
        .map(message => {
          counter++
          if (message && message.message) {
            return message.message.toUpperCase()
          }
          return ''
        })
        .collect()
        .toCallback((err, result) => {
          if (err) return reject(err)
          console.log(`done sayHelloCs counter ${counter}`)
          ctx.response.res = { message: 'Hello ' + counter }
          resolve()
        })
  })
}

async function echoStream (ctx) {
  console.log('got sayHelloBidi')
  console.dir(ctx.metadata, { depth: 3, colors: true })
  let counter = 0
  ctx.req.on('data', d => {
    counter++
    ctx.res.write({ message: 'Hello ' + d.message })
  })
  ctx.req.on('end', () => {
    console.log(`done sayHelloBidi counter ${counter}`)
    ctx.res.end()
  })
}

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 */
function main () {
  const app = new Mali(PROTO_PATH, 'EchoService')
  app.use({ echoUnary, echoServerStream, echoClientStream, echoStream })
  app.start(HOSTPORT)
  console.log(`Echo service running @ ${HOSTPORT}`)
}

main()
