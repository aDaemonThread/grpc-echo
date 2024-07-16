// server.js

const path = require('path')
const Mali = require('mali')
const hl = require('highland')

const PROTO_PATH = path.resolve(__dirname, './protos/echo.proto')
const PORT = process.env.PORT | '8080';

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
  console.log(`got echoUnary request message: ${ctx.req.message}`)

  ctx.set('foo', 'bar')
  ctx.sendMetadata()

  ctx.res = { message: ctx.req.message }
  console.log(`set echoUnary response: ${ctx.res.message}`)
}

async function echoServerStream (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log(`got echoServerStream request message: ${ctx.req.message}`)

  ctx.set('foo', 'bar')
  ctx.sendMetadata()

  ctx.res = hl(streamData)
  console.log(`done echoServerStream`)
}

async function echoClientStream (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log('got echoClientStream')
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
          console.log(`done echoClientStream counter ${counter}`)

          ctx.set('foo', 'bar')
          ctx.sendMetadata()

          ctx.response.res = { message: 'Processed ' + counter + 'messages'}
          resolve()
        })
  })
}

async function echoBidiStream (ctx) {
  console.log('got echoBidiStream')
  console.dir(ctx.metadata, { depth: 3, colors: true })
  let counter = 0

  ctx.set('foo', 'bar')
  ctx.sendMetadata()
  
  ctx.req.on('data', d => {
    counter++
    ctx.res.write({ message:  d.message })
  })
  ctx.req.on('end', () => {
    console.log(`done echoBidiStream counter ${counter}`)
    ctx.res.end()
  })
}

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 */
function main () {
  const app = new Mali(PROTO_PATH, 'EchoService')
  app.use({ echoUnary, echoServerStream, echoClientStream, echoBidiStream })
  app.start(`0.0.0.0:${PORT}`)
  console.log(`Echo service running @ :${PORT}`)
}

main()
