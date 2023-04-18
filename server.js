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
  ctx.res = { message: 'Echo ' + ctx.req.message }
  console.log(`set echoUnary response: ${ctx.res.message}`)
}

async function echoServerStream (ctx) {
  console.dir(ctx.metadata, { depth: 3, colors: true })
  console.log(`got echoServerStream request message: ${ctx.req.message}`)
  ctx.res = hl(streamData)
  console.log(`done sayHellos`)
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
          ctx.response.res = { message: 'Hello ' + counter }
          resolve()
        })
  })
}

async function echoStream (ctx) {
  console.log('got echoStream')
  console.dir(ctx.metadata, { depth: 3, colors: true })
  let counter = 0
  ctx.req.on('data', d => {
    counter++
    ctx.res.write({ message: 'Hello ' + d.message })
  })
  ctx.req.on('end', () => {
    console.log(`done echoStream counter ${counter}`)
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
  app.start(`0.0.0.0:${PORT}`)
  console.log(`Echo service running @ :${PORT}`)
}

main()
