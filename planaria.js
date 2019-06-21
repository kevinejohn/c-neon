const { planaria } = require('neonplanaria')
const MongoClient = require('mongodb')
const lmdb = require('node-lmdb')
const { ungzip } = require('node-gzip')
const path = require('path')
const crypto = require('crypto')
const fs = require('fs')

const cPath = path.join(__dirname, './data/c')
if (!fs.existsSync(cPath)) {
  fs.mkdirSync(cPath, { recursive: true })
}

const lmdbPath = path.join(__dirname, './data/lmdb')
if (!fs.existsSync(lmdbPath)) {
  fs.mkdirSync(lmdbPath)
}
const en = new lmdb.Env()
en.open({
  path: lmdbPath,
  mapSize: 2 * 1024 * 1024 * 1024,
  maxDbs: 3
})
const db_mediatype = en.openDbi({ name: 'mediatype', create: true })
const db_b = en.openDbi({ name: 'b', create: true })
const db_c = en.openDbi({ name: 'c', create: true })

let db
function sleep (ms = 1000) {
  return new Promise(resolve => setTimeout(() => resolve(), ms))
}
function connect () {
  return new Promise((resolve, reject) => {
    MongoClient.connect(
      'mongodb://localhost:27017',
      { useNewUrlParser: true },
      async (err, client) => {
        if (err) {
          console.log('retrying...')
          await sleep()
          return connect()
        } else {
          db = client.db('c-hash')
          resolve()
        }
      }
    )
  })
}

async function save (collection, txs) {
  const saveTxs = []
  for (const tx of txs) {
    try {
      const { lb2, b2, s3: type, s4: encoding, s5: name } = tx.out.shift()
      const { i: height, h: block, t: time } = tx.blk || {}
      let data = Buffer.from(lb2 || b2, 'base64')
      const txid = tx.tx.h
      if (encoding === 'gzip') {
        try {
          data = await ungzip(data)
        } catch (err) {
          console.log('invalid gzip', h, err)
        }
      }
      const hash = crypto
        .createHash('sha256')
        .update(data)
        .digest('hex')

      saveTxs.push({
        type,
        encoding,
        name,
        c: hash,
        b: txid,
        height,
        block,
        time: time || Math.floor(+new Date() / 1000)
      })
      fs.writeFile(path.join(cPath, hash), data, function (er) {
        if (er) {
          console.log('Error = ', er)
          throw er
        } else {
          const txn = en.beginTxn()
          txn.putString(db_mediatype, hash, type || 'unknown')
          txn.putString(db_c, hash, txid)
          txn.putString(db_b, txid, hash)
          txn.commit()
        }
      })
    } catch (err) {
      console.log(err)
    }
  }
  await collection.insertMany(saveTxs)
  return saveTxs
}
planaria.start({
  filter: {
    from: 566470,
    q: {
      find: { 'out.s1': '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut' },
      project: {
        'out.b2': 1,
        'out.lb2': 1,
        'out.s3': 1,
        'out.s4': 1,
        'out.s5': 1
      }
    }
  },
  onmempool: async e => {
    await save(db.collection('u'), [e.tx])
  },
  onblock: async e => {
    const arr = await save(db.collection('c'), e.tx)
    for (const { b } of arr) {
      await db.collection('u').deleteMany({ b })
    }
  },
  onstart: async e => {
    if (!e.tape.self.start) {
      await planaria.exec('docker', ['pull', 'mongo:4.0.4'])
      await planaria.exec('docker', [
        'run',
        '-d',
        '--rm',
        '--name',
        'mongodb',
        '-p',
        '27017-27019:27017-27019',
        '-v',
        `${path.join(__dirname, './data/mongodb')}:/data/db`,
        'mongo:4.0.4'
      ])
    }
    await connect()
    // await db.collection('u').deleteMany() // Clear current mempool
    if (e.tape.self.start) {
      await db.collection('c').deleteMany({ 'height': { $gt: e.tape.self.end } })
    }
  }
})
