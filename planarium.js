const { planarium } = require('neonplanaria')
const bitquery = require('bitquery')
const path = require('path')
const lmdb = require('node-lmdb')
const fs = require('fs')

const filepath = path.join(__dirname, './data/c')
const lmdbPath = path.join(__dirname, './data/lmdb')
const en = new lmdb.Env()
en.open({
  path: lmdbPath,
  mapSize: 2 * 1024 * 1024 * 1024,
  maxDbs: 3
})

planarium.start({
  name: 'C://',
  port: 3000,
  onstart: async () => {
    const db = await bitquery.init({
      url: 'mongodb://localhost:27017',
      address: 'c-hash'
    })
    const db_mediatype = en.openDbi({ name: 'mediatype', create: true })
    const db_b = en.openDbi({ name: 'b', create: true })
    return { db, db_mediatype, db_b, filepath }
  },
  onquery: e => {
    let code = Buffer.from(e.query, 'base64').toString()
    let req = JSON.parse(code)
    if (req.q && req.q.find) {
      e.core.db.read('c-hash', req).then(result => {
        e.res.json(result)
      })
    } else {
      e.res.json([])
    }
  },
  custom: e => {
    e.app.get('/c/:id', (req, res) => {
      const id = req.params.id
      const filename = path.join(e.core.filepath, id)
      const txn = en.beginTxn()
      const value = txn.getString(e.core.db_mediatype, id)
      txn.commit()
      if (value) {
        res.setHeader('Content-type', value)
      }
      console.log('[serve c]', id, value)
      fs.stat(filename, function (err, stat) {
        if (stat && stat.size) {
          res.setHeader('Content-Length', stat.size)
        }
        const filestream = fs.createReadStream(filename)
        filestream.on('error', function (e) {
          res.send('')
        })
        filestream.pipe(res)
      })
    })

    e.app.get('/b/:id', (req, res) => {
      const id = req.params.id
      const txn = en.beginTxn()
      const c_hash = txn.getString(e.core.db_b, id)
      if (!c_hash) {
        txn.commit()
        return res.status(404).send('')
      }
      const value = txn.getString(e.core.db_mediatype, c_hash)
      txn.commit()
      if (value) {
        res.setHeader('Content-type', value)
      }
      console.log('[serve b]', id, value)

      const filename = path.join(e.core.filepath, c_hash)
      fs.stat(filename, function (err, stat) {
        if (stat && stat.size) {
          res.setHeader('Content-Length', stat.size)
        }
        const filestream = fs.createReadStream(filename)
        filestream.on('error', function (e) {
          res.send('')
        })
        filestream.pipe(res)
      })
    })
  }
})
