# bcoin-mongo-api
Mongodb api for Bcoin. Replacement for bcoin-mongo-models.

# Example:

```
const DB = require('bcoin-mongo-api');
const bcoin = require('bcoin');

const FullNode = bcoin.fullnode;

const node = new FullNode({
  network: 'testnet',
  db: 'memory',
  workers: true
});

const api = new DB({
  dbhost: '127.0.0.1',
  dbname: 'bcoin-mongo'
});

(async () => {
  await node.open();
  await node.connect();
  await api.open();

  node.on('connect', async (entry, block) => {
    console.log('%s (%d) added to chain.', entry.rhash(), entry.height);
    await api.saveBcoinBlock(entry, block);
  });
});

```