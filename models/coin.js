const mongoose = require('mongoose');
const util = require('./util');

const Schema = mongoose.Schema;

const CoinSchema = new Schema({
  key:       Buffer,
  data:      Buffer,
  version:   Number,
  height:    Number,
  mintTxid:  String,
  mintIndex: Number,
  script:    String,
  coinbase:  Boolean,
  value:     Number,
  address:   String,
  wallets:   [String],
  spentTxId: String
});

CoinSchema.set('toObject', {
  transform: function(doc, ret) {
    ret.id = ret._id;
    delete ret._id;
  }
});

CoinSchema.index({ key: 1 });
CoinSchema.index({ height: 1 });
CoinSchema.index({ mintTxid: 1, mintIndex: 1 });
CoinSchema.index({ address: 1 });
CoinSchema.index({ wallets: 1 }, { sparse: true });
CoinSchema.index({ spentTxid: 1 }, { sparse: true });

CoinSchema.statics.saveCoins = function saveCoins(key, data, coin, hash, index) {
  const Coin = this.model('Coin');
  const output = coin.output.toJSON();

  return new Coin({
    key,
    data,
    version:   coin.version,
    height:    coin.height,
    mintTxid:  hash,
    mintIndex: index,
    script:    output.script,
    coinbase:  coin.coinbase,
    value:     output.value,
    address:   output.address
  }).save();
};

CoinSchema.statics.getCoins = function getCoins(key) {
  return new Promise((res, rej) => {
    return this.model('Coin').findOne({ key },
    (err, coins) => {
      if (err) {
        return rej(err);
      }
      return coins ? res(coins.data) : res(coins);
    });
  });
};

CoinSchema.statics.hasCoins = function hasCoins(key) {
  return new Promise((res, rej) => {
    return this.model('Coin')
      .findOne({ key })
      .count((err, count) => {
        err ? rej(err) : res(count >= 1);
      });
  });
};

CoinSchema.statics.hasDupeCoins = function hasDupeCoins(key, height) {
  return new Promise((res, rej) => {
    return this.model('Coin')
      .findOne({
        key: key,
        height: { $lte: height }
      })
      .count((err, count) => {
        err ? rej(err) : res(count >= 1);
      });
  });
};

CoinSchema.statics.removeCoins = function removeCoins(key) {
  return this.model('Coin').find({ key }).remove();
};

module.exports = CoinSchema;
