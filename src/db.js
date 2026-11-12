const mongoose = require('mongoose');
const config = require('./config');

async function connectDb(uri = config.mongoUri) {
  await mongoose.connect(uri);
  return mongoose.connection;
}

async function disconnectDb() {
  await mongoose.disconnect();
}

module.exports = { connectDb, disconnectDb };
