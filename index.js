const {TableReducer} = require('./TableReducer')

const app = new TableReducer('ellipsys_test_db_backup.db3')

app.run()