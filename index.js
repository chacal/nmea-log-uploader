const pgp = require('pg-promise')()
const db = pgp('postgres://localhost/signalk')
const fs = require('fs')
const nmeaParser = require('nmea-0183')
const Bluebird = require('bluebird')


const inputFile = process.argv[2]
console.log('Importing ', inputFile)


const positions = fs.readFileSync(inputFile, 'ascii')
  .split('\n')
  .filter(l => l.includes('$GPGLL'))
  .map(l => {
    [ts, , nmea] = l.split(' ')
    const timestamp = new Date(Number(ts.replace(':', '')))
    return Object.assign(nmeaParser.parse(nmea), {timestamp})
  })

Bluebird.map(positions, writePositionToDb, {concurrency: 5})
  .then(() => pgp.end())


function writePositionToDb(position) {
  return db.query(`
    INSERT INTO track (vessel_id, timestamp, point)
    VALUES ($1, $2, st_point($3, $4))
    ON CONFLICT (vessel_id, timestamp)
    DO UPDATE SET point = st_point($3, $4)
  `,
  ['freya', position.timestamp, position.longitude, position.latitude])
  .catch(e => console.error(`Failed to write position to DB! Complete input data: ${JSON.stringify(delta)} Position: ${JSON.stringify(position)}`, e))
}
