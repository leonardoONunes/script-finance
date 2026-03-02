import { Client } from 'pg'
import { parseCsvToObjects } from './readcsv';
import path from 'path';
 
async function main() {
    const objects = await parseCsvToObjects(path.join(__dirname, "..", "teste-50.csv"));
    const client = new Client({
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        host: process.env.PG_HOST,
        port: 5432,
        database: 'finance_db',
        ssl: {
            rejectUnauthorized: false
        }
      })
      
      await client.connect()

      const res = await client.query('SELECT $1::text as message', ['Hello world!'])
      console.log(res.rows[0].message)
      await client.end()
}

main()