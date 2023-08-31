const sqlite3 = require('sqlite3').verbose()
const db = new sqlite3.Database('ellipsys_test_db.db3')

const SOURCE_TABLE_NAME = 'oa_trf_src'
const REDUCED_TABLE_NAME = 'oa_trf_src_red'

/**
 * Fonction pour créer une table de correspondance
 * @param {string} colName 
 * @returns {Promise<void>}
 */
const createMapperTable = (colName) => {
    return new Promise((resolve, reject) => {
        db.all(`PRAGMA table_info(${SOURCE_TABLE_NAME})`, (err, rows) => {
            if(err) reject(err)

            // Trouver le type de la colonne actuelle
            const colInfo = rows.find((row) => row.name === colName)
            if(!colInfo){
                reject(new Error(`La colonne ${colName} n'existe pas.`))
                return
            } 
            
            // verifier si le type de la colonne est un entier
            if(colInfo.type.toLowerCase() === 'integer'){
                resolve()
                return
            }

            // Création de la table de correspondance
            const tableName = `oa_trf_src_${colName}_lkp`;
            db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, champ ${colInfo.type})`, (err) => {
                if(err) reject(err)
                resolve()
            })
        })
    })
}

/**
 * Fonction pour remplir la table de correspondance
 * @param {string} colName 
 * @returns {Promise<void>}
 */
const fillMapperTable = (colName) => {
    return new Promise((resolve, reject) => {
        const tableName = `oa_trf_src_${colName}_lkp`;

        db.all(`SELECT DISTINCT ${colName} FROM ${SOURCE_TABLE_NAME}`, (err, rows) => {
            if(err) reject(err)

            const insertRow = db.prepare(`INSERT INTO ${tableName} (champ) VALUES (?)`);
            rows.forEach(row => insertRow.run(row[colName]))
            insertRow.finalize(resolve)
        })
    })
}

/**
 * Fonction pour remplir la table de correspondance
 * @returns {Promise<void>}
 */
const createReducedTable = () => {
    return new Promise((resolve, reject) => {
        db.all(`PRAGMA table_info(${SOURCE_TABLE_NAME})`, (err, rows) => {
            if(err) reject(err)

            // Creer la chaine de création de la table
            const columnDefinition = rows.map((row) => {
                if(row.type.toLowerCase() === 'integer'){
                    return `${row.name} ${row.type}`
                }else{
                    return `${row.name} INTEGER`
                }
            }).join(', ')

            db.run(`CREATE TABLE IF NOT EXISTS ${REDUCED_TABLE_NAME} (${columnDefinition})`, (err) =>{
                if(err) reject(err)
                resolve()
            })
        })

        db.run(`CREATE TABLE IF NOT EXISTS ${REDUCED_TABLE_NAME} (id INTEGER, )`)
    })
}

const fillReducedTable = () => {
    return new Promise((resolve, reject) => {

    })    
}
