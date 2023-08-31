const sqlite3 = require('sqlite3').verbose()
const db = new sqlite3.Database('ellipsys_test_db.db3')

const SOURCE_TABLE_NAME = 'oa_trf_src'
const REDUCED_TABLE_NAME = 'oa_trf_src_red'
let colsToReduce = []
let allCols = []
let unmodifiedCols = []

const getCols = () => {
    return new Promise((resolve, reject) => {
        db.all(`PRAGMA table_info(${SOURCE_TABLE_NAME})`, (err, rows) => {
            if(err) reject(err)

            colsToReduce = rows.map((row) => {
                if(row.type.toLowerCase() !== 'integer'){
                    return row.name
                }
            })

            allCols = rows.map(row => row.name)

            unmodifiedCols = allCols.filter((col) =>!colsToReduce.includes(col))

            resolve()
        })
    })
}

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
    })
}

const fillReducedTable = () => {
    return new Promise((resolve, reject) => {
        db.all(`SELECT DISTINCT * FROM ${SOURCE_TABLE_NAME}`, (err, rows)=>{
            if(err) reject(err)
            const colNames = allCols.join(', ')
            const colValues = allCols.map(_ => '?').join(', ')
            const insertStmt = db.prepare(`INSERT INTO ${REDUCED_TABLE_NAME}(${colNames}) VALUES (${colValues})`)
            console.log('here', rows)
            rows.forEach(row => {
                console.log('here', row)
                const promises = colsToReduce.map((col)=> {
                    return new Promise((rowResolve, rowReject) => {
                        const tableName = `oa_trf_src_${col}_lkp`;
                        db.get(`SELECT * FROM ${tableName} WHERE champ = ?`, [row[col]], (err, row) => {
                            if(err) rowReject(err)
                            rowResolve(row.id)
                        })
                    })
                })

                Promise.all(promises).then(ids => {
                    const mappedValues = [...ids]
                    unmodifiedCols.forEach(unmodifiedCol => {
                        mappedValues.push(row[unmodifiedCol])
                        insertStmt.run(mappedValues)
                    })
                })
                insertStmt.finalize(resolve)
            })
        })
    })    
}

(async () => {
    await getCols()
    // for(const colName of colsToReduce){
    //     await createMapperTable(colName)
    //     await fillMapperTable(colName)
    // }
    // await createReducedTable()
     await fillReducedTable()
})()
