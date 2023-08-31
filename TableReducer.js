const sqlite3 = require('sqlite3').verbose()

class TableReducer {
    db = null
    SOURCE_TABLE_NAME = 'oa_trf_src'
    REDUCED_TABLE_NAME = 'oa_trf_src_red'
    colsToReduce = []
    allCols = []
    unmodifiedCols = []

    constructor(database) {
        this.db = new sqlite3.Database(database)
    }

    getCols = () => {
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
                if(err) reject(err)
    
                this.colsToReduce = rows.map((row) => {
                    if(row.type.toLowerCase() !== 'integer'){
                        return row.name
                    }
                })
    
                this.allCols = rows.map(row => row.name)
    
                this.unmodifiedCols = this.allCols.filter((col) =>!this.colsToReduce.includes(col))
    
                resolve()
            })
        })
    }
    
    /**
     * Fonction pour créer une table de correspondance
     * @param {string} colName 
     * @returns {Promise}
     */
    createMapperTable = (colName) => {
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
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
                this.db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, champ ${colInfo.type})`, (err) => {
                    if(err) reject(err)
                    resolve()
                })
            })
        })
    }
    
    /**
     * Fonction pour remplir la table de correspondance
     * @param {string} colName 
     * @returns {Promise}
     */
    fillMapperTable = (colName) => {
        return new Promise((resolve, reject) => {
            const tableName = `oa_trf_src_${colName}_lkp`;
    
            this.db.all(`SELECT DISTINCT ${colName} FROM ${this.SOURCE_TABLE_NAME}`, (err, rows) => {
                if(err) reject(err)
    
                const insertRow = this.db.prepare(`INSERT INTO ${tableName} (champ) VALUES (?)`);
                rows.forEach(row => insertRow.run(row[colName]))
                insertRow.finalize()
                resolve()
            })
        })
    }
    
    /**
     * Fonction pour créer la table de correspondance
     * @returns {Promise}
     */
    createReducedTable = () => {
        return new Promise((resolve, reject) => {
            this.db.all(`PRAGMA table_info(${this.SOURCE_TABLE_NAME})`, (err, rows) => {
                if(err) reject(err)
    
                // Creer la chaine de création de la table
                const columnDefinition = rows.map((row) => {
                    if(row.type.toLowerCase() === 'integer'){
                        return `${row.name} ${row.type}`
                    }else{
                        return `${row.name} INTEGER`
                    }
                }).join(', ')
    
                this.db.run(`CREATE TABLE IF NOT EXISTS ${this.REDUCED_TABLE_NAME} (${columnDefinition})`, (err) =>{
                    if(err) reject(err)
                    resolve()
                })
            })
        })
    }
    
    /**
     * Methode pour remplir la table de correspondance
     * @returns {Promise}
     */
    fillReducedTable = async () => {
        const batchSize = 1000; 
        let offset = 0; 

        // Boucle pour traiter les données en lots
        while (true) {
            // Récupérer un lot de lignes de la table d'origine
            const rows = await new Promise((resolve, reject) => {
                this.db.all(`SELECT * FROM ${this.SOURCE_TABLE_NAME} LIMIT ${batchSize} OFFSET ${offset}`, (err, rows) => {
                    if (err) reject(err);
                    resolve(rows);
                });
            });

            // Si aucune ligne n'est retournée, toutes les lignes ont été traitées
            if (rows.length === 0) {
                break;
            }

            // Insérer le lot actuel de lignes dans la nouvelle table
            await this.insertBatch(rows);

            // Mettre à jour l'offset pour le prochain lot
            offset += batchSize;
        }
    };

    /**
     * Methode pour insérer un lot de lignes dans la nouvelle table
     * @param {*} rows 
     * @returns 
     */
    insertBatch = (rows) => {
        return new Promise((resolve, reject) => {
            const colNames = this.allCols.join(', ');
            const colValues = this.allCols.map(_ => '?').join(', ');

            // Début de la transaction SQLite
            this.db.run('BEGIN TRANSACTION');

            // Parcours de chaque ligne du lot
            rows.forEach(row => {
                // Préparation de l'instruction INSERT pour chaque ligne
                const insertStmt = this.db.prepare(`INSERT INTO ${this.REDUCED_TABLE_NAME}(${colNames}) VALUES (${colValues})`);

                const promises = this.allCols.map((col) => {
                    return new Promise((rowResolve, rowReject) => {
                        if (this.unmodifiedCols.includes(col)) {
                            rowResolve(row[col]);
                            return;
                        }

                        const tableName = `oa_trf_src_${col}_lkp`;
                        this.db.get(`SELECT id FROM ${tableName} WHERE champ = ?`, [row[col]], (err, rowReduced) => {
                            if (err) rowReject(err);
                            rowResolve(rowReduced?.id);
                        });
                    });
                });

                Promise.all(promises)
                    .then((ids) => {
                        insertStmt.run(ids, function(err) {
                            if (err) {
                                console.error('Insert error:', err);
                            }
                            // Finalisation de l'instruction INSERT pour chaque ligne
                            insertStmt.finalize();
                        });
                    })
                    .catch((err) => {
                        console.error('Promise.all error:', err);
                    });
            });

            // Commit de la transaction
            this.db.run('COMMIT', (err) => {
                if (err) {
                    this.db.run('ROLLBACK');
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    };

    /**
     * Call to run the program
     */
    run() {
        this.getCols()
        .then(() => {
            const mapperPromises = this.colsToReduce.map(colName => {
                return this.createMapperTable(colName)
                .then(() => this.fillMapperTable(colName));
            });
            return Promise.all(mapperPromises);
        })
        .then(() => this.createReducedTable())
        .then(() => this.fillReducedTable())
        .catch(err => console.error("An error occurred:", err));
    }
    
}

module.exports = {
    TableReducer
}