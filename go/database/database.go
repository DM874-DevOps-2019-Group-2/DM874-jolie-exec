package database // import "DM874-jolie-exec/database"

import (
	"database/sql"
	"fmt"
)

/*DBConnect connects a postgres database and returns a pointer to the sql.DB object*/
func DBConnect(host string, port string, user string, pwd string, dbname string) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%s user=%s "+
			"password=%s dbname=%s sslmode=disable",
		host, port, user, pwd, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, err
}
