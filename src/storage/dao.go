package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const SQL_SELECT_DATABASE_COUNT = "SELECT COUNT(*) FROM pg_database WHERE datname = $1"

const SQL_CREATE_DATABASE = `
	CREATE DATABASE storage
		WITH
		OWNER = postgres
		ENCODING = 'UTF8'
		TABLESPACE = pg_default
		CONNECTION LIMIT = 100;
`

const SQL_CREATE_TABLES = `
	CREATE TABLE IF NOT EXISTS promotions(
		index bigint NOT NULL PRIMARY KEY,
		id uuid NOT NULL UNIQUE,
		price double precision NOT NULL,
		expiration_date timestamp with time zone NOT NULL
	);

	CREATE SEQUENCE IF NOT EXISTS promotions_index_seq
		INCREMENT 1
		START 1
		MINVALUE 1
		MAXVALUE 9223372036854775807
		CACHE 1
		OWNED BY promotions.index;

	ALTER TABLE promotions ALTER COLUMN index 
		SET DEFAULT nextval('promotions_index_seq'::regclass);
`

const SQL_RESET_PROMOTIONS_SEQUENCE = "SELECT setval('promotions_index_seq', 1, false);"

const SQL_SELECT_PROMOTION_BY_ID = "SELECT id, price, expiration_date FROM promotions where id = $1"
const SQL_SELECT_PROMOTION_BY_INDEX = "SELECT id, price, expiration_date FROM promotions where index = $1"

const SQL_INSERT_PROMOTION = `
	INSERT INTO promotions(id, price, expiration_date)
		VALUES ($1, $2, $3)
	ON CONFLICT(id) DO UPDATE SET
		price = excluded.price,
		expiration_date = excluded.expiration_date
`

const SQL_DELETE_PROMOTIONS = "DELETE FROM promotions"

type Dao struct {
	db *sql.DB

	Promotions *PromotionsDao
}

type PromotionsDao struct {
	db *sql.DB
}

func NewDao(cfg *Configuration) *Dao {
	setupDatabase(cfg)
	db := connectToAppDB(cfg)

	return &Dao{db, &PromotionsDao{db}}
}

func (dao *PromotionsDao) GetByIndex(index int) (*Promotion, error) {
	row := dao.db.QueryRow(SQL_SELECT_PROMOTION_BY_INDEX, index)
	return dao.convertRowToPromotionModel(row)
}

func (dao *PromotionsDao) GetById(id uuid.UUID) (*Promotion, error) {
	row := dao.db.QueryRow(SQL_SELECT_PROMOTION_BY_ID, id)
	return dao.convertRowToPromotionModel(row)
}

func (dao *PromotionsDao) Upsert(promotion *Promotion) {
	id := promotion.Id
	price := promotion.Price
	expirationDate := promotion.ExpirationDate.Format("2006-01-02 15:04:05-07")

	_, err := dao.db.Exec(SQL_INSERT_PROMOTION, id, price, expirationDate)
	if err != nil {
		log.Printf("Failed to save promotion %s. Error: %v\n", id, err)
	}
}

func (dao *PromotionsDao) Rewrite(promotions chan *Promotion) {
	tx, err := dao.db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Printf("Failed to start transaction: %v\n", err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(SQL_DELETE_PROMOTIONS)
	if err != nil {
		log.Printf("Failed to delete promotions: %v\n", err)
		return
	}

	_, err = dao.db.Exec(SQL_RESET_PROMOTIONS_SEQUENCE)
	if err != nil {
		log.Printf("Failed to reset promotions sequence: %v\n", err)
		return
	}

	for p := range promotions {
		id := p.Id
		price := p.Price
		expirationDate := p.ExpirationDate.Format("2006-01-02 15:04:05-07")

		_, err := tx.Exec(SQL_INSERT_PROMOTION, id, price, expirationDate)
		if err != nil {
			log.Printf("Failed to insert promotion: %v\n", err)
		}
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
		return
	}

	log.Println("Storage rewritten")
}

func (dao *Dao) Close() error {
	return dao.db.Close()
}

func setupDatabase(cfg *Configuration) {
	db := connectToMaintenanceDB(cfg)
	defer db.Close()

	// Check if database exists
	var count int
	row := db.QueryRow(SQL_SELECT_DATABASE_COUNT, cfg.DataBase.AppDBName)
	if err := row.Scan(&count); err != nil {
		log.Fatalf("Failed to check if database exists: %v", err)
	}

	if count > 0 {
		log.Println("Database exists, no need to create one")
		return
	}

	log.Println("Database does not exist, creating...")
	_, err := db.Exec(SQL_CREATE_DATABASE)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	createTables(cfg)
	log.Println("Database created")
}

func createTables(cfg *Configuration) {
	db := connectToAppDB(cfg)
	defer db.Close()

	_, err := db.Exec(SQL_CREATE_TABLES)
	if err != nil {
		log.Fatalf("Failed to create tables in database: %v", err)
	}
}

func connectToMaintenanceDB(cfg *Configuration) *sql.DB {
	host := cfg.DataBase.Host
	port := cfg.DataBase.Port
	user := cfg.DataBase.User
	password := cfg.DataBase.Password
	dbname := cfg.DataBase.MaintenanceDBName

	conn := makeConnectionString(host, port, user, password, dbname)
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatalf("Failed to connect to maintenance db: %v", err)
	}

	return db
}

func connectToAppDB(cfg *Configuration) *sql.DB {
	host := cfg.DataBase.Host
	port := cfg.DataBase.Port
	user := cfg.DataBase.User
	password := cfg.DataBase.Password
	dbname := cfg.DataBase.AppDBName

	conn := makeConnectionString(host, port, user, password, dbname)
	return connecToDB(conn)
}

func connecToDB(conn string) *sql.DB {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatalf("Failed to connect to maintenance db: %v", err)
	}

	return db
}

func makeConnectionString(host string, port int, user string, password string, dbname string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
}

func (dao *PromotionsDao) convertRowToPromotionModel(row *sql.Row) (*Promotion, error) {
	var pId string
	var pPrice float64
	var pDate string

	if err := row.Scan(&pId, &pPrice, &pDate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &NotFound{}
		}

		return nil, err
	}

	parsedId, err := uuid.Parse(pId)
	if err != nil {
		return nil, err
	}

	parsedDate, err := time.Parse("2006-01-02T15:04:05Z", pDate)
	if err != nil {
		return nil, err
	}

	return &Promotion{parsedId, pPrice, parsedDate}, nil
}
