package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/assembly-hub/db"
	"github.com/assembly-hub/impl-db-sql"
	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host            string
	Port            int
	Username        string
	Password        string
	DBName          string
	MaxOpenConn     int
	MaxIdleConn     int
	ConnMaxLifeTime int
	ConnMaxIdleTime int
	DSNParams       string
}

type Client struct {
	cfg *Config
}

func NewClient(cfg *Config) *Client {
	c := new(Client)
	c.cfg = cfg
	return c
}

func (c *Client) Connect() (db.Executor, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.cfg.Username, c.cfg.Password, c.cfg.Host, c.cfg.Port, c.cfg.DBName)
	if c.cfg.DSNParams != "" {
		dsn += "?" + c.cfg.DSNParams
	}
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	conn.SetConnMaxLifetime(time.Duration(c.cfg.ConnMaxLifeTime) * time.Millisecond)
	conn.SetConnMaxIdleTime(time.Duration(c.cfg.ConnMaxIdleTime) * time.Millisecond)
	conn.SetMaxOpenConns(c.cfg.MaxOpenConn)
	conn.SetMaxIdleConns(c.cfg.MaxIdleConn)
	return impl.NewDB(conn), err
}
