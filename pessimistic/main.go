package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type ProductPessimistic struct {
	ID    uint `grom:"primaryKey"`
	Name  string
	Stock int64
}

func setupDB() *gorm.DB {
	dsn := "host=localhost user=postgres password=postgres dbname=database port=5433 sslmode=disable"
	db, err := gorm.Open(
		postgres.Open(dsn),
		&gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		},
	)
	if err != nil {
		log.Fatal("Failed to connect database")
	}

	err = db.AutoMigrate(&ProductPessimistic{})
	if err != nil {
		log.Fatal("Failed run migration")
	}

	return db
}

func processOrderPessimistic(db *gorm.DB, productID uint, quantity int64, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()

	err := db.Transaction(func(tx *gorm.DB) error {
		var product ProductPessimistic

		if err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
		}).First(&product, productID).Error; err != nil {
			return err
		}

		fmt.Printf("Acquired lock. Worker: %d, Current stock: %d\n", workerID, product.Stock)
		time.Sleep(100 * time.Millisecond)

		if product.Stock < quantity {
			return fmt.Errorf("worker %d: not enough stock", workerID)
		}

		product.Stock -= quantity
		if err := tx.Save(&product).Error; err != nil {
			return err
		}

		fmt.Printf("Worker %d: Oder processed. New stock: %d\n", workerID, product.Stock)
		return nil
	})

	if err != nil {
		log.Printf("Transaction failed: %v, worker: %d", err, workerID)
	}
}

func main() {
	db := setupDB()

	db.Exec("TRUNCATE TABLE products RESTART IDENTITY")
	product := ProductPessimistic{Name: "Sugar", Stock: 10}
	db.Create(&product)

	fmt.Println("Running Pessimistic Locking Example")

	var wg sync.WaitGroup

	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go processOrderPessimistic(db, product.ID, 1, &wg, i)
	}

	wg.Wait()
	fmt.Println("All workers finished")
}
