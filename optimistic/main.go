package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/optimisticlock"
)

type ProductOptimistic struct {
	ID    uint `grom:"primaryKey"`
	Name  string
	Stock int64
	// optimistic purpose
	Version optimisticlock.Version
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

	err = db.AutoMigrate(&ProductOptimistic{})
	if err != nil {
		log.Fatal("Failed run migration")
	}

	return db
}

func processOrderOptimistic(db *gorm.DB, productID uint, quantity int64, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()

	for i := 0; i < 2; i++ {
		var product ProductOptimistic

		if err := db.First(&product, productID).Error; err != nil {
			log.Printf("Worker %d: Failed to find product: %v", workerID, err)
			return
		}

		if product.Stock < quantity {
			log.Printf("Worker %d: Not enough stock.", workerID)
			return
		}

		time.Sleep(100 * time.Millisecond)

		result := db.Model(&product).Updates(ProductOptimistic{Stock: product.Stock - quantity})
		if result.Error != nil {
			log.Printf("Worker %d: Update failed: %v", workerID, result.Error)
			continue
		}

		if result.RowsAffected == 0 {
			log.Printf("Worker %d: Conflict detected (version mismatch). Retrying...", workerID)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		fmt.Printf("Worker %d: Update successful! New stock: %d\n", workerID, product.Stock-quantity)
		return
	}

	log.Printf("Worker %d: Failed to process order after multiple retries.", workerID)
}

func main() {
	db := setupDB()

	db.Exec("TRUNCATE TABLE products RESTART IDENTITY")
	product := ProductOptimistic{Name: "Sugar", Stock: 10}
	db.Create(&product)

	fmt.Println("Running Optimistic Locking Example")

	var wg sync.WaitGroup

	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go processOrderOptimistic(db, product.ID, 1, &wg, i)
	}

	wg.Wait()
	fmt.Println("All workers finished")
}
