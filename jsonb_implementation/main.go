package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func worker(wg *sync.WaitGroup, client *mongo.Client, id int) {
	defer wg.Done()

	collection := client.Database("testdb_jsonb1").Collection("testcollection")

	// Создание контекста с тайм-аутом
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Вставка документа с вложенными массивами и вложенными документами
	insertResult, err := collection.InsertOne(ctx, bson.D{
		{"name", "parent_document"},
		{"worker", id},
		{"items", bson.A{
			bson.D{
				{"item_name", "item1"},
				{"item_value", 10},
				{"details", bson.D{
					{"detail_name", "detail1"},
					{"detail_value", 100},
				}},
			},
			bson.D{
				{"item_name", "item2"},
				{"item_value", 20},
				{"details", bson.D{
					{"detail_name", "detail2"},
					{"detail_value", 200},
				}},
			},
		}},
	})
	if err != nil {
		log.Printf("Worker %d insert: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d inserted document with ID: %v\n", id, insertResult.InsertedID)

	/*
		// Поиск и вывод всех документов с вложенными элементами
		cursor, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Printf("Worker %d find: %v\n", id, err)
			return
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err = cursor.All(ctx, &results); err != nil {
			log.Printf("Worker %d find: %v\n", id, err)
			return
		}
		fmt.Printf("Worker %d found documents: %+v\n", id, results)
	*/
	// Обновление вложенного документа
	updateFilter := bson.D{
		{"name", "parent_document"},
		{"items.item_name", "item2"},
		{"items.details.detail_name", "detail2"},
	}
	update := bson.D{
		{"$set", bson.D{
			{"items.1.details.detail_value", 47484839}, // Прямое указание индекса массива
		}},
	}
	updateResult, err := collection.UpdateMany(ctx, updateFilter, update)
	if err != nil {
		log.Printf("Worker %d update: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d updated %d document(s)\n", id, updateResult.ModifiedCount)

	deleteFilter := bson.D{
		{"name", "parent_document"},
		{"items.details.detail_name", "detail1"},
	}
	deleteResult, err := collection.DeleteOne(ctx, deleteFilter)
	if err != nil {
		log.Printf("Worker %d delete: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d deleted %d document(s)\n", id, deleteResult.DeletedCount)

	exactMatchFilter := bson.D{
		{"name", "parent_document"},
		{"items.details.detail_name", "detail1"},
	}
	var exactMatchResults []bson.M
	cursor, err := collection.Find(ctx, exactMatchFilter)
	if err != nil {
		log.Printf("Worker %d exact match find: %v\n", id, err)
		return
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &exactMatchResults); err != nil {
		log.Printf("Worker %d exact match find: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d found documents: %+v\n", id, exactMatchResults)

}

func main() {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:3464")
	//clientOptions.SetMaxPoolSize(1000)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")

	// Создание WaitGroup для ожидания завершения всех горутин
	var wg sync.WaitGroup

	// Количество горутин для запуска
	numWorkers := 1

	// Запуск горутин
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(&wg, client, i)
	}

	// Ожидание завершения всех горутин
	wg.Wait()

	// Отключение от сервера
	err = client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}
