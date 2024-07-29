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

/*func listDatabases(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	databases, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return err
	}

	fmt.Println("Databases:")
	for _, db := range databases {
		fmt.Println("- ", db)
	}
	return nil
}*/

/*func listCollections(client *mongo.Client, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collectionNames, err := client.Database(dbName).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return err
	}

	fmt.Printf("Collections in database %s:\n", dbName)
	for _, coll := range collectionNames {
		fmt.Println("- ", coll)
	}
	return nil
}*/

/*func createCollectionIfNotExists(client *mongo.Client, dbName, collName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collectionNames, err := client.Database(dbName).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return err
	}

	for _, name := range collectionNames {
		if name == collName {
			fmt.Printf("Collection %s already exists in database %s\n", collName, dbName)
			return nil
		}
	}

	err = client.Database(dbName).CreateCollection(ctx, collName)
	if err != nil {
		return err
	}

	fmt.Printf("Collection %s created in database %s\n", collName, dbName)
	return nil
}*/

func worker(wg *sync.WaitGroup, client *mongo.Client, id int) {
	defer wg.Done()

	collection := client.Database("testdb").Collection("testcollection")

	// Создание контекста с тайм-аутом
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Пример запроса на вставку (insert)
	insertResult, err := collection.InsertMany(ctx, []interface{}{
		bson.D{{"name", "Rihanna"}, {"worker1", id}},
		bson.D{{"name", "LMFAO"}, {"worker1", id}},
		bson.D{{"name", "example"}, {"worker5", id}},
		bson.D{{"name", "example"}, {"worker7", id}},
		bson.D{{"name", "example"}, {"worker8", id}},
		bson.D{{"name", "example"}, {"worker9", id}},
	})
	if err != nil {
		log.Printf("Worker %d insert: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d inserted documents: %v\n", id, insertResult.InsertedIDs)

	// Еще один пример запроса на вставку (insert)
	insertResult2, err := collection.InsertOne(ctx, bson.D{{"name", "example"}, {"worker2", id}})
	if err != nil {
		log.Printf("Worker %d insert: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d inserted another document: %v\n", id, insertResult2.InsertedID)

	var result bson.M
	filter := bson.D{{"name", "example"}}
	err = collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		log.Printf("Worker %d find: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d found a single document: %+v\n", id, result)

	// Пример запроса на чтение (find)
	filter = bson.D{{"name", "example"}}
	cursor, err := collection.Find(ctx, filter)
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

	// Пример запроса на обновление (update)
	updateFilter := bson.D{{"name", "example"}}
	update := bson.D{
		{"$set", bson.D{
			{"names", "update_aidachar"},
		}},
	}
	updateResult, err := collection.UpdateMany(ctx, updateFilter, update)
	if err != nil {
		log.Printf("Worker %d update: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d updated %d document(s)\n", id, updateResult.ModifiedCount)

	// Пример запроса на удаление (delete)
	deleteResult, err := collection.DeleteMany(ctx, bson.D{{"name", "example"}})
	if err != nil {
		log.Printf("Worker %d delete: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d deleted %d document(s)\n", id, deleteResult.DeletedCount)
}

func main() {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:3385")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")

	/*err = listDatabases(client)
	if err != nil {
		log.Fatal(err)
	}*/

	/*dbName := "testdb"
	err = listCollections(client, dbName)
	if err != nil {
		log.Fatal(err)
	}*/

	/*collName := "testcollection"
	err = createCollectionIfNotExists(client, dbName, collName)
	if err != nil {
		log.Fatal(err)
	}*/

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
