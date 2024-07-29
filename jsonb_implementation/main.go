package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
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

	collection := client.Database("testdb24").Collection("testcollection")

	// Создание контекста с тайм-аутом
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Вставка документа с вложенным массивом
	insertResult, err := collection.InsertOne(ctx, bson.D{
		{"name", "parent_document"},
		{"worker", id},
		{"items", bson.A{
			bson.D{{"item_name", "item1"}, {"item_value", 10}},
			bson.D{{"item_name", "item2"}, {"item_value", 20}},
		}},
	})
	if err != nil {
		log.Printf("Worker %d insert: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d inserted document with ID: %v\n", id, insertResult.InsertedID)
	/*
		// Поиск документа с вложенным массивом
		filter := bson.D{{"name", "parent_document"}}
		var result bson.M
		err = collection.FindOne(ctx, filter).Decode(&result)
		if err != nil {
			log.Printf("Worker %d find: %v\n", id, err)
			return
		}
		fmt.Printf("Worker %d found document: %+v\n", id, result)
	*/
	// Обновление вложенного документа

	updateFilter := bson.D{{"name", "parent_document"}}
	update := bson.D{
		{"$set", bson.D{
			{"itemd", 18},
		}},
	}
	updateResult, err := collection.UpdateMany(ctx, updateFilter, update)
	if err != nil {
		log.Printf("Worker %d update: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d updated %d document(s)\n", id, updateResult.ModifiedCount)

	deleteFilter := bson.D{{"items.item_name", "item2"}}
	deleteResult, err := collection.DeleteMany(ctx, deleteFilter)
	if err != nil {
		log.Printf("Worker %d delete: %v\n", id, err)
		return
	}
	fmt.Printf("Worker %d deleted %d document(s)\n", id, deleteResult.DeletedCount)

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
	// Удаление документов с определённым вложенным значением
	/*
		deleteFilter := bson.D{{"name", "parent_document"}}
		deleteResult, err := collection.DeleteMany(ctx, deleteFilter)
		if err != nil {
			log.Printf("Worker %d delete: %v\n", id, err)
			return
		}
		fmt.Printf("Worker %d deleted %d document(s)\n", id, deleteResult.DeletedCount)
	*/
	/*
		findFilter := bson.D{{"name", "parent_document"}}
		var result bson.M
		err = collection.FindOne(ctx, findFilter).Decode(&result)
		if err != nil {
			log.Printf("Worker %d findOne: %v\n", id, err)
			return
		}
		fmt.Printf("Worker %d found one document: %+v\n", id, result)
	*/

	exactMatchFilter := bson.D{{"items.item_value", 10}}
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
	clientOptions := options.Client().ApplyURI("mongodb://localhost:4464")
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
	numWorkers := 10

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
