package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type firestoreUser struct {
	uid   string
	email string
}

type firestoreStore struct {
	refId    string
	url      string
	platform string
}

type firestoreTag struct {
	refId string
	name  string
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	queries := []string{}

	userSql, _ := generateUserSql()
	queries = append(queries, userSql)

	storeSql, _ := generateStoreSql()
	queries = append(queries, storeSql)

	tagSql, _ := generateTagSql()
	queries = append(queries, tagSql)

	writeOutput(queries)
}

func fetchFirestoreData(collection string) ([]firestore.DocumentSnapshot, error) {
	ctx := context.Background()
	sa := option.WithCredentialsFile("./service-account.json")
	projectID := os.Getenv("PROJECT_ID")

	client, err := firestore.NewClient(ctx, projectID, sa)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var data []firestore.DocumentSnapshot

	iter := client.Collection(collection).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		data = append(data, *doc)
	}

	return data, nil
}

func generateUserSql() (string, error) {
	firestoreUsersSnapshot, err := fetchFirestoreData("users")
	if err != nil {
		return "", err
	}

	query := "INSERT IGNORE INTO users(`uid`, `username`, `email`, `hashed_password`, `role`, `status`, `created_at`, `updated_at`) \nVALUES\n"

	for i, doc := range firestoreUsersSnapshot {
		user := firestoreUser{uid: doc.Ref.ID, email: doc.Data()["email"].(string)}
		query += fmt.Sprintf("\t('%s', NULL, '%s', NULL, 'user', -10, sysdate(), sysdate())", user.uid, user.email)
		if i < len(firestoreUsersSnapshot)-1 {
			query += ",\n"
		} else {
			query += ";\n"
		}
	}

	return query, nil
}

func generateStoreSql() (string, error) {
	firestoreStoresSnapshot, err := fetchFirestoreData("shops")
	if err != nil {
		return "", err
	}

	query := "INSERT IGNORE INTO stores(`url`, `ref_id`, `platform`, `is_active`, `created_at`, `updated_at`)\nVALUES\n"

	for i, doc := range firestoreStoresSnapshot {
		snapshot := doc.Data()
		store := firestoreStore{
			refId:    doc.Ref.ID,
			url:      snapshot["url"].(string),
			platform: snapshot["flatform"].(string),
		}
		query += fmt.Sprintf("\t('%s', '%s', '%s', 1, sysdate(), sysdate())", store.url, store.refId, store.platform)
		if i < len(firestoreStoresSnapshot)-1 {
			query += ",\n"
		} else {
			query += ";\n"
		}
	}

	return query, nil
}

func generateTagSql() (string, error) {
	firestoreTagsSnapshot, err := fetchFirestoreData("tags")
	if err != nil {
		return "", err
	}

	query := "INSERT IGNORE INTO tags(`ref_id`, `name`, `created_at`, `updated_at`)\nVALUES\n"

	for i, doc := range firestoreTagsSnapshot {
		snapshot := doc.Data()
		tag := firestoreTag{
			refId: doc.Ref.ID,
			name:  snapshot["tagname"].(string),
		}
		query += fmt.Sprintf("\t('%s', '%s', sysdate(), sysdate())", tag.refId, tag.name)
		if i < len(firestoreTagsSnapshot)-1 {
			query += ",\n"
		} else {
			query += ";\n"
		}
	}

	return query, nil
}

func writeOutput(queries []string) error {
	currentTime := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("output/%s.sql", currentTime)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, query := range queries {
		_, err = f.Write([]byte(query))
		if err != nil {
			return err
		}
	}

	fmt.Printf("Data is written to file: %s", filename)

	return nil
}
