package mongopaging

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/magiconair/properties/assert"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

type UserModelResp struct {
	FirstName string `json:"first_name" bson:"first_name"`
	LastName  string `json:"last_name" bson:"last_name"`
	Dob       string `json:"dob" bson:"dob"`
	ImageUrl  string `json:"image_url" bson:"image_url"`
	Email     string `json:"email" bson:"email"`
	CreatedAt int64  `json:"created_at" bson:"created_at"`
}

const (
	DatabseHost  string = "mongodb://localhost:27017"
	DatabaseName string = "users"
)

func TestDecode(t *testing.T) {
	_, session := newDBConn()
	db := session.Database(DatabaseName)
	pagingQuery := New(db, "users")
	pagingQuery.Find(bson.M{"email": bson.M{"$ne": ""}}).Limit(3).Sort("-created_at")
	fmt.Println(pagingQuery.Explain())
	var users []UserModelResp
	raws, cur, err := pagingQuery.Decode(context.Background())
	for _, raw := range raws {
		var user UserModelResp
		bson.Unmarshal(raw, &user)
		users = append(users, user)
	}

	lastUser := users[len(users)-1]
	cursorData := bson.D{bson.E{"created_at", lastUser.CreatedAt}}
	cursorInterface := cursor{}
	expectedCur, _ := cursorInterface.Create(cursorData)
	assert.Equal(t, cur, expectedCur)
	assert.Equal(t, err, nil)
}

func newDBConn() (a *mongo.Database, b *mongo.Client) {
	var connectOnce sync.Once
	var db *mongo.Database
	var session *mongo.Client
	connectOnce.Do(func() {
		db, session = connectToMongo()
	})

	return db, session
}

func connectToMongo() (a *mongo.Database, b *mongo.Client) {
	var err error
	session, err := mongo.NewClient(DatabseHost)
	if err != nil {
		log.Fatal(err)
	}
	err = session.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	var db = session.Database(DatabaseName)
	return db, session
}
