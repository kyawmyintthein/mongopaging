
# mongopaging
Mongo paging library using mongo-go-driver.

### Demo
      pagingQuery := New(db, "users")
      pagingQuery.Find(bson.M{"email": bson.M{"$ne": ""}}).Limit(10).Sort("-created_at")
      results, cur, err := pagingQuery.Decode(context.Background())
      
      var users []UserModelResp
      for _, raw := range results {
    	var user UserModelResp
    	bson.Unmarshal(raw, &user)
    	users = append(users, user)
      }
