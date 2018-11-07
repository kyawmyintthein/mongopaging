package mongopaging

import (
	"encoding/base64"

	"github.com/mongodb/mongo-go-driver/bson"
)

type Cursor interface {
	Create(cursorData bson.D) (string, error)
	Parse(c string) (cursorData bson.D, err error)
}

type cursor struct{}

func (cursor) Create(cursorData bson.D) (string, error) {
	data, err := bson.Marshal(cursorData)
	return base64.RawURLEncoding.EncodeToString(data), err
}

func (cursor) Parse(c string) (cursorData bson.D, err error) {
	var data []byte
	if data, err = base64.RawURLEncoding.DecodeString(c); err != nil {
		return
	}

	err = bson.Unmarshal(data, &cursorData)
	return
}
