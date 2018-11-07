package mongopaging

import (
	"context"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"

	"github.com/mongodb/mongo-go-driver/mongo"
)

// PagingQuery is to construct mongo find command (https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find).
// And, it will return cursor id (value of sorting field from last document) and result as []bson.Raw
type PagingQuery interface {
	// Find set the filter for query results.
	Find(criteria interface{}) PagingQuery

	// Sort used to do sorting for query results according to sort field.
	// The sort field may contain two parts,
	// prefix {{-}} or {{+}}, represents ascending or descending order and
	// fieldname {{document field name}} which need to be indexed.
	// Default: -_id
	Sort(field string) PagingQuery

	// Limit is to set the maximun number of documents to be retrieved.
	// There is not default limit.
	Limit(count uint) PagingQuery

	// Select used to enable fields which should be retrieved.
	Select(selector interface{}) PagingQuery

	// Cursor is used to do pagination for document query.
	// Documents can be retrieved from that cursor value.
	Cursor(lastCursorValue string) PagingQuery

	// Decode will run the command to database and return result as []bson.Raw and error
	Decode(ctx context.Context) (result []bson.Raw, cursor string, err error)

	// Explain is to print out prepared query for command.
	Explain() string
}

type pagingQuery struct {
	db          *mongo.Database
	collection  string
	criteria    interface{}
	projection  interface{}
	sortKind    int
	sortField   string
	limit       uint
	cursorValue string
	cursorField string
	cursorError error
	cursor      Cursor
	max         bson.D
	min         bson.D
}

// New is to construct PagingQuery object with mongo.Database and collection name
func New(db *mongo.Database, collection string) PagingQuery {
	return &pagingQuery{
		db:         db,
		cursor:     cursor{},
		collection: collection,
	}
}

func (q *pagingQuery) Find(criteria interface{}) PagingQuery {
	q.criteria = criteria
	return q
}

func (q *pagingQuery) Sort(field string) PagingQuery {
	return q.sort(field)
}

// sort is to prepare mongo sorting statement from custom format (-fieldname)
func (q *pagingQuery) sort(field string) PagingQuery {
	n := 1
	if field == "" {
		field = "-_id"
	}
	if field[0] == '+' {
		field = field[1:]
	} else if field[0] == '-' {
		n, field = -1, field[1:]
	}
	q.sortField = field
	q.sortKind = n
	return q
}

func (q *pagingQuery) Limit(count uint) PagingQuery {
	q.limit = count
	return q
}

func (q *pagingQuery) Cursor(lastCursorValue string) PagingQuery {
	q.cursorValue = lastCursorValue
	return q
}

func (q *pagingQuery) Select(selector interface{}) PagingQuery {
	q.projection = selector
	return q
}

func (q *pagingQuery) Decode(ctx context.Context) (result []bson.Raw, cursor string, err error) {
	cmd, err := q.prepareCommand()
	if err != nil {
		return
	}

	data, err := q.db.RunCommand(ctx, cmd)
	if err != nil {
		return
	}

	var res struct {
		Cursor struct {
			ID         interface{} `bson:"id"`
			NS         string      `bson:"ns"`
			OK         interface{} `bson:"ok"`
			FirstBatch []bson.Raw  `bson:"firstBatch"`
		} `bson:"cursor"`
	}

	err = bson.Unmarshal(data, &res)
	if err != nil {
		return
	}

	result = res.Cursor.FirstBatch
	if len(result) > 0 {
		if q.cursorField != "" {
			var doc bson.M
			err = bson.Unmarshal(result[len(result)-1], &doc)
			if err != nil {
				return
			}
			cursorData := bson.D{bson.E{q.cursorField, doc[q.cursorField]}}
			cursor, err = q.cursor.Create(cursorData)
			if err != nil {
				return
			}
		}
	} else {
		cursor = q.cursorValue
	}

	return
}

// getMinOrMax is to choose mongo query operator ($min, $max) based on
// sorting type ascending and descanding
func (q *pagingQuery) getMinOrMax() error {
	q.cursorField = q.sortField
	if q.cursorValue != "" {
		if q.sortKind == -1 {
			q.max, q.cursorError = q.cursor.Parse(q.cursorValue)
		} else {
			q.min, q.cursorError = q.cursor.Parse(q.cursorValue)
		}
	} else {
		q.min, q.cursorError = nil, nil
		q.max, q.cursorError = nil, nil
	}

	if q.cursorError != nil {
		return q.cursorError
	}
	return nil
}

func (q *pagingQuery) prepareCommand() (interface{}, error) {
	err := q.getMinOrMax()
	if err != nil {
		return nil, err
	}

	cmd := bson.D{
		{"find", q.collection},
		{"limit", q.limit},
		{"batchSize", q.limit},
		{"singleBatch", true},
	}

	if q.criteria != nil {
		cmd = append(cmd, bson.E{"filter", q.criteria})
	}

	if q.sortField != "" {
		cmd = append(cmd, bson.E{"sort", bson.M{q.sortField: q.sortKind}})
	}

	if q.projection != nil {
		cmd = append(cmd, bson.E{"projection", q.projection})
	}

	if q.min != nil {
		cmd = append(cmd,
			bson.E{"skip", 1},
			bson.E{"min", q.min},
		)
	}

	if q.max != nil {
		cmd = append(cmd, bson.E{"max", q.max})
	}
	return cmd, nil
}

func (q *pagingQuery) Explain() string {
	cmd, err := q.prepareCommand()
	if err != nil {
		fmt.Sprintf("%s", err)
	}
	return fmt.Sprintf("%v \n", cmd)
}
