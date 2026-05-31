package trackers

import (
	"context"
	"log/slog"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Item struct {
	ID          bson.ObjectID      `bson:"id,omitempty"`
	ItemCode    string             `bson:"itemCode"`
	Skills      map[string]float64 `bson:"skills"`
	State       int                `bson:"state"`
	Status      enums.ItemStatus   `bson:"status"`
	OwnerUserID bson.ObjectID      `bson:"ownerUserId"`
}

type ItemStore struct {
	coll *mongo.Collection
}

func NewItemStore(ctx context.Context, db *mongo.Database) *ItemStore {
	store := &ItemStore{
		coll: db.Collection("items"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *ItemStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on items.itemCode & items._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "ownerUserId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on items.ownerUserId",
			"error", err,
		)
		return
	}
}
